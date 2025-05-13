import asyncio
import os
from datetime import datetime
from typing import List

from celery import Celery
from dotenv import load_dotenv
from duckduckgo_search import DDGS
from motor.motor_asyncio import AsyncIOMotorClient
import httpx
from bs4 import BeautifulSoup
import logging

from models.schemas import ResearchResult
from api.services.research_service import ResearchOrchestrator

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")

from pymongo import MongoClient
mongo_client = MongoClient(MONGO_URL)
db = mongo_client.agentdb

celery_app = Celery("research_agent")
celery_app.conf.update(
    broker_url=REDIS_URL,
    result_backend=REDIS_URL,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    task_track_started=True,
    task_time_limit=300,
    worker_prefetch_multiplier=1,
    result_expires=3600,
    result_backend_transport_options={
        'retry_policy': {
            'timeout': 5.0
        }
    },
    broker_connection_retry_on_startup=True,
    broker_connection_max_retries=10
)

def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def discover_candidates(trade, city, state, keywords, min_results=20):
    query = f"{trade} contractors {city} {state} " + " ".join(keywords)
    candidates = set()
    with DDGS() as ddgs:
        for r in ddgs.text(query, max_results=50):
            url = r.get("href") or r.get("url")
            if url and url.startswith("http"):
                candidates.add(url)
            if len(candidates) >= min_results:
                break
    return list(candidates)

async def extract_profile(url):
    profile = {
        "name": None,
        "address": None,
        "phone": None,
        "licensing": None,
        "bonding": None,
        "portfolio": None,
        "union_status": None
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            soup = BeautifulSoup(resp.text, "lxml")
            profile["name"] = soup.title.string.strip() if soup.title and soup.title.string else None
            text = soup.get_text(" ", strip=True)
            for tag in soup.find_all(['address']):
                if tag.text:
                    profile["address"] = tag.text.strip()
                    break
            import re
            phone_match = re.search(r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', text)
            if phone_match:
                profile["phone"] = phone_match.group(0)
            if "license" in text.lower():
                profile["licensing"] = "Found"
         
            if "bonded" in text.lower() or "bonding" in text.lower():
                profile["bonding"] = "Found"
         
            if "projects" in text.lower() or "portfolio" in text.lower():
                profile["portfolio"] = "Found"
         
            if "union" in text.lower():
                profile["union_status"] = "Union"
            elif "non-union" in text.lower():
                profile["union_status"] = "Non-union"
    except Exception as e:
        logger.error(f"Error extracting profile for {url}: {str(e)}")
    return profile

@celery_app.task(bind=True, name="research_subcontractors")
def research_subcontractors(self, request: dict):
    """
    Celery task to research subcontractors based on request criteria.
    Orchestrates the 5-stage research pipeline:
    FR1: Web discovery
    FR2: Profile extraction
    FR3: License verification
    FR4: Project history parsing
    FR5: Relevance scoring
    """
    try:
        logger.info(f"[Task] Starting research task for {request.get('trade')} in {request.get('city')}, {request.get('state')}")
        logger.info(f"[Task] Full request: {request}")
        
        orchestrator = ResearchOrchestrator()
        
        logger.info("[Task] Executing research pipeline through orchestrator...")
        results = run_async(orchestrator.execute_research(request))
        
        results_list = results or []
        
        logger.info(f"[Task] Research pipeline completed with {len(results_list)} results")
        if results_list:
            for i, result in enumerate(results_list[:3]):
                logger.info(f"[Task] Top result #{i+1}: Name={result.name}, "
                          f"Score={result.score}, License={result.lic_number}")

            record = {
                "task_id": self.request.id,
                "status": "COMPLETED",
                "request": request,
                "results": [r.dict() for r in results_list],
                "metadata": {
                    "candidates_found": len(results_list),
                    "success_rate": f"{(len(results_list) / (request.get('expected_candidates', 20) or 20)) * 100:.1f}%",
                    "execution_time": datetime.utcnow().isoformat(),
                    "timestamp": datetime.utcnow()
                }
            }

            logger.info(f"[Task] Persisting {len(results_list)} results to database...")
            _persist_results_sync(db, self.request.id, record)
            logger.info("[Task] Results persisted successfully")
        else:
            logger.warning("[Task] No results found for research request")
        
        try:
            serialized_results = [r.dict() for r in results_list] if results_list else []
            
            if serialized_results:
                logger.info(f"[Task] Successfully serialized {len(serialized_results)} results")
                logger.info(f"[Task] Sample result: {serialized_results[0]}")
            else:
                logger.warning("[Task] No serialized results available")
        except Exception as e:
            logger.error(f"[Task] Error serializing results: {str(e)}")
            serialized_results = []
        
        logger.info(f"[Task] Research task completed successfully with {len(serialized_results)} results")
        return serialized_results
    
    except Exception as e:
        logger.error(f"[Task] Research task failed: {str(e)}", exc_info=True)
        
        if any(err in str(e).lower() for err in ["connection", "timeout", "network"]):
            logger.info(f"[Task] Retrying task due to connection/timeout error (attempt {self.request.retries + 1})")
            self.retry(exc=e, countdown=30, max_retries=3)
            
        return []

def run_async(coro):
    """Helper function to run async code in a new event loop"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

def _persist_results_sync(db, task_id: str, record: dict):
    """Synchronous version of persist results"""
    try:

        existing = db.research_jobs.find_one({"task_id": task_id})
        
        if existing:
            logger.info(f"[Persist] Updating existing record for task {task_id}")
            result = db.research_jobs.replace_one({"task_id": task_id}, record)
            if result.modified_count > 0:
                logger.info(f"[Persist] Successfully updated record for task {task_id}")
            else:
                logger.warning(f"[Persist] Update operation did not modify any records for task {task_id}")
        else:
            logger.info(f"[Persist] Creating new record for task {task_id}")
            result = db.research_jobs.insert_one(record)
            logger.info(f"[Persist] Successfully inserted record for task {task_id} with ID {result.inserted_id}")
            
    except Exception as e:
        logger.error(f"[Persist] Error checking/updating MongoDB: {str(e)}")
        try:
            logger.info(f"[Persist] Falling back to direct insert for task {task_id}")
            db.research_jobs.insert_one(record)
            logger.info(f"[Persist] Successfully stored research results for task {task_id}")
        except Exception as e2:
            logger.error(f"[Persist] Fallback insert also failed: {str(e2)}")
            raise