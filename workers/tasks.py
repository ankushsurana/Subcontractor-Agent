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

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Load environment variables
load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Use separate env vars for broker and backend
CELERY_BROKER_URL = os.getenv("REDIS_BROKER_URL", "redis://redis:6379/0")
CELERY_BACKEND_URL = os.getenv("REDIS_BACKEND_URL", "redis://redis:6379/1")

# Configure Celery
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

# MongoDB connection
mongo_client = AsyncIOMotorClient(os.getenv("MONGO_URL", "mongodb://mongo:27017"))
db = mongo_client.agentdb

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
            # Simple extraction logic (should be improved for production)
            profile["name"] = soup.title.string.strip() if soup.title and soup.title.string else None
            # Try to extract address, phone, licensing, bonding, portfolio, union status
            text = soup.get_text(" ", strip=True)
            # Address (very naive)
            for tag in soup.find_all(['address']):
                if tag.text:
                    profile["address"] = tag.text.strip()
                    break
            # Phone (naive regex)
            import re
            phone_match = re.search(r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', text)
            if phone_match:
                profile["phone"] = phone_match.group(0)
            # Licensing
            if "license" in text.lower():
                profile["licensing"] = "Found"
            # Bonding
            if "bonded" in text.lower() or "bonding" in text.lower():
                profile["bonding"] = "Found"
            # Portfolio
            if "projects" in text.lower() or "portfolio" in text.lower():
                profile["portfolio"] = "Found"
            # Union status
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
    This is a synchronous function that uses asyncio.run() to run async code.
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
        
        # Create orchestrator
        orchestrator = ResearchOrchestrator()
        
        # Run async orchestrator in a synchronous context
        logger.info("[Task] Executing research pipeline through orchestrator...")
        results = asyncio.run(orchestrator.execute_research(request))
        
        # Store results (with a fallback empty list if results is None)
        results_list = results or []
        
        # Log the results
        logger.info(f"[Task] Research pipeline completed with {len(results_list)} results")
        if results_list:
            # Log summary of first few results
            for i, result in enumerate(results_list[:3]):
                logger.info(f"[Task] Top result #{i+1}: Name={result.name}, "
                          f"Score={result.score}, License={result.lic_number}")
            
            # Persist results to database
            logger.info(f"[Task] Persisting {len(results_list)} results to database...")
            asyncio.run(_persist_results(self.request.id, request, results_list))
            logger.info("[Task] Results persisted successfully")
        else:
            logger.warning("[Task] No results found for research request")
        
        # Convert results to dictionaries for Celery serialization
        try:
            serialized_results = [r.dict() for r in results_list] if results_list else []
            
            # Verify serialized results
            if serialized_results:
                logger.info(f"[Task] Successfully serialized {len(serialized_results)} results")
                # Log first result as sample
                logger.info(f"[Task] Sample result: {serialized_results[0]}")
            else:
                logger.warning("[Task] No serialized results available")
        except Exception as e:
            logger.error(f"[Task] Error serializing results: {str(e)}")
            # Fallback to empty list
            serialized_results = []
        
        logger.info(f"[Task] Research task completed successfully with {len(serialized_results)} results")
        return serialized_results
    
    except Exception as e:
        logger.error(f"[Task] Research task failed: {str(e)}", exc_info=True)
        
        # Retry on network/timeout errors
        if any(err in str(e).lower() for err in ["connection", "timeout", "network"]):
            logger.info(f"[Task] Retrying task due to connection/timeout error (attempt {self.request.retries + 1})")
            self.retry(exc=e, countdown=30, max_retries=3)
            
        # Return empty list on error
        return []

async def _persist_results(task_id: str, request: dict, results: List[ResearchResult]):
    """
    Persist research results to MongoDB.
    Saves the full pipeline results with metadata for retrieval later.
    """
    try:
        if not task_id:
            logger.warning("[Persist] Missing task_id for persistence, aborting")
            return
            
        # Prepare results for storage
        logger.info(f"[Persist] Preparing to store {len(results)} results for task {task_id}")
        
        # Calculate success metrics
        candidates_count = request.get("expected_candidates", 20) or 20
        if isinstance(candidates_count, str) and candidates_count.isdigit():
            candidates_count = int(candidates_count)
        elif not isinstance(candidates_count, int):
            candidates_count = 20
            
        success_rate = f"{(len(results) / candidates_count) * 100:.1f}%" if candidates_count > 0 else "0%"
        
        # Serialize results with error handling
        serialized_results = []
        for r in results:
            try:
                if hasattr(r, "dict"):
                    result_dict = r.dict()
                    serialized_results.append(result_dict)
                elif isinstance(r, dict):
                    serialized_results.append(r)
                else:
                    # Last resort - convert to dict if it has __dict__ attribute
                    if hasattr(r, "__dict__"):
                        serialized_results.append(r.__dict__)
            except Exception as e:
                logger.error(f"[Persist] Error serializing result: {str(e)}")
                logger.error(f"[Persist] Problematic result: {type(r)}")
                
        if len(serialized_results) != len(results):
            logger.warning(f"[Persist] Some results could not be serialized: {len(results)} → {len(serialized_results)}")
        
        # Prepare the record
        record = {
            "task_id": task_id,
            "status": "COMPLETED",
            "request": request,
            "results": serialized_results,
            "metadata": {
                "candidates_found": len(results),
                "success_rate": success_rate,
                "execution_time": datetime.utcnow().isoformat(),
                "timestamp": datetime.utcnow()
            }
        }
        
        # Check if we should update an existing record
        try:
            existing = await db.research_jobs.find_one({"task_id": task_id})
            
            if existing:
                logger.info(f"[Persist] Updating existing record for task {task_id}")
                result = await db.research_jobs.replace_one({"task_id": task_id}, record)
                if result.modified_count > 0:
                    logger.info(f"[Persist] Successfully updated record for task {task_id}")
                else:
                    logger.warning(f"[Persist] Update operation did not modify any records for task {task_id}")
            else:
                logger.info(f"[Persist] Creating new record for task {task_id}")
                result = await db.research_jobs.insert_one(record)
                logger.info(f"[Persist] Successfully inserted record for task {task_id} with ID {result.inserted_id}")
                
        except Exception as e:
            logger.error(f"[Persist] Error checking/updating MongoDB: {str(e)}")
            # Fallback to direct insert
            try:
                logger.info(f"[Persist] Falling back to direct insert for task {task_id}")
                await db.research_jobs.insert_one(record)
                logger.info(f"[Persist] Successfully stored research results for task {task_id}")
            except Exception as e2:
                logger.error(f"[Persist] Fallback insert also failed: {str(e2)}")
                raise
                
    except Exception as e:
        logger.error(f"[Persist] Error persisting results: {str(e)}", exc_info=True)
        # Don't raise, just log - we want to return the results even if persistence fails