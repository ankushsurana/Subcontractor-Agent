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
    """
    try:
        logger.info(f"Starting research task for {request.get('trade')} in {request.get('city')}")
        
        # Create orchestrator
        orchestrator = ResearchOrchestrator()
        
        # Run async orchestrator in a synchronous context
        results = asyncio.run(orchestrator.execute_research(request))
        
        # Store results (with a fallback empty list if results is None)
        results_list = results or []
        if results_list:
            logger.info(f"Found {len(results_list)} results, persisting to database")
            asyncio.run(_persist_results(self.request.id, request, results_list))
        else:
            logger.warning("No results found for research request")
        
        # Convert results to dictionaries for Celery serialization
        serialized_results = [r.dict() for r in results_list] if results_list else []
        
        logger.info(f"Research task completed successfully with {len(serialized_results)} results")
        return serialized_results
    
    except Exception as e:
        logger.error(f"Research failed: {str(e)}", exc_info=True)
        if "connection" in str(e).lower() or "timeout" in str(e).lower():
            self.retry(exc=e, countdown=30, max_retries=3)
        return []

async def _persist_results(task_id: str, request: dict, results: List[ResearchResult]):
    """
    Persist research results to MongoDB.
    """
    try:
        if not task_id or not results:
            logger.warning("Missing task_id or results for persistence")
            return
            
        # Calculate success rate safely
        candidates_count = getattr(request, "get", lambda x, y: 0)("expected_candidates", 20) or 20
        success_rate = f"{(len(results) / candidates_count) * 100:.1f}%" if candidates_count > 0 else "0%"
        
        record = {
            "task_id": task_id,
            "status": "COMPLETED",
            "request": request,
            "results": [r.dict() for r in results],
            "metadata": {
                "candidates_found": len(results),
                "success_rate": success_rate,
                "execution_time": datetime.utcnow().isoformat()
            }
        }
        await db.research_jobs.insert_one(record)
        logger.info(f"Successfully stored research results for task {task_id}")
    except Exception as e:
        logger.error(f"Error persisting results: {str(e)}")
        # Don't raise, just log - we want to return the results even if persistence fails