# from fastapi import APIRouter, HTTPException
# from pydantic import BaseModel
# from workers.tasks import research_subcontractors
# from celery.result import AsyncResult
# import os


# router = APIRouter()

# class ResearchRequest(BaseModel):
#     trade: str
#     city: str
#     state: str
#     min_bond: int
#     keywords: list[str] = []

# @router.post("/research-jobs")
# def submit_research_job(request: ResearchRequest):
#     task = research_subcontractors.delay(request.dict())
#     return {"task_id": task.id, "status": "submitted"}

# @router.get("/results/{task_id}")
# def get_research_results(task_id: str):
#     result = AsyncResult(task_id)
#     if result.state == "PENDING":
#         return {"status": "pending"}
#     elif result.state == "SUCCESS":
#         # Format the result to match the required output
#         return {"status": "SUCCEEDED", "results": result.result}
#     elif result.state == "FAILURE":
#         return {"status": "failed", "error": str(result.result)}
#     else:
#         return {"status": result.state}

from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, constr, conint
from celery.result import AsyncResult
from typing import List, Optional
import time
import asyncio
import logging

# Configure logger
logger = logging.getLogger(__name__)

# Define the celery_app import only, not research_subcontractors
import workers.tasks

router = APIRouter()

class ResearchRequest(BaseModel):
    trade: constr(min_length=2, max_length=50)
    city: constr(min_length=2, max_length=50)
    state: constr(min_length=2, max_length=2)
    min_bond: conint(gt=0)
    keywords: List[constr(max_length=20)] = Field(default_factory=list)
    
class ResearchResult(BaseModel):
    name: str
    website: str
    city: str
    state: str
    lic_active: bool
    lic_number: str
    bond_amount: int
    tx_projects_past_5yrs: int
    score: int
    evidence_url: str
    evidence_text: str
    last_checked: str

class ResearchResponse(BaseModel):
    status: str
    results: List[ResearchResult]

@router.post("/research-jobs", response_model=dict)
def submit_research_job(request: ResearchRequest):
    task = workers.tasks.celery_app.send_task("research_subcontractors", args=[request.dict()])
    return {"task_id": task.id, "status": "submitted"}

@router.get("/results/{task_id}", response_model=ResearchResponse)
def get_research_results(
    task_id: str,
    wait: Optional[bool] = True,
    timeout: Optional[int] = 60,
    poll_interval: Optional[float] = 1.0
):
    """
    Get research results by task ID.
    - wait: If True, block until task completes or timeout (default: True)
    - timeout: Max seconds to wait (default: 60)
    - poll_interval: How often to poll in seconds (default: 1.0)
    """
    result = AsyncResult(task_id, app=workers.tasks.celery_app)
    start_time = time.time()

    def build_status_response(state, status_code=202, extra=None):
        content = {"status": state}
        if extra:
            content.update(extra)
        return JSONResponse(status_code=status_code, content=content)

    # Helper: Validate and normalize results
    def normalize_results(task_result):
        if not isinstance(task_result, list):
            logger.error(f"Unexpected result structure: {type(task_result)}")
            return []
        validated_results = []
        required_fields = [
            "name", "website", "city", "state", "lic_active",
            "lic_number", "bond_amount", "tx_projects_past_5yrs",
            "score", "evidence_url", "evidence_text", "last_checked"
        ]
        for item in task_result:
            if not isinstance(item, dict):
                continue
            for field in required_fields:
                if field not in item:
                    if field == "lic_active":
                        item[field] = False
                    elif field in ["bond_amount", "tx_projects_past_5yrs", "score"]:
                        item[field] = 0
                    else:
                        item[field] = ""
            validated_results.append(item)
        return validated_results

    # Polling loop if wait=True
    while wait and result.state in ("PENDING", "STARTED"):
        if time.time() - start_time > timeout:
            return build_status_response(
                state="TIMEOUT",
                status_code=408,
                extra={"message": f"Task is still in {result.state} state after {timeout} seconds"}
            )
        time.sleep(poll_interval)
        result = AsyncResult(task_id, app=workers.tasks.celery_app)

    # After polling or if wait=False, check final state
    if result.state == "SUCCESS":
        task_result = result.result
        validated_results = normalize_results(task_result)
        return {"status": "SUCCEEDED", "results": validated_results}
    elif result.state == "FAILURE":
        return build_status_response(
            state="FAILED",
            status_code=500,
            extra={"error": str(result.result) if result.result else "Unknown error"}
        )
    elif result.state == "REVOKED":
        return build_status_response(
            state="REVOKED",
            status_code=410,
            extra={"error": "Task was revoked."}
        )
    elif result.state in ("PENDING", "STARTED"):
        return build_status_response(state=result.state, status_code=202)
    else:
        return build_status_response(
            state=result.state,
            status_code=500,
            extra={"error": str(result.result) if result.result else "Unknown error"}
        )