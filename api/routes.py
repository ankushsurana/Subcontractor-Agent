from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, constr, conint
from celery.result import AsyncResult
from typing import List, Optional
import time
import logging

logger = logging.getLogger(__name__)

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

def _normalize_results(task_result):
    """Helper to validate and normalize results (extracted from GET endpoint)"""
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

def _poll_task_result(task_id, timeout, poll_interval):
    """Helper to poll task results (extracted from GET endpoint)"""
    start_time = time.time()
    result = AsyncResult(task_id, app=workers.tasks.celery_app)
    
    while result.state in ("PENDING", "STARTED"):
        if time.time() - start_time > timeout:
            return {"status": "TIMEOUT", "message": f"Task {task_id} timed out after {timeout}s"}
        time.sleep(poll_interval)
        result = AsyncResult(task_id, app=workers.tasks.celery_app)
    
    if result.state == "SUCCESS":
        return {"status": "SUCCEEDED", "results": _normalize_results(result.result)}
    elif result.state == "FAILURE":
        return {"status": "FAILED", "error": str(result.result)}
    else:
        return {"status": result.state}

@router.post("/research-jobs", response_model=dict)
def submit_research_job(
    request: ResearchRequest,
    wait: Optional[bool] = Query(True, description="Wait for task completion"),
    timeout: Optional[int] = Query(120, description="Max seconds to wait"),
    poll_interval: Optional[float] = Query(1.0, description="Polling frequency in seconds")
):
    task = workers.tasks.celery_app.send_task("research_subcontractors", args=[request.dict()])
    
    if wait:
        poll_result = _poll_task_result(task.id, timeout, poll_interval)
        if poll_result["status"] == "SUCCEEDED":
            return JSONResponse(
                status_code=200,
                content={"status": "SUCCEEDED", "results": poll_result["results"]}
            )
        else:
            return JSONResponse(
                status_code=202 if poll_result["status"] == "TIMEOUT" else 500,
                content=poll_result
            )
    else:
        return {"task_id": task.id, "status": "submitted"}

@router.get("/results/{task_id}", response_model=ResearchResponse)
def get_research_results(
    task_id: str,
    wait: Optional[bool] = Query(True, description="Wait for task completion"),
    timeout: Optional[int] = Query(60, description="Max seconds to wait"),
    poll_interval: Optional[float] = Query(1.0, description="Polling frequency in seconds")
):
    result = AsyncResult(task_id, app=workers.tasks.celery_app)
    start_time = time.time()

    def build_response(state, status_code, content=None):
        response_content = {"status": state}
        if content:
            response_content.update(content)
        return JSONResponse(status_code=status_code, content=response_content)

    while wait and result.state in ("PENDING", "STARTED"):
        if time.time() - start_time > timeout:
            return build_response(
                "TIMEOUT", 408,
                {"message": f"Task timed out after {timeout}s"}
            )
        time.sleep(poll_interval)
        result = AsyncResult(task_id, app=workers.tasks.celery_app)


    if result.state == "SUCCESS":
        return {
            "status": "SUCCEEDED",
            "results": _normalize_results(result.result)
        }
    elif result.state == "FAILURE":
        return build_response("FAILED", 500, {"error": str(result.result)})
    else:
        return build_response(result.state, 202)