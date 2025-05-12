from fastapi import Request
from core.discovery import discover_domains
from core.extractor import extract_html
from core.license import check_license
from core.history import parse_history
from core.scoring import score_project

async def discovery_handler(request: Request):
    query = request.query_params.get("q")
    return await discover_domains(query)

async def extractor_handler(request: Request):
    url = request.query_params.get("url")
    return await extract_html(url)

async def license_handler(request: Request):
    url = request.query_params.get("url")
    return await check_license(url)

async def history_handler(request: Request):
    url = request.query_params.get("url")
    return await parse_history(url)

async def scoring_handler(request: Request):
    url = request.query_params.get("url")
    return await score_project(url)
