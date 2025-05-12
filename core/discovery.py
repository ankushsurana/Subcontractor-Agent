# discovery.py
import os
import httpx
from typing import List, Dict
from urllib.parse import urlparse
from duckduckgo_search import DDGS
import logging

logger = logging.getLogger(__name__)

class DiscoveryService:
    """
    Service to discover subcontractor websites through web searches.
    """
    def __init__(self):
        self.min_candidates = 20
        self.api_key = os.getenv("SERPAPI_KEY", "")
        self.base_url = "https://serpapi.com/search.json"
        self.search_sources = {
            "primary": {
                "params": {
                    "engine": "google",
                    "num": 20,
                    "hl": "en",
                    "gl": "us"
                }
            },
            "fallback": {
                "engine": "bing",
                "params": {"count": 20}
            }
        }

    async def find_subcontractors(self, trade: str, city: str, state: str, keywords: List[str]) -> List[Dict]:
        """
        Find subcontractor websites based on search criteria.
        Returns at least self.min_candidates URLs.
        """
        # Ensure parameters are not None
        trade = str(trade or "")
        city = str(city or "")
        state = str(state or "")
        keywords = keywords or []
        
        # Build query with valid parameters
        query_parts = [part for part in [
            f"{trade} contractors" if trade else None,
            city if city else None,
            state if state else None
        ] if part]
        
        query = " ".join(query_parts + keywords)
        
        if not query:
            logger.error("Empty search query - missing required parameters")
            return []
            
        candidates = []
        
        try:
            with DDGS() as ddgs:
                for r in ddgs.text(query, max_results=50):
                    url = r.get("href") or r.get("url")
                    title = r.get("title", "")
                    description = r.get("body", "")
                    
                    if url and url.startswith("http"):
                        candidates.append({
                            "url": url,
                            "title": title,
                            "description": description
                        })
                    
                    if len(candidates) >= self.min_candidates:
                        break
        except Exception as e:
            logger.error(f"Error during discovery search: {str(e)}")
            
        if len(candidates) < self.min_candidates:
            logger.warning(f"Only found {len(candidates)} candidates, below minimum {self.min_candidates}")

        return candidates

    async def _try_primary_search(self, trade: str, city: str, state: str, keywords: List[str]) -> List[Dict]:
        params = {
            **self.search_sources["primary"]["params"],
            "q": self._build_query(trade, city, state, keywords),
            "location": f"{city}, {state}, United States",
            "api_key": self.api_key
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(self.base_url, params=params)
            return self._process_results(response.json())

    def _build_query(self, trade: str, city: str, state: str, keywords: List[str]) -> str:
        base = f"{trade} contractor {city} {state}"
        extras = " ".join(keywords) + " bonding -association -jobs"
        return f"{base} {extras}".strip()

    def _process_results(self, data: Dict) -> List[Dict]:
        return [{
            "url": r["link"],
            "display_url": r.get("displayed_link", ""),
            "title": r["title"],
            "snippet": r.get("snippet")
        } for r in data.get("organic_results", [])[:20]]
    

    def _validate_candidate(candidate: dict) -> bool:
        required = ["url", "title"]
        return all(candidate.get(k) for k in required) and \
            "http" in candidate["url"] and \
            len(candidate["title"]) > 3


    def process_serpapi_results(data: dict) -> List[dict]:
        def validate_candidate(candidate: dict) -> bool:
            required = ["url", "title"]
            return all(candidate.get(k) for k in required) and \
                "http" in candidate["url"] and \
                len(candidate["title"]) > 3

        return [c for c in (
            {
                "url": r["link"],
                "title": r.get("title", "").split(" - ")[0],
                "source": "serpapi",
                "domain": urlparse(r["link"]).netloc
            } for r in data.get("organic_results", [])
        ) if validate_candidate(c)][:20]