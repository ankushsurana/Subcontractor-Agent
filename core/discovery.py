# # discovery.py
# import os
# import httpx
# from typing import List, Dict
# from urllib.parse import urlparse
# from duckduckgo_search import DDGS
# import logging

# logger = logging.getLogger(__name__)

# class DiscoveryService:
#     """
#     Service to discover subcontractor websites through web searches.
#     """
#     def __init__(self):
#         self.min_candidates = 20
#         self.api_key = os.getenv("SERPAPI_KEY", "")
#         self.base_url = "https://serpapi.com/search.json"
#         self.search_sources = {
#             "primary": {
#                 "params": {
#                     "engine": "google",
#                     "num": 20,
#                     "hl": "en",
#                     "gl": "us"
#                 }
#             },
#             "fallback": {
#                 "engine": "bing",
#                 "params": {"count": 20}
#             }
#         }

#     async def find_subcontractors(self, trade: str, city: str, state: str, keywords: List[str]) -> List[Dict]:
#         """
#         Find subcontractor websites based on search criteria.
#         Returns at least self.min_candidates URLs from multiple sources.
#         """
#         trade = str(trade or "")
#         city = str(city or "")
#         state = str(state or "")
#         keywords = keywords or []
#         query_parts = [part for part in [
#             f"{trade} contractors" if trade else None,
#             city if city else None,
#             state if state else None
#         ] if part]
#         query = " ".join(query_parts + keywords)
#         if not query:
#             logger.error("Empty search query - missing required parameters")
#             return []
#         candidates = []
#         seen_urls = set()
#         # 1. DuckDuckGo
#         try:
#             with DDGS() as ddgs:
#                 for r in ddgs.text(query, max_results=50):
#                     url = r.get("href") or r.get("url")
#                     title = r.get("title", "")
#                     description = r.get("body", "")
#                     if url and url.startswith("http") and url not in seen_urls:
#                         candidates.append({
#                             "url": url,
#                             "title": title,
#                             "description": description
#                         })
#                         seen_urls.add(url)
#                     if len(candidates) >= self.min_candidates:
#                         break
#         except Exception as e:
#             logger.error(f"Error during DDGS search: {str(e)}")
#         # 2. Bing (if needed)
#         if len(candidates) < self.min_candidates:
#             try:
#                 import requests
#                 bing_key = os.getenv("BING_API_KEY", "")
#                 if bing_key:
#                     headers = {"Ocp-Apim-Subscription-Key": bing_key}
#                     params = {"q": query, "count": 20}
#                     resp = requests.get("https://api.bing.microsoft.com/v7.0/search", headers=headers, params=params, timeout=10)
#                     if resp.ok:
#                         for r in resp.json().get("webPages", {}).get("value", []):
#                             url = r.get("url")
#                             title = r.get("name", "")
#                             description = r.get("snippet", "")
#                             if url and url.startswith("http") and url not in seen_urls:
#                                 candidates.append({
#                                     "url": url,
#                                     "title": title,
#                                     "description": description
#                                 })
#                                 seen_urls.add(url)
#                             if len(candidates) >= self.min_candidates:
#                                 break
#             except Exception as e:
#                 logger.error(f"Error during Bing search: {str(e)}")
#         # 3. Yelp/BBB (if needed)
#         # (Pseudo-code, as these may require scraping or API keys)
#         # Add similar blocks for Yelp, BBB, etc. as needed.
#         if len(candidates) < self.min_candidates:
#             logger.warning(f"Only found {len(candidates)} candidates, below minimum {self.min_candidates}")
#         return candidates[:self.min_candidates]

#     async def _try_primary_search(self, trade: str, city: str, state: str, keywords: List[str]) -> List[Dict]:
#         params = {
#             **self.search_sources["primary"]["params"],
#             "q": self._build_query(trade, city, state, keywords),
#             "location": f"{city}, {state}, United States",
#             "api_key": self.api_key
#         }
#         async with httpx.AsyncClient() as client:
#             response = await client.get(self.base_url, params=params)
#             return self._process_results(response.json())

#     def _build_query(self, trade: str, city: str, state: str, keywords: List[str]) -> str:
#         """
#         Builds a precision search query to find licensed Texas contractors with high accuracy.
#         Focuses on TDLR-registered businesses without making assumptions about license status.
#         """
#         # Base components
#         components = [
#             f"{trade} contractor" if trade else "",
#             city if city else "",
#             f"{state}" if state else "",
#         ]
        
#         # License and certification indicators (without hardcoding TDLR)
#         license_terms = [
#             "licensed",
#             "certified",
#             "registered",
#             "regulated",
#             "insured",
#             "bonded"
#         ]
        
#         project_terms = keywords if keywords else ["commercial"]
        
        
#         query_parts = [
#             " ".join(filter(None, components)),
#             "(" + " OR ".join(license_terms) + ")",
#             "(" + " OR ".join(project_terms) + ")"
#         ]

#         query = " ".join(filter(None, query_parts))
        
#         if state and state.upper() == "TX":
#             query = f"{query} (tdlr OR 'texas department of licensing')"
        
#         return query.strip()

#     def _process_results(self, data: Dict) -> List[Dict]:
#         return [{
#             "url": r["link"],
#             "display_url": r.get("displayed_link", ""),
#             "title": r["title"],
#             "snippet": r.get("snippet")
#         } for r in data.get("organic_results", [])[:20]]


# discovery.py
import os
import httpx
import logging
from typing import List, Dict
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import random

logger = logging.getLogger(__name__)

class DiscoveryService:
    """
    Free search engine discovery service using:
    - DuckDuckGo Lite (primary)
    - Brave Search (fallback)
    - Mojeek (secondary fallback)
    """
    def __init__(self):
        self.min_candidates = 20
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]
        self.search_engines = [
            self._search_duckduckgo_lite,
            self._search_brave,
            self._search_mojeek
        ]

    async def find_subcontractors(self, trade: str, city: str, state: str, keywords: List[str]) -> List[Dict]:
        """Find subcontractors using only free search engines"""
        query = self._build_query(trade, city, state, keywords)
        if not query:
            logger.error("Empty search query")
            return []

        candidates = []
        seen_urls = set()
        
        for search_func in self.search_engines:
            if len(candidates) >= self.min_candidates:
                break
                
            try:
                results = await search_func(query)
                for result in results:
                    url = result.get("url")
                    if url and url.startswith("http") and url not in seen_urls:
                        candidates.append({
                            "url": url,
                            "title": result.get("title", ""),
                            "description": result.get("description", "")
                        })
                        seen_urls.add(url)
            except Exception as e:
                logger.warning(f"Search failed with {search_func.__name__}: {str(e)}")
                continue

        if len(candidates) < self.min_candidates:
            logger.warning(f"Only found {len(candidates)} candidates (target: {self.min_candidates})")
        return candidates[:self.min_candidates]

    async def _search_duckduckgo_lite(self, query: str) -> List[Dict]:
        """Search using DuckDuckGo Lite (free)"""
        try:
            headers = {"User-Agent": random.choice(self.user_agents)}
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://lite.duckduckgo.com/lite/",
                    params={"q": query},
                    headers=headers,
                    timeout=15
                )
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    results = []
                    for row in soup.select(".result"):
                        link = row.select_one(".result-link")
                        if link and link.get("href"):
                            results.append({
                                "url": link["href"],
                                "title": link.text.strip(),
                                "description": row.select_one(".result-snippet").text.strip() if row.select_one(".result-snippet") else ""
                            })
                    return results
            return []
        except Exception as e:
            logger.warning(f"DuckDuckGo Lite failed: {str(e)}")
            return []

    async def _search_brave(self, query: str) -> List[Dict]:
        """Search using Brave Search (free)"""
        try:
            headers = {"User-Agent": random.choice(self.user_agents)}
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://search.brave.com/search",
                    params={"q": query},
                    headers=headers,
                    timeout=15
                )
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    return [{
                        "url": a["href"],
                        "title": a.text.strip(),
                        "description": ""
                    } for a in soup.select(".result a")[:20] if a.get("href")]
            return []
        except Exception as e:
            logger.warning(f"Brave search failed: {str(e)}")
            return []

    async def _search_mojeek(self, query: str) -> List[Dict]:
        """Search using Mojeek (privacy-focused, free)"""
        try:
            headers = {"User-Agent": random.choice(self.user_agents)}
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://www.mojeek.com/search",
                    params={"q": query},
                    headers=headers,
                    timeout=15
                )
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    return [{
                        "url": a["href"],
                        "title": a.text.strip(),
                        "description": ""
                    } for a in soup.select(".results a")[:20] if a.get("href")]
            return []
        except Exception as e:
            logger.warning(f"Mojeek search failed: {str(e)}")
            return []

    def _build_query(self, trade: str, city: str, state: str, keywords: List[str]) -> str:
        """Build optimized search query (same as before)"""
        components = [
            f"{trade} contractor" if trade else "",
            city if city else "",
            state if state else "",
        ]
        
        license_terms = ["licensed", "certified", "registered"]
        project_terms = keywords if keywords else ["commercial"]
        
        query = " ".join(filter(None, [
            " ".join(filter(None, components)),
            f"({' OR '.join(license_terms)})",
            f"({' OR '.join(project_terms)})"
        ]))
        
        if state and state.upper() == "TX":
            query += " (tdlr OR 'texas department of licensing')"
            
        return query.strip()