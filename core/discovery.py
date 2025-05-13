import asyncio
import httpx
import logging
import random
import re
from urllib.parse import urlparse, quote
from typing import List, Dict
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class DiscoveryService:
    """
    Free Google-search compatible discovery service using:
    - Startpage (Google results proxy)
    - Brave Search
    """
    def __init__(self):
        self.min_candidates = 20
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        ]
        self.search_engines = [
            self._search_brave 
        ]
        self.domain_blacklist = {
            'terms', 'privacy', 'contact', 'blog', 
            'wikipedia', 'facebook', 'indeed', 'ziprecruiter', 'linkedin', "yelp"
        }
        self.timeout = 20
        self.max_retries = 2

    async def find_subcontractors(self, trade: str, city: str, state: str, keywords: List[str]) -> List[Dict]:
        """Main discovery method with enhanced Google-style search"""
        query = self._build_google_style_query(trade, city, state, keywords)
        if not query:
            logger.error("Invalid search query")
            return []

        candidates = []
        seen_domains = set()
        
        for search_func in self.search_engines:
            if len(candidates) >= self.min_candidates:
                break
                
            try:
                results = await self._retry_search(search_func, query)
                for result in results:
                    url = self._normalize_url(result.get("url"))
                    if self._is_valid_contractor_url(url, seen_domains):
                        candidates.append({
                            "url": url,
                            "title": result.get("title", ""),
                            "description": result.get("description", ""),
                            "source": search_func.__name__
                        })
                        seen_domains.add(urlparse(url).netloc)
            except Exception as e:
                logger.warning(f"Search failed with {search_func.__name__}: {str(e)}")
                continue

        logger.info(f"Found {len(candidates)} valid candidates from {len(seen_domains)} unique domains")
        return candidates[:self.min_candidates]

    def _build_google_style_query(self, trade: str, city: str, state: str, keywords: List[str]) -> str:
        """Builds Google-style search queries with precision operators"""
        components = []
        
        if trade:
            components.append(f'"{trade} contractor"')
        
        if city and state:
            components.append(f'"{city}, {state}"')
        elif state:
            components.append(f'"{state}"')

        if keywords:
            components.append(f"({' OR '.join(keywords)})")

        
        return " ".join(components)

    async def _retry_search(self, search_func, query: str) -> List[Dict]:
        """Retry failed searches with exponential backoff"""
        for attempt in range(self.max_retries):
            try:
                return await search_func(query)
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"Retry {attempt + 1} for {search_func.__name__} after {wait_time}s")
                await asyncio.sleep(wait_time)
        return []

    def _normalize_url(self, url: str) -> str:
        """Standardize URLs to root domains"""
        if not url:
            return ""
        try:
            parsed = urlparse(url)
            if not parsed.netloc:
                return ""
            domain = parsed.netloc.lower()
            if domain.startswith("www."):
                domain = domain[4:]
            return f"{parsed.scheme or 'https'}://{domain}"
        except:
            return ""

    def _is_valid_contractor_url(self, url: str, seen_domains: set) -> bool:
        """Updated validation for contractor URLs"""
        if not url.startswith(("http://", "https://")):
            return False
            
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        
        # Allow subdomains and newer TLDs
        domain_pattern = r"^([a-z0-9-]+\.)*[a-z0-9-]+\.[a-z]{2,}(\.[a-z]{2,})?$"
        
        return (
            domain not in seen_domains and
            not any(bad in domain for bad in self.domain_blacklist) and
            re.match(domain_pattern, domain) and
            not any(part in parsed.path.lower() for part in ['/terms', '/privacy'])
        ) 
            
    async def _search_brave(self, query: str) -> List[Dict]:
        """Search Brave with improved selectors for 2025 structure"""
        try:
            headers = {
                "User-Agent": random.choice(self.user_agents),
                "Accept-Language": "en-US,en;q=0.9"
            }
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://search.brave.com/search",
                    params={"q": query},
                    headers=headers,
                    timeout=self.timeout
                )
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    results = []
                    
                    # Target main organic results
                    for result in soup.select('[data-type="web"]'):
                        # Extract URL
                        link = result.select_one('a[href]')
                        if not link:
                            continue
                        
                        # Extract title
                        title = link.get_text(strip=True)
                        
                        # Extract description
                        description = ""
                        desc_elem = result.select_one('.snippet-content, .snippet-description')
                        if desc_elem:
                            description = desc_elem.get_text(strip=True)
                        
                        results.append({
                            "url": link['href'],
                            "title": title,
                            "description": description
                        })
                    
                    return results[:20]  # Return first 20 results
                    
        except Exception as e:
            logger.error(f"Brave search failed: {str(e)}")
        return []
