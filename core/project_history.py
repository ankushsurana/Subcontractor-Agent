import asyncio
import re
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Set, Optional, Tuple
from urllib.parse import urljoin, urlparse
from functools import lru_cache

import httpx
from bs4 import BeautifulSoup
from cachetools import TTLCache, cached

logger = logging.getLogger(__name__)

class ProjectHistoryParser:
    DEFAULT_TIMEOUT = 15.0
    MAX_PROJECT_LINKS = 10
    REQUEST_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    
    def __init__(self, keywords: List[str] = None, max_concurrent_requests: int = 20):
        self.keywords = set(keywords or [
            "project", "case-study", "news", "portfolio", 
            "completed", "construction", "client", "built",
            "recent", "work", "showcase", "gallery", "featured"
        ])
        
        self.tx_cities = {
            "houston", "dallas", "austin", "san antonio", "fort worth", 
            "el paso", "arlington", "corpus christi", "plano", "lubbock", 
            "irving", "garland", "amarillo", "mckinney", "frisco", 
            "waco", "denton", "midland", "odessa", "tyler", "katy",
            "college station", "galveston", "beaumont", "sugar land",
            "round rock", "killeen", "temple", "laredo", "georgetown",
            "pflugerville", "san marcos", "boerne", "cedar park", "leander",
            "conroe", "new braunfels", "bryan", "wichita falls", "richardson",
            "league city", "allen", "san angelo", "edinburg", "euless",
            "longview", "lufkin", "nacogdoches", "pearland", "the woodlands"
        }
        
        city_pattern = "|".join([rf"\b{re.escape(city)}\b" for city in self.tx_cities])
        self.tx_pattern = re.compile(r"\bTX\b|\bTexas\b|" + city_pattern, re.IGNORECASE)
        self.year_pattern = re.compile(r"\b(20\d{2}|19\d{2})\b")
        self.project_type_pattern = re.compile(
            r"\b(commercial|residential|school|hospital|bridge|road|industrial|office|retail"
            r"|mechanical|hvac|plumbing|electrical|renovation|construction|install|build|facility"
            r"|manufacturing|warehouse|plant|factory|university|college|corporate|government)\b", 
            re.IGNORECASE
        )
        
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        self.response_cache = TTLCache(maxsize=1000, ttl=3600)
        
        self.processed_urls = set()
        
        self.current_year = datetime.now().year
        self.past_5yrs = set(str(yr) for yr in range(self.current_year-5, self.current_year+1))
        self.older_years = set(str(yr) for yr in range(self.current_year-15, self.current_year-5))

    async def enrich_profiles(self, profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process profiles to add project history data efficiently"""
        if not profiles:
            logger.warning("No profiles to process in project history parser")
            return []
        
        start_time = time.time()
        
        try:
            limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
            async with httpx.AsyncClient(
                timeout=self.DEFAULT_TIMEOUT,
                limits=limits,
                headers=self.REQUEST_HEADERS,
                follow_redirects=True,  
                http2=False 
            ) as client:
                tasks = [self._enrich_profile(client, profile) for profile in profiles]
                enriched_profiles = await asyncio.gather(*tasks, return_exceptions=True)
                
                result = []
                for i, profile_or_exception in enumerate(enriched_profiles):
                    if isinstance(profile_or_exception, Exception):
                        logger.error(f"Error processing profile {i}: {str(profile_or_exception)}")
                        if i < len(profiles):
                            profiles[i]["tx_projects_past_5yrs"] = 0
                            result.append(profiles[i])
                    else:
                        result.append(profile_or_exception)
                
                compliant_profiles = [p for p in result if p.get("tx_projects_past_5yrs", 0) > 0]
                
                processing_time = time.time() - start_time
                logger.info(f"Project history enrichment completed in {processing_time:.2f}s. "
                           f"Found {len(compliant_profiles)} compliant profiles out of {len(profiles)}.")
                
                return compliant_profiles
                
        except Exception as e:
            logger.error(f"Critical error in project history enrichment: {str(e)}", exc_info=True)
            for profile in profiles:
                profile["tx_projects_past_5yrs"] = 0
            return profiles

    async def _fetch_url(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        """Fetch URL content with error handling and caching"""
        if not url or not isinstance(url, str):
            return None
            
        if url in self.processed_urls:
            logger.debug(f"Skipping already processed URL: {url}")
            return None
            
        url = url.strip()
        try:
            parsed = urlparse(url)
            if not parsed.scheme:
                url = f"https://{url}"
        except Exception:
            return None
            
        self.processed_urls.add(url)
            
        cache_key = url.strip().lower()
        if cache_key in self.response_cache:
            return self.response_cache[cache_key]
            
        try:
            async with self.semaphore:
                resp = await client.get(url, timeout=self.DEFAULT_TIMEOUT)
                
                if resp.url != url:
                    logger.info(f"HTTP Request: GET {url} redirected to {resp.url}")
                    self.processed_urls.add(str(resp.url))
                else:
                    logger.info(f"HTTP Request: GET {url} \"{resp.http_version} {resp.status_code} {resp.reason_phrase}\"")
                
                if resp.status_code == 200:
                    content_type = resp.headers.get("content-type", "")
                    if "text/html" not in content_type.lower():
                        logger.debug(f"Non-HTML content type ({content_type}) for URL: {url}")
                        return None
                        
                    html_content = resp.text
                    self.response_cache[cache_key] = html_content
                    return html_content
                elif 300 <= resp.status_code < 400:
                    logger.debug(f"Redirect status ({resp.status_code}) for URL: {url}")
                    return None
                elif resp.status_code == 403:
                    logger.warning(f"Access forbidden (403) for URL: {url}")
                    return None
                elif resp.status_code == 404:
                    logger.debug(f"Page not found (404) for URL: {url}")
                    return None
                elif 400 <= resp.status_code < 500:
                    logger.warning(f"Client error ({resp.status_code}) for URL: {url}")
                    return None
                elif 500 <= resp.status_code < 600:
                    logger.warning(f"Server error ({resp.status_code}) for URL: {url}")
                    return None
                else:
                    logger.warning(f"Unexpected status code ({resp.status_code}) for URL: {url}")
                    return None
                    
        except httpx.RequestError as e:
            logger.debug(f"Request error fetching {url}: {str(e)}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error fetching {url}: {str(e)}")
            return None

    @lru_cache(maxsize=100)
    def _extract_keywords_from_text(self, text: str) -> bool:
        """Check if any keywords are present in text (cached)"""
        if not text:
            return False
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.keywords)

    def _extract_project_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract project-related links from HTML"""
        if not soup:
            return []
            
        links = set()
        
        nav_elements = soup.find_all(['nav', 'header', 'div'], class_=lambda c: c and any(
            term in (c.lower() if c else "") for term in ['nav', 'menu', 'header']
        ))
        
        elements_to_search = nav_elements + [soup] 
        
        for element in elements_to_search:
            for a in element.find_all("a", href=True):
                href = a.get("href", "").strip()
                if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                    continue
                    
                if any(domain in href.lower() for domain in ['facebook.com', 'twitter.com', 'linkedin.com', 
                                                            'instagram.com', 'youtube.com', 'pinterest.com']):
                    continue
                    
                link_text = a.get_text(" ", strip=True).lower()
                
                is_project_link = any(k in href.lower() for k in self.keywords)
                is_project_text = any(k in link_text for k in self.keywords)
                
                if is_project_link or is_project_text:
                    try:
                        abs_link = urljoin(base_url, href)
                        base_domain = urlparse(base_url).netloc
                        link_domain = urlparse(abs_link).netloc
                        
                        if link_domain == base_domain and abs_link != base_url:
                            links.add(abs_link)
                    except Exception:
                        continue
        
        return list(links)[:self.MAX_PROJECT_LINKS]

    def _check_for_texas_content(self, text: str) -> bool:
        """Check if text contains Texas-related terms"""
        if not text:
            return False
        return bool(re.search(self.tx_pattern, text))

    def _extract_years(self, text: str) -> Tuple[Set[str], Set[str]]:
        """Extract years from text and return found recent and older years"""
        if not text:
            return set(), set()
            
        all_years = re.findall(self.year_pattern, text)
        recent_years = set(year for year in all_years if year in self.past_5yrs)
        older_years = set(year for year in all_years if year in self.older_years)
        
        return recent_years, older_years

    def _has_project_type_keywords(self, text: str) -> bool:
        """Check if text contains project type keywords"""
        if not text:
            return False
        return bool(re.search(self.project_type_pattern, text))

    def _extract_snippet(self, text: str, tx_match_position: int, year: str = None) -> str:
        if not text:
            return ""
            
        if tx_match_position < 0:
            if year:
                year_match = re.search(fr"\b{year}\b", text)
                if year_match:
                    tx_match_position = year_match.start()
                    
        if tx_match_position < 0:
            start = 0
            end = min(len(text), 300)
            return text[start:end]
            
        start = max(0, tx_match_position - 100)
        end = min(len(text), tx_match_position + 200)
        
        text_portion = text[start:end]
        
        tx_match = re.search(self.tx_pattern, text_portion)
        if tx_match:
            start_pos, end_pos = tx_match.span()
            tx_term = text_portion[start_pos:end_pos]
            text_portion = text_portion[:start_pos] + f"[{tx_term}]" + text_portion[end_pos:]
        
        for year in sorted(list(self.past_5yrs) + list(self.older_years), reverse=True):
            if year in text_portion:
                text_portion = text_portion.replace(year, f"[{year}]")
                
        return text_portion

    async def _process_project_page(
        self, 
        client: httpx.AsyncClient, 
        url: str, 
        evidence_list: List[Dict]
    ) -> Tuple[int, int]:
        tx_recent_count = 0
        tx_older_count = 0
        
        html_content = await self._fetch_url(client, url)
        if not html_content:
            return 0, 0
            
        soup = BeautifulSoup(html_content, "html.parser")
        project_text = soup.get_text(" ", strip=True)
        
        if not self._extract_keywords_from_text(project_text):
            return 0, 0
            
        has_project_keywords = self._has_project_type_keywords(project_text)
        
        is_tx_project = self._check_for_texas_content(project_text)
        if not is_tx_project:
            return 0, 0
            
        recent_years, older_years = self._extract_years(project_text)
        
        if recent_years:
            tx_recent_count = 1
            
            tx_pos = -1
            tx_match = re.search(self.tx_pattern, project_text)
            if tx_match:
                tx_pos = tx_match.start()
            
            recent_year = sorted(recent_years, reverse=True)[0] if recent_years else None
            snippet = self._extract_snippet(project_text, tx_pos, recent_year)
            
            evidence_quality = 1  
            evidence_quality += 1 if has_project_keywords else 0
            evidence_quality += 1 if len(recent_years) > 1 else 0  
            evidence_quality += 1 if tx_match else 0 
            
            evidence_list.append({
                "url": url,
                "text": snippet,
                "recent": True,
                "texas": True,
                "years": list(recent_years),
                "quality": min(5, evidence_quality)  # Scale from 1-5
            })
        elif older_years:
            tx_older_count = 1
            
        return tx_recent_count, tx_older_count

    async def _enrich_profile(self, client: httpx.AsyncClient, profile: Dict[str, Any]) -> Dict[str, Any]:
        if not profile or not isinstance(profile, dict):
            logger.warning("[Projects] Invalid profile passed to project history parser")
            return {"tx_projects_past_5yrs": 0}
            
        url = profile.get("website") or profile.get("source_url")
        if not url:
            profile["tx_projects_past_5yrs"] = 0
            return profile
            
        self.processed_urls = set()
            
        try:
            tx_recent_count = 0
            tx_older_count = 0
            project_evidence = []
            
            html_content = await self._fetch_url(client, url)
            if not html_content:
                profile["tx_projects_past_5yrs"] = 0
                return profile
                
            soup = BeautifulSoup(html_content, "html.parser")
            main_text = soup.get_text(" ", strip=True)
            
            has_tx = self._check_for_texas_content(main_text)
            recent_years, older_years = self._extract_years(main_text)
            
            project_links = self._extract_project_links(soup, url)
            
            if has_tx and recent_years:
                has_project_keywords = self._has_project_type_keywords(main_text)
                if self._extract_keywords_from_text(main_text) or has_project_keywords:
                    for tx_match in re.finditer(self.tx_pattern, main_text):
                        tx_pos = tx_match.start()
                        nearby_text = main_text[max(0, tx_pos-150):min(len(main_text), tx_pos+250)]
                        
                        nearby_years = set()
                        for year in recent_years:
                            if year in nearby_text:
                                nearby_years.add(year)
                                
                        if nearby_years:
                            tx_recent_count += 1
                            
                            evidence_quality = 1 
                            evidence_quality += 1 if has_project_keywords else 0
                            evidence_quality += 1 if len(nearby_years) > 1 else 0  
                            evidence_quality += 1 if self._extract_keywords_from_text(nearby_text) else 0
                            
                            snippet = self._extract_snippet(nearby_text, 0, sorted(nearby_years, reverse=True)[0])
                            
                            project_evidence.append({
                                "url": url,
                                "text": snippet,
                                "recent": True,
                                "texas": True,
                                "years": list(nearby_years),
                                "quality": min(5, evidence_quality)  
                            })
            
            if project_links:
                link_tasks = []
                for link in project_links:
                    task = self._process_project_page(client, link, project_evidence)
                    link_tasks.append(task)
                
                batch_size = 3
                for i in range(0, len(link_tasks), batch_size):
                    batch_results = await asyncio.gather(*link_tasks[i:i+batch_size], return_exceptions=True)
                    
                    for result in batch_results:
                        if isinstance(result, Exception):
                            logger.debug(f"Error processing project link: {str(result)}")
                            continue
                        
                        recent_count, older_count = result
                        tx_recent_count += recent_count
                        tx_older_count += older_count
            
            if tx_recent_count == 0:
                sections = []
                
                project_section_keywords = ['project', 'portfolio', 'work', 'case', 'gallery']
                for keyword in project_section_keywords:
                    sections.extend(soup.find_all(['div', 'section', 'article'], 
                                              class_=lambda c: c and keyword in (c.lower() if c else ""), 
                                              id=lambda i: i and keyword in (i.lower() if i else "")))
                
                if not sections:
                    sections = soup.find_all(['div', 'section', 'article'])
                
                for section in sections[:20]: 
                    section_text = section.get_text(" ", strip=True)
                    
                    if self._check_for_texas_content(section_text):
                        recent_yrs, _ = self._extract_years(section_text)
                        
                        if recent_yrs and (self._extract_keywords_from_text(section_text) or 
                                          self._has_project_type_keywords(section_text)):
                            tx_recent_count += 1
                            
                            evidence_quality = 1
                            evidence_quality += 1 if self._has_project_type_keywords(section_text) else 0
                            evidence_quality += 1 if len(recent_yrs) > 1 else 0  
                            
                            tx_pos = -1
                            tx_match = re.search(self.tx_pattern, section_text)
                            if tx_match:
                                tx_pos = tx_match.start()
                                
                            recent_year = sorted(recent_yrs, reverse=True)[0] if recent_yrs else None
                            snippet = self._extract_snippet(section_text, tx_pos, recent_year)
                            
                            project_evidence.append({
                                "url": url,
                                "text": snippet,
                                "recent": True,
                                "texas": True,
                                "years": list(recent_yrs),
                                "quality": min(5, evidence_quality)
                            })
            
            project_evidence.sort(key=lambda x: x.get('quality', 1), reverse=True)
            
            profile["tx_projects_past_5yrs"] = max(1, tx_recent_count) if tx_recent_count > 0 else 0
            profile["tx_older_projects"] = tx_older_count
            profile["project_evidence"] = project_evidence
            
            return profile
            
        except Exception as e:
            logger.error(f"[Projects] Error processing profile with URL {url}: {str(e)}")
            profile["tx_projects_past_5yrs"] = 0
            return profile