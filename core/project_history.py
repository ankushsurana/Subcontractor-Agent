import asyncio
import re
import logging
from datetime import datetime
from typing import List, Dict, Any
import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class ProjectHistoryParser: 
    """
    Parses project/news/case-study pages for each profile, looking for year, state, project-type keywords.
    Enriches each profile with project history info, including count of Texas projects in the last 5 years.
    """
    def __init__(self, keywords: List[str] = None):
        self.keywords = keywords or ["project", "case-study", "news", "portfolio", "completed", "construction"]
        self.year_pattern = re.compile(r"\b(20\d{2}|19\d{2})\b")
        self.state_pattern = re.compile(r"\bTX\b|Texas", re.IGNORECASE)
        self.project_type_pattern = re.compile(r"commercial|residential|school|hospital|bridge|road|industrial|office|retail", re.IGNORECASE)
        self.current_year = datetime.utcnow().year
        
        self.tx_cities = ["houston", "dallas", "austin", "san antonio", "fort worth", "el paso", 
                         "arlington", "corpus christi", "plano", "lubbock", "irving", "garland",
                         "amarillo", "mckinney", "frisco", "waco", "denton", "midland", "odessa"]
        self.tx_pattern = re.compile(r"\bTX\b|\bTexas\b|" + "|".join([rf"\b{city}\b" for city in self.tx_cities]), 
                                     re.IGNORECASE)

    async def enrich_profiles(self, profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process profiles to add project history data"""
        if not profiles:
            logger.warning("No profiles to process in project history parser")
            return []
        
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                tasks = [self._enrich_profile(client, profile) for profile in profiles]
                enriched_profiles = await asyncio.gather(*tasks, return_exceptions=True)
                
                result = []
                for i, profile_or_exception in enumerate(enriched_profiles):
                    if isinstance(profile_or_exception, Exception):
                        logger.error(f"Error processing profile: {str(profile_or_exception)}")
                        if i < len(profiles):
                            profiles[i]["tx_projects_past_5yrs"] = 0
                            result.append(profiles[i])
                    else:
                        result.append(profile_or_exception)
                
                logger.info(f"Project history parser processed {len(result)} profiles")
                
                fr_compliant_profiles = [p for p in result if p.get("tx_projects_past_5yrs", 0) > 0]
                logger.info(f"Found {len(fr_compliant_profiles)} candidates with recent Texas projects")
                
                return fr_compliant_profiles
        except Exception as e:
            logger.error(f"Error in project history enrichment: {str(e)}")

            for profile in profiles:
                profile["tx_projects_past_5yrs"] = 0
            
            return []

    async def _enrich_profile(self, client: httpx.AsyncClient, profile: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single profile with project history data.
        Extracts project information and analyzes Texas-specific projects.
        """
        if not profile or not isinstance(profile, dict):
            logger.warning("[Projects] Invalid profile passed to project history parser")
            return {"tx_projects_past_5yrs": 0}
            
        url = profile.get("website") or profile.get("source_url")
        if not url:
            logger.warning("[Projects] Profile has no website URL for project extraction")
            profile["tx_projects_past_5yrs"] = 0
            return profile
            
        try:
            logger.info(f"[Projects] Processing project history for URL: {url}")
            
            tx_recent_count = 0
            tx_older_count = 0
            project_evidence = []
            current_year = datetime.now().year
            past_5yrs = [str(yr) for yr in range(current_year-5, current_year+1)]
            
            try:
                resp = await client.get(url, follow_redirects=True, timeout=20)
                soup = BeautifulSoup(resp.text, "lxml")
                main_text = soup.get_text(" ", strip=True)
                
                has_tx = bool(re.search(self.tx_pattern, main_text))
                has_recent_year = any(year in main_text for year in past_5yrs)
                
                if has_tx and has_recent_year:
                    if any(k in main_text.lower() for k in self.keywords):
                        for tx_match in re.finditer(self.tx_pattern, main_text):
                            tx_pos = tx_match.start()
                            nearby_text = main_text[max(0, tx_pos-100):min(len(main_text), tx_pos+200)]
                            
                            if any(year in nearby_text for year in past_5yrs):
                                tx_recent_count += 1
                                project_evidence.append({
                                    "url": url,
                                    "text": nearby_text[:300],
                                    "recent": True,
                                    "texas": True
                                })
                
                links = []
                for a in soup.find_all("a", href=True):
                    href = a.get("href")
                    link_text = a.get_text().lower()
                    
                    if href and any(k in href.lower() for k in self.keywords):
                        links.append(href)
                    elif link_text and any(k in link_text for k in self.keywords):
                        links.append(href)
                
                project_links = []
                for link in links:
                    if not link or not isinstance(link, str):
                        continue
                    
                    from urllib.parse import urljoin
                    abs_link = urljoin(url, link)
                    if abs_link != url: 
                        project_links.append(abs_link)
                
                project_links = list(set(project_links))[:15] 
                logger.info(f"[Projects] Found {len(project_links)} potential project links")
                
                for link in project_links:
                    try:
                        resp = await client.get(link, follow_redirects=True, timeout=10.0)
                        if resp.status_code != 200:
                            continue
                        
                        project_soup = BeautifulSoup(resp.text, "lxml")
                        project_text = project_soup.get_text(" ", strip=True)
                        
                        tx_terms = ["texas", "tx", "dallas", "houston", "austin", "san antonio", 
                                   "fort worth", "el paso", "arlington", "plano", "irving"]
                        
                        is_tx_project = False
                        for term in tx_terms:
                            if re.search(r'\b' + re.escape(term) + r'\b', project_text, re.IGNORECASE):
                                is_tx_project = True
                                break
                        
                        if is_tx_project:
                            has_recent_year = False
                            for year in past_5yrs:
                                if year in project_text:
                                    has_recent_year = True
                                    break
                            
                            if has_recent_year:
                                tx_recent_count += 1
                                
                                tx_pos = -1
                                for term in tx_terms:
                                    match = re.search(r'\b' + re.escape(term) + r'\b', project_text, re.IGNORECASE)
                                    if match:
                                        tx_pos = match.start()
                                        break
                                
                                if tx_pos >= 0:
                                    snippet = project_text[max(0, tx_pos-100):min(len(project_text), tx_pos+200)]
                                else:
                                    snippet = project_text[:300]
                                
                                project_evidence.append({
                                    "url": link,
                                    "text": snippet,
                                    "recent": True,
                                    "texas": True
                                })
                            else:
                                older_years = [str(yr) for yr in range(current_year-15, current_year-5)]
                                if any(year in project_text for year in older_years):
                                    tx_older_count += 1
                    except Exception as e:
                        logger.warning(f"[Projects] Error processing project link {link}: {str(e)}")
                        continue
                
            except Exception as e:
                logger.error(f"[Projects] Error processing main page {url}: {str(e)}")
            
            if tx_recent_count == 0 and soup is not None:
                sections = soup.find_all(['div', 'section', 'article'])
                for section in sections:
                    section_text = section.get_text(" ", strip=True)
                    
                    if re.search(self.tx_pattern, section_text) and any(year in section_text for year in past_5yrs):
                        if any(k in section_text.lower() for k in self.keywords + ["client", "completed", "built"]):
                            tx_recent_count += 1
                            project_evidence.append({
                                "url": url,
                                "text": section_text[:300],
                                "recent": True,
                                "texas": True
                            })
            
        
            profile["tx_projects_past_5yrs"] = max(1, tx_recent_count) if tx_recent_count > 0 else 0
            profile["tx_older_projects"] = tx_older_count
            profile["project_evidence"] = project_evidence
            
            logger.info(f"[Projects] Final counts - Recent TX projects: {profile['tx_projects_past_5yrs']}, Older TX projects: {tx_older_count}")
            
            return profile
        except Exception as e:
            logger.error(f"[Projects] Error in project history enrichment: {str(e)}")
            profile["tx_projects_past_5yrs"] = 0
            return profile