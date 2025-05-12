# project_history.py
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

    async def enrich_profiles(self, profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process profiles to add project history data"""
        if not profiles:
            logger.warning("No profiles to process in project history parser")
            return []
        
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                tasks = [self._enrich_profile(client, profile) for profile in profiles]
                enriched_profiles = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results, keeping original profiles if exceptions occurred
                result = []
                for i, profile_or_exception in enumerate(enriched_profiles):
                    if isinstance(profile_or_exception, Exception):
                        logger.error(f"Error processing profile: {str(profile_or_exception)}")
                        # Keep original profile
                        if i < len(profiles):
                            profiles[i]["tx_projects_past_5yrs"] = 0
                            result.append(profiles[i])
                    else:
                        result.append(profile_or_exception)
                
                logger.info(f"Project history parser processed {len(result)} profiles")
                return result
        except Exception as e:
            logger.error(f"Error in project history enrichment: {str(e)}")
            # If all else fails, return original profiles
            for profile in profiles:
                profile["tx_projects_past_5yrs"] = 0
            return profiles

    async def _enrich_profile(self, client: httpx.AsyncClient, profile: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a single profile with project history data"""
        # Ensure profile is not None and is a dict
        if not profile or not isinstance(profile, dict):
            logger.warning("Invalid profile passed to project history parser")
            return {"tx_projects_past_5yrs": 0}
            
        # Get URL from profile, with fallbacks
        url = profile.get("website") or profile.get("source_url")
        if not url:
            profile["tx_projects_past_5yrs"] = 0
            return profile
            
        try:
            # Add debug logging
            logger.info(f"Processing project history for URL: {url}")
            
            resp = await client.get(url)
            soup = BeautifulSoup(resp.text, "lxml")
            links = [a.get("href") for a in soup.find_all("a", href=True)]
            
            # Filter links for project/news/case-study keywords
            project_links = [l for l in links if l and isinstance(l, str) and any(k in l.lower() for k in self.keywords)]
            
            # Make absolute URLs
            from urllib.parse import urljoin
            project_links = [urljoin(url, l) for l in project_links]
            
            # Debug links found
            logger.info(f"Found {len(project_links)} potential project links")
            
            # Limit to 5 project pages per profile
            project_links = project_links[:5]
            
            # Scrape and parse each project page
            tx_recent_count = 0
            project_evidence = []
            
            for plink in project_links:
                try:
                    presp = await client.get(plink)
                    if presp.status_code != 200:
                        continue
                        
                    ptext = presp.text
                    # Find all years
                    years = [int(y) for y in self.year_pattern.findall(ptext)]
                    # Find all states
                    states = self.state_pattern.findall(ptext)
                    # Find project types
                    types = self.project_type_pattern.findall(ptext)
                    
                    # Count if Texas and year in last 5 years
                    for y in years:
                        if self.current_year - y <= 5 and states:
                            tx_recent_count += 1
                            # Add evidence
                            evidence = {
                                "url": plink,
                                "year": y,
                                "type": types[0] if types else "unknown"
                            }
                            project_evidence.append(evidence)
                except Exception as e:
                    logger.warning(f"Error scraping project page {plink}: {e}")
            
            # Add data to profile
            profile["tx_projects_past_5yrs"] = tx_recent_count
            if project_evidence:
                profile["project_evidence"] = project_evidence[:3]  # Store top 3 project evidences
            
            logger.info(f"Found {tx_recent_count} Texas projects in past 5 years")
            return profile
            
        except Exception as e:
            logger.warning(f"Error scraping main page {url}: {e}")
            profile["tx_projects_past_5yrs"] = 0
            return profile