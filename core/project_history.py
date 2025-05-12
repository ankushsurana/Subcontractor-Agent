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

            for profile in profiles:
                profile["tx_projects_past_5yrs"] = 0
            return profiles

    async def _enrich_profile(self, client: httpx.AsyncClient, profile: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single profile with project history data.
        Extracts project information and analyzes Texas-specific projects.
        """
        # Ensure profile is not None and is a dict
        if not profile or not isinstance(profile, dict):
            logger.warning("[Projects] Invalid profile passed to project history parser")
            return {"tx_projects_past_5yrs": 0}
            
        # Get URL from profile, with fallbacks
        url = profile.get("website") or profile.get("source_url")
        if not url:
            logger.warning("[Projects] Profile has no website URL for project extraction")
            profile["tx_projects_past_5yrs"] = 0
            return profile
            
        try:
            # Add debug logging
            logger.info(f"[Projects] Processing project history for URL: {url}")
            
            # Get main site content
            resp = await client.get(url, follow_redirects=True)
            soup = BeautifulSoup(resp.text, "lxml")
            
            # Look for project-related links
            links = [a.get("href") for a in soup.find_all("a", href=True)]
            
            project_links = []
            for link in links:
                if not link or not isinstance(link, str):
                    continue
                    
                # Check if link contains project-related keywords
                if any(k in link.lower() for k in self.keywords):
                    # Convert relative links to absolute
                    from urllib.parse import urljoin
                    abs_link = urljoin(url, link)
                    project_links.append(abs_link)
            
            # Remove duplicates and limit to 5 project links
            project_links = list(set(project_links))[:5]
            logger.info(f"[Projects] Found {len(project_links)} potential project links")
            
            # Initialize counters and evidence
            tx_recent_count = 0
            tx_older_count = 0
            project_evidence = []
            
            # Process each project link
            for link in project_links:
                try:
                    logger.info(f"[Projects] Analyzing project page: {link}")
                    
                    # Get project page content
                    resp = await client.get(link, follow_redirects=True, timeout=10.0)
                    if resp.status_code != 200:
                        continue
                        
                    project_soup = BeautifulSoup(resp.text, "lxml")
                    project_text = project_soup.get_text(" ", strip=True)
                    
                    # Check for Texas references
                    is_tx_project = any(
                        term in project_text.lower() 
                        for term in ["texas", " tx ", "tx,", "dallas", "houston", "austin", "san antonio"]
                    )
                    
                    # Check for recent dates (last 5 years)
                    has_recent_date = False
                    current_year = datetime.now().year
                    past_5yrs = [str(yr) for yr in range(current_year-5, current_year+1)]
                    
                    # Look for year mentions (e.g., "2023", "Completed in 2022")
                    for year in past_5yrs:
                        if year in project_text:
                            has_recent_date = True
                            break
                            
                    # Track project counts
                    if is_tx_project:
                        if has_recent_date:
                            tx_recent_count += 1
                            
                            # Save evidence snippet for reference
                            project_evidence.append({
                                "url": link,
                                "text": project_text[:300],
                                "recent": True,
                                "texas": True
                            })
                        else:
                            tx_older_count += 1
                            
                    logger.info(f"[Projects] Link analysis: TX={is_tx_project}, Recent={has_recent_date}")
                        
                except Exception as e:
                    logger.warning(f"[Projects] Error processing project link {link}: {str(e)}")
                    continue
            
            # Also check main page for project mentions
            main_text = soup.get_text(" ", strip=True).lower()
            for texas_term in ["texas", " tx ", "tx,", "dallas", "houston", "austin", "san antonio"]:
                if texas_term in main_text:
                    for year in past_5yrs:
                        if year in main_text:
                            text_snippet = main_text[max(0, main_text.find(texas_term)-50):main_text.find(texas_term)+150]
                            project_evidence.append({
                                "url": url,
                                "text": text_snippet,
                                "recent": True,
                                "texas": True
                            })
                            tx_recent_count += 1
                            break
                    break
            
            # Update profile with project history data
            profile["tx_projects_past_5yrs"] = tx_recent_count
            profile["tx_older_projects"] = tx_older_count
            profile["project_evidence"] = project_evidence
            
            logger.info(f"[Projects] Final counts - Recent TX projects: {tx_recent_count}, Older TX projects: {tx_older_count}")
            
            return profile
        except Exception as e:
            logger.error(f"[Projects] Error in project history enrichment: {str(e)}")
            # Return original profile with zero projects on error
            profile["tx_projects_past_5yrs"] = 0
            return profile