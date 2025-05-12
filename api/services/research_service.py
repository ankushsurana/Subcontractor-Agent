from datetime import datetime
from typing import List, Dict, Optional, Any
import logging
import traceback
from core.discovery import DiscoveryService
from core.extractor import SubcontractorExtractor
from core.license import LicenseVerifier
from core.project_history import ProjectHistoryParser
from models.schemas import ResearchResult

logger = logging.getLogger(__name__)

class ResearchOrchestrator:
    def __init__(self):
        self.discovery = DiscoveryService()
        self.extractor = SubcontractorExtractor()
        self.verifier = LicenseVerifier()
        self.project_history = ProjectHistoryParser()

    async def execute_research(self, request: dict) -> List[ResearchResult]:
        """
        Main orchestration method to perform subcontractor research.
        """
        try:
            logger.info(f"Starting research for {request.get('trade')} in {request.get('city')}, {request.get('state')}")
            
            # Phase 1: Discovery
            try:
                candidates = await self.discovery.find_subcontractors(
                    request.get("trade", ""),
                    request.get("city", ""), 
                    request.get("state", ""),
                    request.get("keywords", [])
                )
                
                logger.info(f"Discovery phase found {len(candidates)} candidates")
                
                if not candidates:
                    logger.warning("No candidates found during discovery phase")
                    return []
            except Exception as e:
                logger.error(f"Error in discovery phase: {str(e)}")
                logger.error(traceback.format_exc())
                return []
            
            # Phase 2: Profile Extraction (FR3)
            try:
                urls = [c.get("url", "") for c in candidates if c.get("url")]
                logger.info(f"Extracted {len(urls)} URLs from candidates")
                
                if not urls:
                    logger.warning("No valid URLs found in candidates")
                    return []
                    
                raw_profiles = await self.extractor.extract_profiles(urls)
                logger.info(f"Profile extraction found {len(raw_profiles)} profiles")
                
                if not raw_profiles:
                    logger.warning("No profiles extracted")
                    return []
            except Exception as e:
                logger.error(f"Error in profile extraction phase: {str(e)}")
                logger.error(traceback.format_exc())
                return []

            # Phase 3: Project History Parsing (FR4)
            try:
                logger.info(f"Starting project history parsing for {len(raw_profiles)} profiles")
                enriched_profiles = await self.project_history.enrich_profiles(raw_profiles)
                logger.info(f"Project history parsing enriched {len(enriched_profiles)} profiles")
                
                if not enriched_profiles:
                    logger.warning("No profiles after enrichment, using raw profiles")
                    enriched_profiles = raw_profiles
            except Exception as e:
                logger.error(f"Error in project history phase: {str(e)}")
                logger.error(traceback.format_exc())
                # Continue with raw profiles if history parsing fails
                enriched_profiles = raw_profiles

            # Phase 4: License Verification (FR2)
            try:
                verified_profiles = await self.verifier.verify_batch(enriched_profiles)
                logger.info(f"License verification processed {len(verified_profiles)} profiles")
                
                if not verified_profiles:
                    logger.warning("No profiles after verification")
                    # Still continue with formatting
            except Exception as e:
                logger.error(f"Error in license verification phase: {str(e)}")
                logger.error(traceback.format_exc())
                # Continue with unverified profiles
                verified_profiles = enriched_profiles
            
            # Phase 5: Result Formatting
            try:
                results = self._format_results(verified_profiles, request)
                logger.info(f"Formatted {len(results)} final results")
            except Exception as e:
                logger.error(f"Error in result formatting: {str(e)}")
                logger.error(traceback.format_exc())
                return []
            
            logger.info(f"Research complete: Found {len(results)} qualified subcontractors")
            return results
            
        except Exception as e:
            logger.error(f"Research orchestration failed: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def _format_results(self, profiles: List[Dict], request: dict) -> List[ResearchResult]:
        """
        Format raw profiles into the expected ResearchResult schema.
        """
        results = []
        for profile in profiles:
            try:
                # Skip profiles with insufficient data
                if not profile.get("name") and not profile.get("website") and not profile.get("source_url"):
                    logger.debug(f"Skipping profile with insufficient data: {profile}")
                    continue
                    
                # Ensure all values have appropriate defaults and types
                name = str(profile.get("name", "Unknown"))
                website = str(profile.get("website") or profile.get("source_url", ""))
                city = str(request.get("city", ""))
                state = str(request.get("state", ""))
                lic_active = bool(profile.get("lic_active", False))
                lic_number = str(profile.get("license") or profile.get("lic_number", "Unknown"))
                
                # Convert nullable numeric values
                bond_amount = self._parse_bond_amount(profile, request.get("min_bond", 0))
                tx_projects = profile.get("tx_projects_past_5yrs", 0)
                if not isinstance(tx_projects, int):
                    tx_projects = 0
                score = self._calculate_score(profile, request)
                
                # Handle text values safely
                evidence_url = str(profile.get("website") or profile.get("source_url", ""))
                raw_text = profile.get("raw_text") or profile.get("evidence_text", "")
                evidence_text = str(raw_text)[:500] if raw_text else ""
                
                result = ResearchResult(
                    name=name,
                    website=website,
                    city=city,
                    state=state,
                    lic_active=lic_active,
                    lic_number=lic_number,
                    bond_amount=bond_amount,
                    tx_projects_past_5yrs=tx_projects,
                    score=score,
                    evidence_url=evidence_url,
                    evidence_text=evidence_text,
                    last_checked=datetime.utcnow().isoformat()
                )
                results.append(result)
            except Exception as e:
                logger.error(f"Error formatting result: {str(e)}")
                
        return results

    def _count_tx_projects(self, profile: Dict) -> int:
        """Count verified TX projects from last 5 years"""
        # Use the analyzed count if available (F-4)
        if "tx_projects_past_5yrs" in profile:
            return int(profile["tx_projects_past_5yrs"])
            
        # Fallback to simple count if no analysis was done
        projects = profile.get("projects", [])
        if isinstance(projects, list):
            return len(projects)
        return 0

    def _get_evidence_text(self, profile: Dict) -> str:
        """Extract evidence text with project context"""
        evidence = []
        
        # Include base evidence if available
        if profile.get("raw_text"):
            evidence.append(str(profile["raw_text"])[:300])
            
        # Include project evidence snippets
        if "project_evidence" in profile:
            for item in profile["project_evidence"][:2]:  # Max 2 project snippets
                evidence.append(item["text"][:200])
                
        return " [...] ".join(evidence)[:500]  # Concatenate with length limit

    def _calculate_score(self, profile: Dict, request: Dict) -> int:
        """Enhanced scoring with project history weighting"""
        score = 0
        
        # Geographic match (30 points max)
        if profile.get("state") == request["state"]:
            score += 20
            if profile.get("city") == request["city"]:
                score += 10
                
        # License status (20 points)
        if profile.get("lic_active"):
            score += 20
            
        # Bonding capacity (30 points)
        bond_amount = profile.get("bond_amount", 0)
        min_bond = request.get("min_bond", 0)
        
        if bond_amount >= min_bond:
            score += 30
        elif bond_amount >= min_bond * 0.5:
            score += 15
            
        # Project history (20 points) - now using verified TX projects
        tx_projects = self._count_tx_projects(profile)
        score += min(20, tx_projects * 5)  # 4 projects = max 20 points
        
        # Keyword matching (additional points)
        text = f"{profile.get('name', '')} {profile.get('evidence_text', '')}".lower()
        for keyword in request.get("keywords", []):
            if keyword.lower() in text:
                score += 5
                
        return min(100, score)  # Cap at 100

    def _parse_bond_amount(self, profile: Dict, min_bond: int) -> int:
        """Parse bonding capacity with evidence validation"""
        # Prefer explicitly parsed amounts
        if isinstance(profile.get("bond_amount"), (int, float)):
            return int(profile["bond_amount"])
            
        # Fallback to minimum if bonding mentioned but no amount found
        if "bond" in profile.get("evidence_text", "").lower():
            return int(min_bond)
            
        return 0  # Default for no bonding info