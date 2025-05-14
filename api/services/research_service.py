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
        print(f"[Orchestrator] Input request: {request}")
        try:
            logger.info(f"[Orchestrator] Starting research for {request.get('trade')} in {request.get('city')}, {request.get('state')}")
            
            # Phase 1: Discovery (FR1)
            try:
                logger.info("[FR1] Starting discovery phase...")
                candidates = await self.discovery.find_subcontractors(
                    request.get("trade", ""),
                    request.get("city", ""), 
                    request.get("state", ""),
                    request.get("keywords", [])
                )
                logger.info(f"[FR1] Discovery phase found {len(candidates)} candidates")
                
                # Log the first 3 candidates for debugging
                for i, candidate in enumerate(candidates[:3]):
                    logger.info(f"[FR1] Candidate {i+1}: {candidate.get('url')} - {candidate.get('title')}")
                
                if not candidates:
                    logger.warning("[FR1] No candidates found during discovery phase")
                    return []
            except Exception as e:
                logger.error(traceback.format_exc())
                return []
            
            # Phase 2: Profile Extraction (FR2)
            try:
                logger.info("[FR2] Starting profile extraction phase...")
                urls = [c.get("url", "") for c in candidates if c.get("url")]
                            
                if not urls:
                    return []
                    
                raw_profiles = await self.extractor.extract_profiles(urls)
                
                # Log sample profile data
                if raw_profiles:
                    sample = raw_profiles[0]
                    logger.info(f"[FR2] Sample profile - Name: {sample.get('business_name')}, Website: {sample.get('website')}")
                
                if not raw_profiles:
                    logger.warning("[FR2] No profiles extracted")
                    return []
            except Exception as e:
                logger.error(f"[FR2] Error in profile extraction phase: {str(e)}")
                logger.error(traceback.format_exc())
                return []

            # Phase 3: License Verification (FR3)
            try:
                logger.info("[FR3] Starting license verification phase...")
                verified_profiles = await self.verifier.verify_batch(raw_profiles)
                
                for i, profile in enumerate(verified_profiles[:3]):
                    logger.info(f"[FR3] Profile {i+1} - Business: {profile.get('business_name')}, "
                              f"License: {profile.get('lic_number', 'Unknown')}, "
                              f"Active: {profile.get('lic_active', False)}")
                
                if not verified_profiles:
                    verified_profiles = raw_profiles
            except Exception as e:
                logger.error(f"[FR3] Error in license verification phase: {str(e)}")
                logger.error(traceback.format_exc())
                logger.info("[FR3] Continuing with unverified profiles")
                verified_profiles = raw_profiles

            # Phase 4: Project History Parsing (FR4)
            try:
                logger.info("[FR4] Starting project history parsing phase...")
                enriched_profiles = await self.project_history.enrich_profiles(verified_profiles)
                
                for i, profile in enumerate(enriched_profiles[:3]):
                    tx_projects = profile.get('tx_projects_past_5yrs', 0) or 0
                    logger.info(f"[FR4] Profile {i+1} - Business: {profile.get('business_name')}, "
                              f"TX Projects: {tx_projects}")
                
                if not enriched_profiles:
                    logger.warning("[FR4] No profiles after enrichment, using verified profiles")
                    enriched_profiles = verified_profiles
            except Exception as e:
                logger.error(f"[FR4] Error in project history phase: {str(e)}")
                logger.error(traceback.format_exc())
                logger.info("[FR4] Continuing with verified profiles")
                enriched_profiles = verified_profiles

            try:
                logger.info("[FR5] Starting result formatting and scoring phase...")
                results = self._format_results(enriched_profiles, request)
                
                sorted_results = sorted(results, key=lambda x: x.score, reverse=True)
                for i, result in enumerate(sorted_results[:3]):
                    logger.info(f"[FR5] Result {i+1} - Name: {result.name}, Score: {result.score}, "
                              f"License: {result.lic_number}, Active: {result.lic_active}")
                
                if not results:
                    return []
            except Exception as e:
                logger.error(f"[FR5] Error in result formatting: {str(e)}")
                logger.error(traceback.format_exc())
                return []
            
            logger.info(f"[Orchestrator] Research complete: Found {len(results)} qualified subcontractors")
            return sorted(results, key=lambda x: x.score, reverse=True)
            
        except Exception as e:
            logger.error(f"Research orchestration failed: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def _format_results(self, profiles: List[Dict], request: dict) -> List[ResearchResult]:
        """
        Format raw profiles into the expected ResearchResult schema.
        Maps data from the extractor and enrichers to the final output format.
        """
        results = []
        if not profiles:
            return results
                
        for profile in profiles:
            try:
                logger.debug(f"Raw profile data: {profile}")
                
                if not profile.get("website"):
                    logger.debug(f"Skipping profile with no website: {profile}")
                    continue
                    
                
                name = str(profile.get("business_name") or profile.get("name", "Unknown"))
                website = str(profile.get("website") or profile.get("source_url", ""))

                location = profile.get("hq_address") or ""
                city = str(profile.get("city") or request.get("city", ""))
                state = str(profile.get("state") or request.get("state", ""))
                
                if location and (not city or not state):
                    parts = location.split(",")
                    if len(parts) >= 2:
                        if not city and len(parts) >= 2:
                            city = parts[-2].strip()
                        if not state and len(parts) >= 1:
                            state_zip = parts[-1].strip().split()
                            if state_zip:
                                state = state_zip[0].strip()
                
                lic_active = bool(profile.get("lic_active", False))
                lic_number = str(profile.get("lic_number") or profile.get("license") or 
                               profile.get("licensing_text") or "Unknown")
                
                bond_amount = self._parse_bond_amount(profile, request.get("min_bond", 0))
                
                tx_projects = profile.get("tx_projects_past_5yrs", 0)
                if not isinstance(tx_projects, int) or tx_projects is None:
                    tx_projects = 0
                
                score = self._calculate_score(profile, request)
                if score is None:
                    score = 0
                
                evidence_url = str(profile.get("website") or profile.get("source_url", ""))
                
                evidence_text = self._get_evidence_text(profile)
                
                result_data = {
                    "name": name or "Unknown",
                    "website": website or "",
                    "city": city or "",
                    "state": state or "",
                    "lic_active": bool(lic_active),
                    "lic_number": lic_number or "Unknown",
                    "bond_amount": int(bond_amount) if bond_amount is not None else 0,
                    "tx_projects_past_5yrs": int(tx_projects) if tx_projects is not None else 0,
                    "score": int(score) if score is not None else 0,
                    "evidence_url": evidence_url or "",
                    "evidence_text": evidence_text or "",
                    "last_checked": datetime.utcnow().isoformat()
                }
                                
                result = ResearchResult(**result_data)
                results.append(result)
            except Exception as e:
                logger.error(f"Error formatting result: {str(e)} with profile: {profile}")
                
        logger.info(f"Successfully formatted {len(results)} results")
        return results

    def _count_tx_projects(self, profile: Dict) -> int:
        """Count verified TX projects from last 5 years"""
        if "tx_projects_past_5yrs" in profile:
            return int(profile["tx_projects_past_5yrs"])
            
        projects = profile.get("projects", [])
        if isinstance(projects, list):
            return len(projects)
        return 0

    def _get_evidence_text(self, profile: Dict) -> str:
        """Extract evidence text with project context"""
        evidence = []
        
        if profile.get("raw_text"):
            evidence.append(str(profile["raw_text"])[:300])
            
        if "project_evidence" in profile:
            for item in profile["project_evidence"][:2]: 
                evidence.append(item["text"][:200])
                
        return " [...] ".join(evidence)[:500]

    def _calculate_score(self, profile: Dict, request: Dict) -> int:
        """
        Enhanced scoring algorithm for ranking subcontractors.
        
        Weights multiple factors:
        - Geographic match (30 points max)
        - License status (20 points)
        - Bonding capacity (30 points)
        - Project history (20 points)
        - Keyword relevance (bonus points)
        
        Returns an integer score from 0-100.
        """
        try:
            score = 0
            request_state = request.get("state", "")
            request_city = request.get("city", "")

            location_score = 0
            profile_state = profile.get("state", "")

            if profile_state and request_state and profile_state.upper() == request_state.upper():
                location_score += 20
                
                profile_city = profile.get("city", "")
                if profile_city and request_city and profile_city.lower() == request_city.lower():
                    location_score += 10
            
            score += location_score
            
            license_score = 0
            lic_active = profile.get("lic_active", False)
            if lic_active:
                license_score = 20
            elif profile.get("lic_number") and profile.get("lic_number") != "Unknown":
                license_score = 10
                
            score += license_score
                
            bond_score = 0
            bond_amount = profile.get("bond_amount", 0) or 0 
            min_bond = request.get("min_bond", 0) or 0  
            
            if isinstance(bond_amount, str) and bond_amount.isdigit():
                bond_amount = int(bond_amount)
            if isinstance(min_bond, str) and min_bond.isdigit():
                min_bond = int(min_bond)
                
            if bond_amount and min_bond:
                if bond_amount >= min_bond:
                    bond_score = 30
                elif bond_amount >= (min_bond * 0.5):
                    bond_score = 15
                elif bond_amount > 0:
                    bond_score = 5
                
            score += bond_score
                
            project_score = 0
            tx_projects = self._count_tx_projects(profile)
            
            if tx_projects >= 4:
                project_score = 20
            else:
                project_score = min(20, tx_projects * 5)  
                
            score += project_score

            keyword_score = 0
            profile_name = profile.get("business_name", "") or profile.get("name", "") or ""
            evidence_text = profile.get("evidence_text", "") or ""
            text = f"{profile_name} {evidence_text}".lower()
            
            for keyword in request.get("keywords", []):
                if keyword and keyword.lower() in text:
                    keyword_score += 2
                    
            keyword_score = min(10, keyword_score)
            score += keyword_score
                    
            final_score = min(100, score)
            logger.debug(f"[Scoring] Final score: {final_score} (Location: {location_score}, License: {license_score}, "
                      f"Bond: {bond_score}, Projects: {project_score}, Keywords: {keyword_score})")
                    
            return final_score
            
        except Exception as e:
            logger.error(f"[Scoring] Error in _calculate_score: {str(e)}", exc_info=True)
            return 0 

    def _parse_bond_amount(self, profile: Dict, min_bond: int) -> int:
        """Parse bonding capacity with evidence validation"""
        try:
  
            min_bond = int(min_bond) if min_bond is not None else 0
            
            if profile and "bond_amount" in profile:
                bond_amount = profile["bond_amount"]
                if isinstance(bond_amount, (int, float)) and bond_amount is not None:
                    return int(bond_amount)
            
            evidence_text = profile.get("evidence_text", "") or ""
            if evidence_text and "bond" in evidence_text.lower():
                return min_bond
            
            return 0  
        except Exception as e:
            return 0