# scoring.py
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class SubcontractorScorer:
    """Score and rank subcontractors"""
    
    def score_profiles(self, profiles: List[Dict], requirements: Dict) -> List[Dict]:
        """Calculate relevance scores"""
        scored = []
        for profile in profiles:
            try:
                score = self._calculate_score(profile, requirements)
                scored.append({**profile, "score": score})
            except Exception as e:
                logger.error(f"Scoring failed for {profile.get('name')}: {str(e)}")
                scored.append({**profile, "score": 0})
                
        return sorted(scored, key=lambda x: x["score"], reverse=True)

    def _calculate_score(self, profile: Dict, req: Dict) -> int:
        """Individual profile scoring"""
        score = 0
        
        # Location match (30 points)
        if profile.get("state") == req["state"]:
            score += 20
            if profile.get("city") == req["city"]:
                score += 10
                
        # License status (20 points)
        if profile.get("lic_active"):
            score += 20
            
        # Bonding capacity (30 points)
        if profile.get("bond_amount"):
            if profile["bond_amount"] >= req["min_bond"]:
                score += 30
            elif profile["bond_amount"] >= req["min_bond"] * 0.5:
                score += 15
                
        # Project history (20 points)
        if profile.get("projects"):
            score += min(20, len(profile["projects"]) * 5)
            
        return min(100, score)  # Cap at 100