from typing import List, Dict, Optional
from datetime import datetime
import logging
import json
import pandas as pd
from enum import Enum

logger = logging.getLogger(__name__)

class ScoreWeights(Enum):
    EXPERIENCE = 0.30
    LICENSE = 0.25
    BONDING = 0.20
    GEOGRAPHY = 0.15
    REPUTATION = 0.10 

class SubcontractorScorer:
    def __init__(self, 
                 min_bond: int, 
                 target_city: str, 
                 target_state: str,
                 use_llm_validation: bool = False):
        self.min_bond = min_bond
        self.target_city = target_city.lower()
        self.target_state = target_state.lower()
        self.use_llm_validation = use_llm_validation
        self.scoring_weights = {
            'experience': ScoreWeights.EXPERIENCE.value,
            'license': ScoreWeights.LICENSE.value,
            'bonding': ScoreWeights.BONDING.value,
            'geography': ScoreWeights.GEOGRAPHY.value,
            'reputation': ScoreWeights.REPUTATION.value
        }

    def calculate_scores(self, candidates: List[Dict]) -> List[Dict]:
        """Enhanced scoring with dynamic weights"""
        if not candidates:
            return []

        scored_candidates = []
        for candidate in candidates:
            try:
                score_breakdown = self._calculate_score_breakdown(candidate)
                total_score = self._compute_total_score(score_breakdown)
                
                scored_candidate = {
                    **candidate,
                    'score': round(total_score, 2),
                    'score_breakdown': score_breakdown,
                    'score_version': 'initial'
                }
                scored_candidates.append(scored_candidate)
            except Exception as e:
                logger.error(f"Initial scoring failed for {candidate.get('name')}: {str(e)}")
                continue

        return self._rank_candidates(scored_candidates)

    def _calculate_score_breakdown(self, candidate: Dict) -> Dict[str, float]:
        """Calculate individual component scores with enhanced logic"""
        return {
            'experience': self._experience_score(candidate),
            'license': self._license_score(candidate),
            'bonding': self._bonding_score(candidate),
            'geography': self._geographic_score(candidate),
            'reputation': self._reputation_score(candidate)
        }

    def _compute_total_score(self, score_breakdown: Dict) -> float:
        """Compute weighted total score (0-100)"""
        return sum(
            weight * score_breakdown[category] * 100 
            for category, weight in self.scoring_weights.items()
        )

    def _experience_score(self, candidate: Dict) -> float:
        """Enhanced experience scoring with project quality consideration"""
        base_projects = candidate.get('tx_projects_past_5yrs', 0)
        project_quality = candidate.get('project_quality_score', 0.5)  
        
        raw_score = min(1.0, base_projects * 0.2) 
        quality_adjusted = raw_score * (0.7 + 0.3 * project_quality)
        
        return quality_adjusted

    def _license_score(self, candidate: Dict) -> float:
        """License scoring with expiration proximity penalty"""
        if not candidate.get('lic_active'):
            return 0.0
            
        days_until_expiry = candidate.get('days_until_expiry', 365)
        expiry_factor = min(1.0, days_until_expiry / 365) 
        
        return 0.8 * expiry_factor + 0.2  

    def _bonding_score(self, candidate: Dict) -> float:
        """Non-linear bonding score with minimum threshold"""
        bond_amount = candidate.get('bond_amount', 0)
        if bond_amount < self.min_bond * 0.5:
            return 0.0
            
        normalized = (bond_amount - self.min_bond*0.5) / (self.min_bond*1.5)
        return min(1.0, max(0.0, normalized))

    def _geographic_score(self, candidate: Dict) -> float:
        """Geographic scoring with distance decay"""
        city_match = candidate.get('city', '').lower() == self.target_city
        state_match = candidate.get('state', '').lower() == self.target_state
        
        if city_match:
            return 1.0
        elif state_match:
            distance_miles = candidate.get('distance_miles', 100)
            return max(0.3, 1.0 - (distance_miles / 300))
        return 0.0

    def _reputation_score(self, candidate: Dict) -> float:
        """Reputation scoring based on multiple factors"""
        score = 0.0
        if candidate.get('positive_reviews', 0) > 5:
            score += 0.3
        if candidate.get('years_in_business', 0) > 5:
            score += 0.2
        if candidate.get('awards', 0) > 0:
            score += 0.2
        if candidate.get('union_member', False):
            score += 0.3
        return min(1.0, score)


    def _rank_candidates(self, candidates: List[Dict]) -> List[Dict]:
        """Enhanced ranking with multiple tie-breakers"""
        return sorted(
            candidates,
            key=lambda x: (
                -x['score'],  
                -x['score_breakdown']['experience'],  
                -x['score_breakdown']['license'],  
                -x.get('bond_amount', 0),  
                -x.get('years_in_business', 0) 
            )
        )

class ResultFormatter:
    @staticmethod
    def format_results(candidates: List[Dict], format: str = "json") -> Dict:
        """Enhanced result formatter with multiple output options"""
        if not candidates:
            return {"status": "SUCCEEDED", "results": []}

        return {
            "status": "SUCCEEDED",
            "results": [{
                "name": c.get('name'),
                "website": c.get('website'),
                "city": c.get('city'),
                "state": c.get('state'),
                "lic_active": c.get('lic_active'),
                "lic_number": c.get('lic_number'),
                "bond_amount": c.get('bond_amount'),
                "tx_projects_past_5yrs": c.get('tx_projects_past_5yrs'),
                "score": c.get('score'),
                "evidence_url": c.get('evidence_url'),
                "evidence_text": c.get('evidence_text')[:500], 
                "last_checked": c.get('last_checked') or datetime.isoformat()
            } for c in candidates]
        }
