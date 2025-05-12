# scorer.py
from typing import List, Dict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SubcontractorScorer:
    def __init__(self, min_bond: int, target_city: str, target_state: str):
        self.min_bond = min_bond
        self.target_city = target_city
        self.target_state = target_state
        self.scoring_weights = {
            'experience': 0.25,
            'license': 0.25,
            'bonding': 0.25,
            'geography': 0.25
        }

    def calculate_scores(self, candidates: List[Dict]) -> List[Dict]:
        """Calculate and add scores to each candidate profile"""
        scored_candidates = []
        for candidate in candidates:
            try:
                score_breakdown = {
                    'experience': self._experience_score(candidate),
                    'license': self._license_score(candidate),
                    'bonding': self._bonding_score(candidate),
                    'geography': self._geographic_score(candidate)
                }
                
                total_score = sum(
                    weight * score_breakdown[category]
                    for category, weight in self.scoring_weights.items()
                )
                
                candidate['score'] = round(min(100, total_score), 2)
                candidate['score_breakdown'] = score_breakdown
                
                scored_candidates.append(candidate)
            except Exception as e:
                logger.error(f"Scoring failed for {candidate.get('name')}: {str(e)}")
                continue
                
        return self._rank_candidates(scored_candidates)

    def _experience_score(self, candidate: Dict) -> float:
        """Calculate experience fit score (0-25)"""
        projects = candidate.get('tx_projects_past_5yrs', 0)
        return min(25, projects * 5)  # 5 points per project

    def _license_score(self, candidate: Dict) -> float:
        """Calculate license status score (0-25)"""
        return 25 if candidate.get('lic_active') else 0

    def _bonding_score(self, candidate: Dict) -> float:
        """Calculate bonding capacity score (0-25)"""
        bond = candidate.get('bond_amount', 0)
        if bond >= self.min_bond:
            return 25
        elif bond >= self.min_bond * 0.5:
            return (bond / self.min_bond) * 25
        return 0

    def _geographic_score(self, candidate: Dict) -> float:
        """Calculate geographic match score (0-25)"""
        city_match = candidate.get('city', '').lower() == self.target_city.lower()
        state_match = candidate.get('state', '').lower() == self.target_state.lower()
        
        if city_match and state_match:
            return 25
        if state_match:
            return 12.5
        return 0

    def _rank_candidates(self, candidates: List[Dict]) -> List[Dict]:
        """Rank candidates by score with tie-breakers"""
        return sorted(
            candidates,
            key=lambda x: (
                -x['score'],
                -x['score_breakdown']['geography'],
                -x['score_breakdown']['experience'],
                -x['bond_amount']
            )
        )

class ResultFormatter:
    @staticmethod
    def format_results(candidates: List[Dict]) -> Dict:
        """Format final results to specification"""
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
                "evidence_text": c.get('evidence_text')[:500],  # Truncate if needed
                "last_checked": c.get('last_checked') or datetime.utcnow().isoformat()
            } for c in candidates]
        }