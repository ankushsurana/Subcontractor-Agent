from typing import List, Dict, Any, Optional
from datetime import datetime
import logging
from enum import Enum
import asyncio
from functools import lru_cache
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ScoreWeights(Enum):
    """Weights used for scoring subcontractors"""
    EXPERIENCE = 0.30
    LICENSE = 0.25
    BONDING = 0.20
    GEOGRAPHY = 0.15
    REPUTATION = 0.10 

@dataclass
class ScoringConfig:
    """Configuration for scoring parameters"""
    min_bond: int
    target_city: str
    target_state: str
    use_llm_validation: bool = False
    max_workers: int = 10
    cache_ttl: int = 3600 

@dataclass
class ScoreBreakdown:
    """Component scores for a candidate"""
    experience: float
    license: float
    bonding: float
    geography: float
    reputation: float

@dataclass
class ScoredCandidate:
    """Data class for candidate with scores"""
    name: str
    website: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    lic_active: bool = False
    lic_number: Optional[str] = None
    bond_amount: int = 0
    tx_projects_past_5yrs: int = 0
    distance_miles: Optional[int] = None
    score: float = 0.0
    score_breakdown: Optional[ScoreBreakdown] = None
    score_version: str = "enhanced"
    evidence_url: Optional[str] = None
    evidence_text: Optional[str] = None
    last_checked: Optional[str] = None
    project_quality_score: float = 0.5
    days_until_expiry: int = 365
    positive_reviews: int = 0
    years_in_business: int = 0
    awards: int = 0
    union_member: bool = False

class SubcontractorScorer:
    """Efficient and scalable scorer for subcontractor candidates"""
    
    def __init__(self, config: ScoringConfig):
        """Initialize with scoring configuration"""
        self.config = config
        self.target_city = config.target_city.lower()
        self.target_state = config.target_state.lower()
        self.min_bond = config.min_bond
        self.use_llm_validation = config.use_llm_validation
        self.executor = ThreadPoolExecutor(max_workers=config.max_workers)
        
        self.scoring_weights = {
            'experience': ScoreWeights.EXPERIENCE.value,
            'license': ScoreWeights.LICENSE.value,
            'bonding': ScoreWeights.BONDING.value,
            'geography': ScoreWeights.GEOGRAPHY.value,
            'reputation': ScoreWeights.REPUTATION.value
        }

    async def calculate_scores_async(self, candidates: List[Dict]) -> List[Dict]:
        """Asynchronously calculate scores for all candidates"""
        if not candidates:
            return []

        loop = asyncio.get_event_loop()
        tasks = []
        
        for candidate in candidates:
            task = loop.run_in_executor(
                self.executor,
                self._process_candidate,
                candidate
            )
            tasks.append(task)
        
        scored_candidates = await asyncio.gather(*tasks)
        
        valid_candidates = [c for c in scored_candidates if c is not None]
        
        return self._rank_candidates(valid_candidates)
    
    def calculate_scores(self, candidates: List[Dict]) -> List[Dict]:
        """Synchronous API for calculating scores (wrapper for async version)"""
        return asyncio.run(self.calculate_scores_async(candidates))

    def _process_candidate(self, candidate: Dict) -> Optional[Dict]:
        """Process a single candidate (runs in thread pool)"""
        try:
            candidate_obj = self._dict_to_candidate(candidate)
            
            score_breakdown = self._calculate_score_breakdown(candidate_obj)
            total_score = self._compute_total_score(score_breakdown)
            
            result = asdict(candidate_obj)
            result.update({
                'score': round(total_score, 2),
                'score_breakdown': asdict(score_breakdown),
                'score_version': 'enhanced',
                'last_checked': datetime.now().isoformat()
            })
            
            return result
        except Exception as e:
            logger.error(f"Scoring failed for {candidate.get('name', 'Unknown')}: {str(e)}")
            return None

    def _dict_to_candidate(self, data: Dict) -> ScoredCandidate:
        """Convert dictionary to ScoredCandidate dataclass"""
        valid_fields = {k: v for k, v in data.items() 
                        if k in ScoredCandidate.__annotations__}
        
        if 'name' not in valid_fields:
            valid_fields['name'] = "Unknown"
            
        return ScoredCandidate(**valid_fields)

    @lru_cache(maxsize=1024)
    def _calculate_score_breakdown(self, candidate: ScoredCandidate) -> ScoreBreakdown:
        """Calculate individual component scores with caching for performance"""
        return ScoreBreakdown(
            experience=self._experience_score(candidate),
            license=self._license_score(candidate),
            bonding=self._bonding_score(candidate),
            geography=self._geographic_score(candidate),
            reputation=self._reputation_score(candidate)
        )

    def _compute_total_score(self, score_breakdown: ScoreBreakdown) -> float:
        """Compute weighted total score (0-100)"""
        breakdown_dict = asdict(score_breakdown)
        return sum(
            weight * breakdown_dict[category] * 100 
            for category, weight in self.scoring_weights.items()
        )

    def _experience_score(self, candidate: ScoredCandidate) -> float:
        """Enhanced experience scoring with project quality consideration"""
        base_projects = candidate.tx_projects_past_5yrs or 0
        project_quality = candidate.project_quality_score or 0.5  
        
        raw_score = min(1.0, base_projects * 0.2) 
        quality_adjusted = raw_score * (0.7 + 0.3 * project_quality)
        
        return quality_adjusted

    def _license_score(self, candidate: ScoredCandidate) -> float:
        """License scoring with expiration proximity penalty"""
        if not candidate.lic_active:
            return 0.0
            
        days_until_expiry = candidate.days_until_expiry or 365
        expiry_factor = min(1.0, days_until_expiry / 365) 
        
        return 0.8 * expiry_factor + 0.2  

    def _bonding_score(self, candidate: ScoredCandidate) -> float:
        """Non-linear bonding score with minimum threshold"""
        bond_amount = candidate.bond_amount or 0
        if bond_amount < self.min_bond * 0.5:
            return 0.0
            
        normalized = (bond_amount - self.min_bond*0.5) / (self.min_bond*1.5)
        return min(1.0, max(0.0, normalized))

    def _geographic_score(self, candidate: ScoredCandidate) -> float:
        """Geographic scoring with distance decay"""
        city = (candidate.city or "").lower()
        state = (candidate.state or "").lower()
        
        city_match = city == self.target_city
        state_match = state == self.target_state
        
        if city_match:
            return 1.0
        elif state_match:
            distance_miles = candidate.distance_miles or 100
            return max(0.3, 1.0 - (distance_miles / 300))
        return 0.0

    def _reputation_score(self, candidate: ScoredCandidate) -> float:
        """Reputation scoring based on multiple factors"""
        score = 0.0
        if (candidate.positive_reviews or 0) > 5:
            score += 0.3
        if (candidate.years_in_business or 0) > 5:
            score += 0.2
        if (candidate.awards or 0) > 0:
            score += 0.2
        if candidate.union_member:
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
    """Format scoring results in various output formats"""
    
    @staticmethod
    async def format_results_async(candidates: List[Dict], format_type: str = "json") -> Any:
        """Asynchronously format results in specified format"""
        if not candidates:
            return {"status": "SUCCEEDED", "results": []}
        
        if format_type.lower() == "json":
            return ResultFormatter._format_json(candidates)
        else:
            raise ValueError(f"Unsupported format type: {format_type}")
    
    @staticmethod
    def format_results(candidates: List[Dict], format_type: str = "json") -> Any:
        """Synchronous API for formatting results"""
        return asyncio.run(ResultFormatter.format_results_async(candidates, format_type))
    
    @staticmethod
    def _format_json(candidates: List[Dict]) -> Dict:
        """Format results as JSON"""
        results = []
        
        for c in candidates:
            result = {
                "name": c.get('name'),
                "website": c.get('website'),
                "city": c.get('city'),
                "state": c.get('state'),
                "lic_active": c.get('lic_active'),
                "lic_number": c.get('lic_number'),
                "bond_amount": c.get('bond_amount'),
                "tx_projects_past_5yrs": c.get('tx_projects_past_5yrs'),
                "score": c.get('score'),
                "score_breakdown": c.get('score_breakdown'),
                "evidence_url": c.get('evidence_url'),
                "last_checked": c.get('last_checked') or datetime.now().isoformat()
            }
            
            if c.get('evidence_text'):
                result["evidence_text"] = c['evidence_text'][:500]
                
            results.append(result)
            
        return {
            "status": "SUCCEEDED",
            "count": len(results),
            "results": results
        }