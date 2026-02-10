from typing import Dict, Any, List
import pandas as pd
from src.semantic_pipeline.types import ValidationIssue

class QualityScorer:
    """
    Calculates dataset quality score (0-100) based on validation issues.
    """
    
    WEIGHTS = {
        "ERROR": 5.0,
        "WARNING": 2.0,
        "INFO": 0.5
    }
    
    def calculate_score(self, df: pd.DataFrame, issues: List[ValidationIssue]) -> Dict[str, Any]:
        base_score = 100.0
        total_rows = len(df) if len(df) > 0 else 1
        
        penalty = 0.0
        breakdown = []
        
        for issue in issues:
            impact_factor = self.WEIGHTS.get(issue.severity, 1.0)
            # Penalty proportional to affected rows
            issue_penalty = (issue.rows_affected / total_rows) * impact_factor * 10
            
            # Cap per-issue penalty
            issue_penalty = min(issue_penalty, 15.0) 
            
            penalty += issue_penalty
            breakdown.append({
                "rule": issue.rule_id,
                "severity": issue.severity,
                "rows": issue.rows_affected,
                "penalty": round(issue_penalty, 2)
            })
            
        final_score = max(0.0, round(base_score - penalty, 2))
        
        return {
            "score": final_score,
            "penalty_total": round(penalty, 2),
            "breakdown": breakdown
        }
