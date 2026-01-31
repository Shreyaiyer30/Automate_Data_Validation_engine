import pandas as pd
from typing import Dict, List, Any
from datetime import datetime

class QualityReport:
    """
    Generates structured, insightful data quality reports.
    Compares Raw vs Cleaned data across multiple dimensions.
    """
    def __init__(self):
        self.timestamp = datetime.now()

    def build_report(self, initial_df: pd.DataFrame, final_df: pd.DataFrame, 
                     audit_entries: List[Dict[str, Any]], config: Dict[str, Any]) -> Dict[str, Any]:
        """Compile a deep comparative report."""
        stats = self._calculate_comparative_stats(initial_df, final_df)
        score = self._calculate_quality_score(final_df, audit_entries)
        issues = self._detect_remaining_issues(final_df)
        
        return {
            "timestamp": self.timestamp.isoformat(),
            "quality_score": score,
            "statistics": stats,
            "summary": self._build_summary(audit_entries, initial_df, final_df),
            "remaining_issues": issues,
            "recommendations": self._generate_recommendations(score, issues)
        }

    def _calculate_comparative_stats(self, initial_df: pd.DataFrame, final_df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "initial": {
                "rows": len(initial_df),
                "cols": len(initial_df.columns),
                "missing_pct": round(initial_df.isna().mean().mean() * 100, 2),
                "duplicates_pct": round(initial_df.duplicated().mean() * 100, 2)
            },
            "final": {
                "rows": len(final_df),
                "cols": len(final_df.columns),
                "missing_pct": round(final_df.isna().mean().mean() * 100, 2),
                "duplicates_pct": round(final_df.duplicated().mean() * 100, 2)
            }
        }

    def _calculate_quality_score(self, df: pd.DataFrame, audit_entries: List[Dict[str, Any]]) -> float:
        if len(df) == 0: return 0.0
        
        # Penalties based on remaining issues
        base_score = 100.0
        
        # Missing values penalty
        missing_penalty = df.isna().mean().mean() * 100
        
        # Duplicates penalty
        dup_penalty = df.duplicated().mean() * 100 * 2
        
        # Critical errors penalty
        error_count = len([e for e in audit_entries if e.get('event', '').upper() == 'ERROR'])
        error_penalty = error_count * 10
        
        score = max(0, base_score - missing_penalty - dup_penalty - error_penalty)
        return round(score, 1)

    def _detect_remaining_issues(self, df: pd.DataFrame) -> List[str]:
        issues = []
        if df.isna().any().any():
            issues.append("Dataset still contains missing values.")
        if df.duplicated().any():
            issues.append("Dataset still contains duplicate rows.")
        return issues

    def _build_summary(self, audit_entries: List[Dict[str, Any]], 
                       initial_df: pd.DataFrame, final_df: pd.DataFrame) -> Dict[str, Any]:
        mutations = [e for e in audit_entries if e['event'].upper() == 'MUTATION']
        return {
            "total_actions": len(mutations),
            "rows_removed": len(initial_df) - len(final_df),
            "retention_rate": round((len(final_df) / len(initial_df) * 100), 2) if len(initial_df) > 0 else 0
        }

    def _generate_recommendations(self, score: float, issues: List[str]) -> List[str]:
        recs = []
        if score < 80:
            recs.append("Configure more aggressive imputation rules.")
        if issues:
            recs.append("Review remaining issues in the 'Profiling' section.")
        if not recs:
            recs.append("Data is highly reliable for production usage.")
        return recs

    def export_to_markdown(self, report: Dict[str, Any]) -> str:
        """Format as clean Markdown."""
        stats = report['statistics']
        s = report['summary']
        
        return f"""
# 📋 Quality Report Summary
**Score:** `{report['quality_score']}%`
**Retention:** `{s['retention_rate']}%`

### 📈 Metrics Comparison
| Metric | Raw | Cleaned |
| :--- | :--- | :--- |
| Missing % | {stats['initial']['missing_pct']}% | {stats['final']['missing_pct']}% |
| Duplicates % | {stats['initial']['duplicates_pct']}% | {stats['final']['duplicates_pct']}% |
| Row Count | {stats['initial']['rows']:,} | {stats['final']['rows']:,} |

### 🛠️ Actions Taken
- Total Data Mutations: {s['total_actions']}
- Rows Removed: {s['rows_removed']}
"""