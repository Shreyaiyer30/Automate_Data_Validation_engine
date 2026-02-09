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
                     audit_entries: List[Dict[str, Any]], config: Dict[str, Any], initial_stats: Dict[str, Any] = None) -> Dict[str, Any]:
        """Compile a deep comparative report."""
        stats = self._calculate_comparative_stats(initial_df, final_df)
        score = self._calculate_quality_score(final_df, audit_entries, initial_stats)
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
        """Deep comparative stats between raw and cleaned data."""
        return {
            "initial": {
                "rows": len(initial_df),
                "cols": len(initial_df.columns),
                "missing_pct": round(initial_df.isna().mean().mean() * 100, 2),
                "duplicates": int(initial_df.duplicated().sum()),
                "duplicates_pct": round(initial_df.duplicated().mean() * 100, 2) if len(initial_df) > 0 else 0,
                "column_stats": self._get_full_stats(initial_df)
            },
            "final": {
                "rows": len(final_df),
                "cols": len(final_df.columns),
                "missing_pct": round(final_df.isna().mean().mean() * 100, 2),
                "duplicates": int(final_df.duplicated().sum()),
                "duplicates_pct": round(final_df.duplicated().mean() * 100, 2) if len(final_df) > 0 else 0,
                "column_stats": self._get_full_stats(final_df)
            }
        }

    def _get_full_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get Min/Max/Mean and Outlier counts for all columns."""
        res = {}
        for col in df.columns:
            series = df[col]
            stats = {
                "dtype": str(series.dtype),
                "null_count": int(series.isna().sum())
            }
            if pd.api.types.is_numeric_dtype(series):
                non_null = series.dropna()
                if not non_null.empty:
                    stats.update({
                        "min": float(non_null.min()),
                        "max": float(non_null.max()),
                        "mean": float(non_null.mean())
                    })
                    # Add outlier count (IQR for report baseline)
                    q1 = non_null.quantile(0.25)
                    q3 = non_null.quantile(0.75)
                    iqr = q3 - q1
                    outliers = non_null[(non_null < (q1 - 1.5 * iqr)) | (non_null > (q3 + 1.5 * iqr))]
                    stats["outlier_count"] = int(len(outliers))
            res[col] = stats
        return res

    def _calculate_quality_score(self, df: pd.DataFrame, audit_entries: List[Dict[str, Any]], initial_stats: Dict[str, Any] = None) -> float:
        if len(df) == 0: return 0.0
        
        # Start with a base score
        score = 100.0
        
        # 1. Pipeline Failures (Higher penalty for critical errors)
        error_count = len([e for e in audit_entries if e.get('event', '').upper() == 'ERROR' or e.get('critical', False)])
        score -= (error_count * 15.0)
        
        # 2. Data Integrity (Missingness & Duplicates)
        # Weighted Missingness
        total_missing_penalty = 0.0
        importance_map = {"HIGH": 2.0, "MEDIUM": 1.0, "LOW": 0.5}
        
        for col in df.columns:
            missing_pct = df[col].isna().mean() * 100
            # Get importance from initial_stats or default to MEDIUM
            importance = importance_map.get(initial_stats.get(col, {}).get("importance_level", "MEDIUM").upper(), 1.0)
            total_missing_penalty += (missing_pct * 0.8 * importance)
            
        score -= (total_missing_penalty / len(df.columns))
        
        dup_pct = df.duplicated().mean() * 100
        score -= (dup_pct * 2.0)
        
        # 3. Mutation Overhead
        mutation_count = len([e for e in audit_entries if e.get('event', '').upper() == 'MUTATION'])
        score -= min(10.0, mutation_count * 0.2)
        
        # 4. Semantic Drift
        if initial_stats:
            drift_penalty = self._check_semantic_drift(df, initial_stats)
            score -= drift_penalty
            
        return max(0.0, min(100.0, round(score, 1)))

    def _check_semantic_drift(self, df: pd.DataFrame, initial_stats: Dict[str, Any]) -> float:
        """
        Check for semantic drift between raw and cleaned data.
        Penalty is weighted by column importance and normalized by variance.
        """
        total_drift_penalty = 0.0
        for col, stats in initial_stats.items():
            if col not in df.columns: continue
            
            importance = stats.get("importance_score", 0.5)
            
            if pd.api.types.is_numeric_dtype(df[col]) and "mean" in stats:
                initial_mean = stats["mean"]
                initial_std = stats.get("std", 1.0)
                current_mean = df[col].mean()
                
                # Dynamic Threshold: Tolerance is relative to the standard deviation
                tolerance = (initial_std * 0.15) / (importance + 0.1)
                
                if abs(current_mean - initial_mean) > tolerance:
                    deviation_factor = abs(current_mean - initial_mean) / (initial_std + 1e-9)
                    total_drift_penalty += (deviation_factor * 5.0 * importance)
                    
        return total_drift_penalty

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
        stats = report.get('statistics', {})
        s = report.get('summary', {})
        
        initial = stats.get('initial', {})
        final = stats.get('final', {})
        
        return f"""
# 📋 Quality Report Summary
**Score:** `{report.get('quality_score', 0)}%`
**Retention:** `{s.get('retention_rate', 0)}%`

### 📈 Metrics Comparison
| Metric | Raw | Cleaned |
| :--- | :--- | :--- |
| Missing % | {initial.get('missing_pct', 0)}% | {final.get('missing_pct', 0)}% |
| Duplicates % | {initial.get('duplicates_pct', 0)}% | {final.get('duplicates_pct', 0)}% |
| Row Count | {initial.get('rows', 0):,} | {final.get('rows', 0):,} |

### 🛠️ Actions Taken
- Total Data Mutations: {s.get('total_actions', 0)}
- Rows Removed: {s.get('rows_removed', 0)}
"""