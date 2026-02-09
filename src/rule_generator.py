import pandas as pd
import numpy as np
from typing import Dict, List, Any
import re
from scipy import stats

class DynamicRuleGenerator:
    """
    Analyzes datasets to suggest optimal cleaning rules.
    Detects patterns like missing values, outliers (IQR & Z-score), special characters.
    """
    
    def __init__(self, settings: Dict = None):
        self.settings = settings or {}
        self.outlier_method = self.settings.get("outlier_method", "iqr")
        self.z_threshold = self.settings.get("z_threshold", 3.0)
        
    def analyze_column(self, series: pd.Series) -> Dict[str, Any]:
        """Deep analysis of a single column."""
        stats_dict = {
            "dtype": str(series.dtype),
            "missing_count": int(series.isna().sum()),
            "missing_percentage": float(series.isna().mean() * 100),
            "unique_count": int(series.nunique()),
            "is_constant": series.nunique() <= 1 if len(series) > 0 else False
        }
        
        # Numeric analysis
        if pd.api.types.is_numeric_dtype(series):
            non_null = series.dropna()
            if not non_null.empty:
                stats_dict["min"] = float(non_null.min())
                stats_dict["max"] = float(non_null.max())
                stats_dict["mean"] = float(non_null.mean())
                stats_dict["std"] = float(non_null.std())
                
                # IQR Outliers
                q1 = non_null.quantile(0.25)
                q3 = non_null.quantile(0.75)
                iqr = q3 - q1
                iqr_outliers = non_null[(non_null < (q1 - 1.5 * iqr)) | (non_null > (q3 + 1.5 * iqr))]
                stats_dict["iqr_outlier_count"] = int(len(iqr_outliers))
                
                # Z-Score Outliers
                if len(non_null) > 1 and stats_dict["std"] > 0:
                    z_scores = np.abs((non_null - stats_dict["mean"]) / stats_dict["std"])
                    z_outliers = non_null[z_scores > self.z_threshold]
                    stats_dict["zscore_outlier_count"] = int(len(z_outliers))
                else:
                    stats_dict["zscore_outlier_count"] = 0
                
                # Default outlier count based on configured method
                method = self.settings.get("outlier_method", "iqr")
                stats_dict["outlier_count"] = stats_dict["iqr_outlier_count"] if method == "iqr" else stats_dict["zscore_outlier_count"]
                stats_dict["has_outliers"] = stats_dict["outlier_count"] > 0
        
        # String analysis
        elif pd.api.types.is_object_dtype(series):
            non_null = series.dropna().astype(str)
            if not non_null.empty:
                # Check for special characters
                special_char_pattern = re.compile(r'[^a-zA-Z0-9\s]')
                has_special = non_null.apply(lambda x: bool(special_char_pattern.search(x)))
                stats_dict["has_special_chars"] = bool(has_special.any())
                stats_dict["special_char_count"] = int(has_special.sum())
                
                # Check if it looks like a date
                try:
                    pd.to_datetime(non_null.head(10), errors='raise')
                    stats_dict["is_likely_date"] = True
                except:
                    stats_dict["is_likely_date"] = False
                    
        # Calculate Inferred Column Importance (0.0 - 1.0)
        # Higher cardinality/diversity and low nulls increase importance
        importance = 0.5 # Baseline
        
        # Boost for high cardinality (indicates ID or primary feature)
        if len(series) > 0:
            diversity = stats_dict["unique_count"] / len(series)
            importance += diversity * 0.3
            
            # Penalty for high missingness
            importance -= (stats_dict["missing_percentage"] / 100) * 0.4
            
            # Boost for numeric diversity
            if "std" in stats_dict and stats_dict["std"] > 0:
                importance += 0.1
        
        stats_dict["importance_score"] = max(0.0, min(1.0, importance))
        
        return stats_dict

    def generate_cleaning_rules(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate suggested rules for the entire dataframe."""
        rules = {"_global": {}}
        analysis = {}
        
        # Global analysis
        has_dups = df.duplicated().any()
        rules["_global"]["remove_duplicates"] = bool(has_dups)
        rules["_global"]["outlier_method"] = self.settings.get("outlier_method", "iqr")
        
        for col in df.columns:
            col_stats = self.analyze_column(df[col])
            analysis[col] = col_stats
            
            # Suggest rules based on stats
            col_rules = {}
            
            # 1. Missing Value Handling
            if col_stats["missing_percentage"] > 0:
                if pd.api.types.is_numeric_dtype(df[col]):
                    col_rules["handle_missing"] = "impute_with_median"
                else:
                    col_rules["handle_missing"] = "fill_with_mode"
            else:
                col_rules["handle_missing"] = "do_nothing"
                
            # 2. String Cleaning
            if pd.api.types.is_object_dtype(df[col]):
                col_rules["strip_whitespace"] = True
                if col_stats.get("has_special_chars"):
                    col_rules["remove_special_chars"] = True
                if col_stats.get("is_likely_date"):
                    col_rules["convert_to_datetime"] = True
                    
            # 3. Numeric Cleaning
            if pd.api.types.is_numeric_dtype(df[col]):
                if col_stats.get("has_outliers"):
                    col_rules["handle_outliers"] = "clip_at_bounds"
                else:
                    col_rules["handle_outliers"] = "do_nothing"
                    
            rules[col] = col_rules
            
        return {
            "suggested_rules": rules,
            "column_analysis": analysis,
            "quality_score": self._calculate_overall_score(analysis)
        }

    def _calculate_overall_score(self, analysis: Dict[str, Dict]) -> float:
        """Calculate a baseline quality score (0-100)."""
        if not analysis: return 100.0
        
        total_penalty = 0.0
        for col, stats in analysis.items():
            # Missing values penalty
            total_penalty += stats["missing_percentage"] * 0.6
            
            # Constant columns penalty (redundant data)
            if stats["is_constant"]:
                total_penalty += 3.0
                
            # Outliers penalty
            if stats.get("has_outliers"):
                total_penalty += min(5.0, stats["outlier_count"] * 0.1)
                
        return max(0.0, min(100.0, 100.0 - total_penalty))
