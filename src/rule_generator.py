import pandas as pd
import numpy as np
from typing import Dict, List, Any
import re

class DynamicRuleGenerator:
    """
    Analyzes datasets to suggest optimal cleaning rules.
    Detects patterns like missing values, outliers, special characters, and data type inconsistencies.
    """
    
    def __init__(self, settings: Dict = None):
        self.settings = settings or {}
        
    def analyze_column(self, series: pd.Series) -> Dict[str, Any]:
        """Deep analysis of a single column."""
        stats = {
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
                stats["min"] = float(non_null.min())
                stats["max"] = float(non_null.max())
                stats["mean"] = float(non_null.mean())
                
                # Outlier detection (IQR)
                q1 = non_null.quantile(0.25)
                q3 = non_null.quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                outliers = non_null[(non_null < lower_bound) | (non_null > upper_bound)]
                stats["outlier_count"] = int(len(outliers))
                stats["has_outliers"] = len(outliers) > 0
        
        # String analysis
        elif pd.api.types.is_object_dtype(series):
            non_null = series.dropna().astype(str)
            if not non_null.empty:
                # Check for special characters
                special_char_pattern = re.compile(r'[^a-zA-Z0-9\s]')
                has_special = non_null.apply(lambda x: bool(special_char_pattern.search(x)))
                stats["has_special_chars"] = bool(has_special.any())
                stats["special_char_count"] = int(has_special.sum())
                
                # Check if it looks like a date
                try:
                    pd.to_datetime(non_null.head(10), errors='raise')
                    stats["is_likely_date"] = True
                except:
                    stats["is_likely_date"] = False
                    
        return stats

    def generate_cleaning_rules(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate suggested rules for the entire dataframe."""
        rules = {"_global": {}}
        analysis = {}
        
        # Global analysis
        has_dups = df.duplicated().any()
        rules["_global"]["remove_duplicates"] = bool(has_dups)
        
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
            total_penalty += stats["missing_percentage"] * 0.5
            
            # Constant columns penalty
            if stats["is_constant"]:
                total_penalty += 5.0
                
            # Outliers penalty
            if stats.get("has_outliers"):
                total_penalty += 2.0
                
        return max(0.0, min(100.0, 100.0 - total_penalty))
