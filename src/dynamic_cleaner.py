import pandas as pd
import numpy as np
from typing import Dict, Any, List
import re

class DynamicDataCleaner:
    """
    Applies cleaning rules to a DataFrame.
    Supports missing value imputation, string normalization, and outlier handling.
    """
    
    def __init__(self, rule_generator=None, logger=None):
        self.rule_generator = rule_generator
        self.logger = logger
        
    def apply_rules(self, df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
        """Apply a set of rules to the DataFrame."""
        df_clean = df.copy()
        
        # 0. Global Rules (e.g., Duplicates)
        if rules.get("_global", {}).get("remove_duplicates", False):
            rows_before = len(df_clean)
            df_clean = df_clean.drop_duplicates()
            if self.logger:
                self.logger.log_mutation("DYNAMIC_CLEANER", "drop_duplicates", {"count": rows_before - len(df_clean)})
        
        for col, col_rules in rules.items():
            if col.startswith("_"): continue # Skip internal/global rules
            if col not in df_clean.columns:
                continue
                
            # 1. Missing Values
            handle_missing = col_rules.get("handle_missing", "do_nothing")
            if handle_missing != "do_nothing":
                count_before = int(df_clean[col].isna().sum())
                df_clean[col] = self._impute_missing(df_clean[col], handle_missing)
                if self.logger and count_before > 0:
                    self.logger.log_mutation("DYNAMIC_CLEANER", "impute_missing", {"column": col, "strategy": handle_missing, "count": count_before})
                
            # 2. String Cleaning
            if df_clean[col].dtype == 'object':
                if col_rules.get("strip_whitespace", False):
                    df_clean[col] = df_clean[col].astype(str).str.strip()
                
                if col_rules.get("remove_special_chars", False):
                    df_clean[col] = df_clean[col].astype(str).apply(lambda x: re.sub(r'[^a-zA-Z0-9\s\.\-]', '', x))
                    if self.logger:
                        self.logger.log_mutation("DYNAMIC_CLEANER", "string_cleanup", {"column": col})
                
                if col_rules.get("convert_to_datetime", False):
                    try:
                        df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                    except:
                        pass
            
            # 3. Numeric Cleaning (Outliers)
            if col_rules.get("handle_outliers") == "clip_at_bounds" and pd.api.types.is_numeric_dtype(df_clean[col]):
                df_clean[col] = self._clip_outliers(df_clean[col])
                if self.logger:
                    self.logger.log_mutation("DYNAMIC_CLEANER", "clip_outliers", {"column": col})
                
        return df_clean

    def _impute_missing(self, series: pd.Series, strategy: str) -> pd.Series:
        if strategy == "impute_with_median":
            return series.fillna(series.median())
        elif strategy == "impute_with_mean":
            return series.fillna(series.mean())
        elif strategy == "fill_with_mode":
            mode = series.mode()
            return series.fillna(mode[0] if not mode.empty else "Unknown")
        elif strategy == "fill_with_unknown":
            return series.fillna("Unknown")
        elif strategy == "forward_fill":
            return series.ffill()
        elif strategy == "backward_fill":
            return series.bfill()
        return series

    def _clip_outliers(self, series: pd.Series) -> pd.Series:
        non_null = series.dropna()
        if non_null.empty: return series
        
        q1 = non_null.quantile(0.25)
        q3 = non_null.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        return series.clip(lower=lower_bound, upper=upper_bound)
