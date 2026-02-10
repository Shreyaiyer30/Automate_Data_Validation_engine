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
        """
        Apply a set of rules to the DataFrame following defined precedence:
        Domain Constraints > Data Type Enforcement > Statistical Heuristics > Cosmetic Standardization
        """
        df_clean = df.copy()
        
        # 0. LEVEL: Global & Domain Constraints (High Precedence)
        global_rules = rules.get("_global", {})
        if global_rules.get("remove_duplicates", False):
            rows_before = len(df_clean)
            df_clean = df_clean.drop_duplicates()
            if self.logger:
                self.logger.log_mutation("DYNAMIC_CLEANER", "drop_duplicates", {"count": rows_before - len(df_clean)})
        
        outlier_method = global_rules.get("outlier_method", "iqr")
        
        # Order columns for consistent processing
        columns = [c for c in rules.keys() if not c.startswith("_") and c in df_clean.columns]
        
        # 0.5 LEVEL: Semantic Validation & Transformation
        for col in columns:
            col_rules = rules[col]
            semantic_type = col_rules.get("semantic_type", "UNKNOWN")
            handle_semantic = col_rules.get("handle_semantic")
            
            if not handle_semantic:
                continue

            if semantic_type == "AGE":
                df_clean[col] = self._handle_semantic_age(df_clean, col, col_rules)
            
            elif semantic_type == "DOB":
                df_clean[col] = self._handle_semantic_dob(df_clean[col], col_rules)
            
            elif semantic_type == "EMAIL":
                df_clean[col] = self._handle_semantic_email(df_clean[col], col)
            
            elif semantic_type == "PHONE":
                df_clean[col] = self._handle_semantic_phone(df_clean[col], col)
    
        # 1. LEVEL: Data Type Enforcement
        for col in columns:
            col_rules = rules[col]
            if col_rules.get("convert_to_datetime", False):
                fmt = col_rules.get("date_format")
                try:
                    df_clean[col] = pd.to_datetime(df_clean[col], format=fmt, errors='coerce')
                    if self.logger:
                        self.logger.log_mutation("DYNAMIC_CLEANER", "convert_to_datetime", {"column": col})
                except: pass

        # 2. LEVEL: Statistical Heuristics (Imputation & Outliers)
        for col in columns:
            col_rules = rules[col]
            # Missing Values
            handle_missing = col_rules.get("handle_missing", "do_nothing")
            if handle_missing != "do_nothing" and df_clean[col].isna().any():
                df_clean[col] = self._impute_missing(df_clean[col], handle_missing)
                if self.logger:
                    self.logger.log_mutation("DYNAMIC_CLEANER", "impute_missing", {"column": col, "strategy": handle_missing})
            
            # Outliers
            if col_rules.get("handle_outliers") == "clip_at_bounds" and pd.api.types.is_numeric_dtype(df_clean[col]):
                df_clean[col] = self._clip_outliers(df_clean[col], method=outlier_method)
                if self.logger:
                    self.logger.log_mutation("DYNAMIC_CLEANER", "clip_outliers", {"column": col, "method": outlier_method})

        # 3. LEVEL: Cosmetic Standardization (Low Precedence)
        for col in columns:
            col_rules = rules[col]
            if df_clean[col].dtype == 'object':
                if col_rules.get("strip_whitespace", False):
                    df_clean[col] = df_clean[col].astype(str).str.strip()
                
                if col_rules.get("remove_special_chars", False):
                    df_clean[col] = df_clean[col].astype(str).apply(lambda x: re.sub(r'[^a-zA-Z0-9\s\.\-\/\:]', '', x))
                    if self.logger:
                        self.logger.log_mutation("DYNAMIC_CLEANER", "string_cleanup", {"column": col})
                
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

    def _clip_outliers(self, series: pd.Series, method: str = "iqr") -> pd.Series:
        non_null = series.dropna()
        if non_null.empty: return series
        
        if method == "iqr":
            q1 = non_null.quantile(0.25)
            q3 = non_null.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
        else: # z-score
            mean = non_null.mean()
            std = non_null.std()
            if std == 0: return series
            lower_bound = mean - 3 * std
            upper_bound = mean + 3 * std
            
        return series.clip(lower=lower_bound, upper=upper_bound)
