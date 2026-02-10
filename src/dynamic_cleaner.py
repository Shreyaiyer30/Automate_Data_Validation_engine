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

            semantic_type = col_rules.get("semantic_type", "").upper()
            if semantic_type in {"DOB", "DATE", "DATETIME"} and handle_missing in {"impute_with_median", "impute_with_mean"}:
                handle_missing = "fill_with_mode"   # or a new "impute_with_median_date" if you add one

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

    def _convert_excel_serial_to_date(self, val):
        """Helper to convert Excel serial numbers (e.g., 35845) to datetime."""
        try:
            # Excel's base date is December 30, 1899
            # Ensure val is a float/int
            num_val = float(val)
            if 10000 <= num_val <= 100000: # Reasonable range for dates
                return pd.to_datetime('1899-12-30') + pd.to_timedelta(num_val, unit='D')
            return pd.NaT
        except:
            return pd.NaT

    def _handle_semantic_age(self, df: pd.DataFrame, col: str, rules: Dict) -> pd.Series:
        series = df[col]
        strategy = rules.get("handle_semantic")
        
        if strategy == "calculate_age_from_dob":
            dob_col = rules.get("dob_source_column")
            if dob_col and dob_col in df.columns:
                try:
                    # Derivation logic: Always re-calculate, do not trust original Age value
                    dob_series = df[dob_col]
                    
                    # Convert to numeric where possible to catch Excel serials
                    dob_numeric = pd.to_numeric(dob_series, errors='coerce')
                    is_serial = dob_numeric.notna()
                    
                    dob = pd.Series(index=dob_series.index, dtype='datetime64[ns]')
                    # Handle serials
                    if is_serial.any():
                        dob[is_serial] = dob_numeric[is_serial].map(self._convert_excel_serial_to_date)
                    # Handle strings for the rest
                    if (~is_serial).any():
                        dob[~is_serial] = pd.to_datetime(dob_series[~is_serial], errors='coerce')
                    
                    age = (pd.Timestamp.now() - dob).dt.days // 365.25
                    
                    if self.logger:
                        self.logger.log_mutation("SEMANTIC_ENGINE", "derive_age_from_dob", {
                            "source_col": dob_col, 
                            "target_col": col,
                            "note": "Age derived fresh from DOB (handled mixed string/serial format)."
                        })
                    return age
                except: pass
        
        # Default: Range Validation
        r_min, r_max = rules.get("range_min", 0), rules.get("range_max", 120)
        invalid_mask = pd.to_numeric(series, errors='coerce').map(lambda x: not (r_min <= x <= r_max) if not pd.isna(x) else False)
        if invalid_mask.any() and self.logger:
            self.logger.log_mutation("SEMANTIC_VALIDATOR", "out_of_range_age", {"column": col, "count": int(invalid_mask.sum())})
        return series

    def _handle_semantic_dob(self, series: pd.Series, rules: Dict) -> pd.Series:
        try:
            # Handle mixed serials and strings
            numeric_vals = pd.to_numeric(series, errors='coerce')
            is_serial = numeric_vals.notna()
            
            dates = pd.Series(index=series.index, dtype='datetime64[ns]')
            if is_serial.any():
                dates[is_serial] = numeric_vals[is_serial].map(self._convert_excel_serial_to_date)
            if (~is_serial).any():
                dates[~is_serial] = pd.to_datetime(series[~is_serial], errors='coerce')
                
            fmt = rules.get("date_output_format", "DD-MM-YYYY")
            
            if fmt == "DD-MM-YYYY":
                normalized = dates.dt.strftime('%d-%m-%Y')
            elif fmt == "DD Month YYYY":
                normalized = dates.dt.strftime('%d %B %Y')
            else:
                normalized = dates.dt.strftime('%Y-%m-%d')
                
            invalid_count = int(series.notna().sum() - dates.notna().sum())
            if invalid_count > 0 and self.logger:
                self.logger.log_mutation("SEMANTIC_VALIDATOR", "invalid_dob_format", {"column": series.name, "count": invalid_count})
            
            return normalized.fillna(series) # Preserve original if conversion failed
        except:
            return series

    def _handle_semantic_email(self, series: pd.Series, col_name: str) -> pd.Series:
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        invalid_mask = series.astype(str).map(lambda x: not bool(re.match(pattern, x)) if x != 'nan' and not pd.isna(x) else False)
        
        if invalid_mask.any() and self.logger:
            self.logger.log_mutation("SEMANTIC_VALIDATOR", "invalid_email", {"column": col_name, "count": int(invalid_mask.sum())})
        
        return series # Requirement: Flag, don't auto-correct

    def _handle_semantic_phone(self, series: pd.Series, col_name: str) -> pd.Series:
        # India specific rule: Exactly 10 digits
        pattern = r'^\d{10}$'
        def check_phone(val):
            if pd.isna(val) or str(val) == 'nan': return False
            clean_val = re.sub(r'\D', '', str(val))
            return not bool(re.match(pattern, clean_val))
            
        invalid_mask = series.map(check_phone)
        
        if invalid_mask.any() and self.logger:
            self.logger.log_mutation("SEMANTIC_VALIDATOR", "invalid_phone_in", {"column": col_name, "count": int(invalid_mask.sum())})
        
        return series # Flag, don't auto-correct

    def _impute_missing(self, series: pd.Series, strategy: str) -> pd.Series:
        if series is None or series.empty:
            return series

        s = series.copy()

        # Treat empty strings as missing
        if s.dtype == "object":
            s = s.replace(r"^\s*$", np.nan, regex=True)

        def mode_value(x: pd.Series):
            m = x.dropna().mode()
            return m.iloc[0] if not m.empty else None

        if strategy == "impute_with_median":
            # 1) Try numeric coercion FIRST (handles numeric strings safely)
            num = pd.to_numeric(s, errors="coerce")
            if num.notna().mean() >= 0.6:
                med = num.dropna().median()
                return s.fillna(med)

            # 2) Try datetime coercion (handles DOB/date strings)
            dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
            if dt.notna().mean() >= 0.6:
                if dt.dropna().empty:
                    return s
                median_ts = dt.dropna().astype("int64").median()
                median_date = pd.to_datetime(int(median_ts))
                dt_filled = dt.fillna(median_date)
                return dt_filled.dt.strftime("%Y-%m-%d")

            # 3) Fallback to mode (categorical/text)
            mv = mode_value(s)
            return s.fillna(mv if mv is not None else "Unknown")

        elif strategy == "impute_with_mean":
            num = pd.to_numeric(s, errors="coerce")
            if num.notna().mean() >= 0.6:
                mean = num.dropna().mean()
                return s.fillna(mean)

            mv = mode_value(s)
            return s.fillna(mv if mv is not None else "Unknown")

        elif strategy == "fill_with_mode":
            mv = mode_value(s)
            return s.fillna(mv if mv is not None else "Unknown")

        elif strategy == "fill_with_unknown":
           return s.fillna(np.nan)

        elif strategy == "forward_fill":
            return s.ffill()

        elif strategy == "backward_fill":
            return s.bfill()
        
        return s

    def _clip_outliers(self, series: pd.Series, method: str = "iqr") -> pd.Series:
        non_null = series.dropna()
        if non_null.empty:
            return series

        if method == "iqr":
            q1 = non_null.quantile(0.25)
            q3 = non_null.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
        else:  # z-score
            mean = non_null.mean()
            std = non_null.std()
            if std == 0:
                return series
            lower_bound = mean - 3 * std
            upper_bound = mean + 3 * std

        return series.clip(lower=lower_bound, upper=upper_bound)
