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

    def _detect_semantic_type(self, col_name: str, sample_series: pd.Series) -> str:
        """Infer semantic intent from column name and sample data."""
        name = str(col_name).lower().strip()
        
        # 1. Broad Name Matching
        is_dob = bool(re.search(r'\bdob\b|birth|date_of_birth|born|birth_date|bday', name))
        is_age = bool(re.search(r'\bage\b|player_age|user_age|customer_age', name))
        is_email = bool(re.search(r'email|mail_id|email_address', name))
        is_phone = bool(re.search(r'phone|mobile|contact|cell_no|tele', name))
        
        # 2. Excel Serial Heuristic (Stronger for mixed/object columns)
        # Attempt to see if column contains numeric values in the date range
        try:
            numeric_sample = pd.to_numeric(sample_series, errors='coerce').dropna()
            if not numeric_sample.empty:
                # Excel serials for 1980-2030 are roughly 29000-47500
                if is_dob and numeric_sample.between(20000, 60000).mean() > 0.5:
                    return "DOB"
        except: pass
        
        if is_dob: return "DOB"
        if is_age: return "AGE"
        if is_email: return "EMAIL"
        if is_phone: return "PHONE"
        
        return "UNKNOWN"

    def _check_dob_age_consistency(self, df: pd.DataFrame, dob_col: str, age_col: str) -> Dict[str, Any]:
        """Check if Age column is a duplicate of DOB serials or otherwise inconsistent."""
        issues = []
        inconsistencies = 0
        
        if dob_col and age_col:
            try:
                # Use numeric conversion for comparison to handle mixed types/strings
                dob_numeric = pd.to_numeric(df[dob_col], errors='coerce')
                age_numeric = pd.to_numeric(df[age_col], errors='coerce')
                
                # Check for exact duplication (common Excel data error)
                match_mask = dob_numeric.fillna(-1) == age_numeric.fillna(-1)
                both_val_match = match_mask & dob_numeric.notna()
                
                # If even 10% match and it's in the Excel date range, it's highly suspicious
                if both_val_match.any() and (dob_numeric[both_val_match] > 10000).all():
                    match_rate = both_val_match.mean()
                    if match_rate > 0.1: # 10% is enough to flag a quality issue if they are exactly same large numbers
                        issues.append(f"⚠️ Data Quality Issue: Age column '{age_col}' appears to be a duplicate of DOB '{dob_col}' (Excel serial dates detected in both)")
                        inconsistencies = int(both_val_match.sum())
            except: pass
                
        return {"issues": issues, "inconsistencies": inconsistencies}

    def _calculate_structured_metadata(self, df: pd.DataFrame, col: str, semantic_type: str) -> Dict[str, Any]:
        """Calculate structured metadata like missing, invalid, derived_from."""
        series = df[col]
        metadata = {
            "missing": int(series.isna().sum()),
            "invalid": 0
        }
        
        if semantic_type == "DOB":
            metadata["type"] = "date"
            # Detect Excel serials even in object columns
            numeric_vals = pd.to_numeric(series, errors='coerce').dropna()
            if not numeric_vals.empty and numeric_vals.between(10000, 100000).any():
                metadata["source_format"] = "excel_serial"
            else:
                metadata["source_format"] = "string"
        
        elif semantic_type == "EMAIL":
            pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_mask = series.dropna().astype(str).map(lambda x: not bool(re.match(pattern, x)))
            metadata["invalid"] = int(invalid_mask.sum())
            
        elif semantic_type == "PHONE":
            # Simple length check for invalid
            clean_phone = series.dropna().astype(str).str.replace(r'\D', '', regex=True)
            invalid_mask = clean_phone.map(lambda x: len(x) != 10)
            metadata["invalid_length"] = int(invalid_mask.sum())
            metadata["invalid"] = metadata["invalid_length"]
            
        return metadata

    def generate_cleaning_rules(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate suggested rules for the entire dataframe."""
        # --- ARCHITECTURAL CONTRACT: DATA TYPE ENFORCEMENT ---
        if not isinstance(df, pd.DataFrame):
            try:
                df = pd.DataFrame(df)
            except:
                raise TypeError(f"RuleGenerator expects a pandas.DataFrame, but received {type(df)}")
            
        rules = {"_global": {}}
        analysis = {}
        
        # Global analysis
        has_dups = df.duplicated().any()
        rules["_global"]["remove_duplicates"] = bool(has_dups)
        rules["_global"]["outlier_method"] = self.settings.get("outlier_method", "iqr")
        
        # 1. Advanced Semantic Detection
        semantic_types = {col: self._detect_semantic_type(col, df[col]) for col in df.columns}
        dob_cols = [col for col, stype in semantic_types.items() if stype == "DOB"]
        age_cols = [col for col, stype in semantic_types.items() if stype == "AGE"]

        # 2. Cross-Column Consistency (DOB vs AGE)
        consistency_results = {}
        if dob_cols and age_cols:
            consistency_results = self._check_dob_age_consistency(df, dob_cols[0], age_cols[0])
            if consistency_results["issues"]:
                rules["_global"]["quality_alerts"] = consistency_results["issues"]

        for col in df.columns:
            col_stats = self.analyze_column(df[col])
            analysis[col] = col_stats
            semantic_type = semantic_types[col]
            analysis[col]["semantic_type"] = semantic_type
            
            # Structured Metadata per Requirement
            semantic_meta = self._calculate_structured_metadata(df, col, semantic_type)
            analysis[col]["semantic_metadata"] = semantic_meta
            
            # Suggest rules based on stats and semantic type
            col_rules = {"semantic_type": semantic_type}
            
            # 3. Semantic-Aware Rules
            if semantic_type == "AGE":
                if dob_cols:
                    col_rules["handle_semantic"] = "calculate_age_from_dob"
                    col_rules["dob_source_column"] = dob_cols[0]
                    col_rules["derived_from"] = dob_cols[0]
                    col_rules["calculation"] = "current_date - dob"
                    col_rules["ignore_original"] = True # No silent filling, derive fresh
                    col_rules["inconsistencies"] = consistency_results.get("inconsistencies", 0)
                else:
                    col_rules["handle_semantic"] = "range_validation"
                    col_rules["range_min"] = 0
                    col_rules["range_max"] = 120
            
            elif semantic_type == "DOB":
                col_rules["handle_semantic"] = "normalize_date"
                col_rules["date_output_format"] = "DD-MM-YYYY" # Default
                col_rules["convert_to_datetime"] = True
                col_rules["source_format"] = semantic_meta.get("source_format")
            
            elif semantic_type == "EMAIL":
                col_rules["handle_semantic"] = "validate_email"
                col_rules["require_domain"] = True
                col_rules["auto_correct"] = False # Requirement: Flag, don't fix
                col_rules["invalid"] = semantic_meta.get("invalid", 0)
            
            elif semantic_type == "PHONE":
                col_rules["handle_semantic"] = "validate_phone"
                col_rules["region"] = "IN" # Requirement: Country-aware (India)
                col_rules["digit_count"] = 10
                col_rules["invalid_length"] = semantic_meta.get("invalid_length", 0)
            
            # 4. Missing Value Reporting (Requirement 4)
            # Ensure missing counts are prominent in the rules/analysis
            col_rules["missing_count"] = semantic_meta.get("missing", col_stats["missing_count"])

            # 5. General Fallback Rules
            if "handle_semantic" not in col_rules:
                # Missing Value Handling
                if col_stats["missing_percentage"] > 0:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        col_rules["handle_missing"] = "impute_with_median"
                    else:
                        col_rules["handle_missing"] = "fill_with_mode"
                else:
                    col_rules["handle_missing"] = "do_nothing"
                    
                # String/Numeric Cleaning
                if pd.api.types.is_object_dtype(df[col]):
                    col_rules["strip_whitespace"] = True
                    if col_stats.get("has_special_chars"):
                        col_rules["remove_special_chars"] = True
                    if col_stats.get("is_likely_date"):
                        col_rules["convert_to_datetime"] = True
                
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
