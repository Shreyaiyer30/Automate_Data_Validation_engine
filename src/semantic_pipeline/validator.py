from typing import List, Dict, Any, Optional
import pandas as pd
import re
from datetime import datetime
from src.semantic_pipeline.types import ColumnType, ValidationIssue

class ValidationEngine:
    """
    Applies rule-based validation logic based on detected column types.
    """
    
    def validate(self, df: pd.DataFrame, type_map: Dict[str, Any]) -> List[ValidationIssue]:
        issues = []
        for col, meta in type_map.items():
            if col not in df.columns: continue
            
            series = df[col]
            ctype = meta.detected_type
            
            if ctype == ColumnType.PERSON_NAME:
                issues.extend(self._validate_name(col, series))
            elif ctype == ColumnType.PHONE_NUMBER:
                issues.extend(self._validate_phone(col, series))
            elif ctype == ColumnType.EMAIL:
                issues.extend(self._validate_email(col, series))
            elif ctype == ColumnType.DATE_OF_BIRTH:
                issues.extend(self._validate_dob(col, series))
            elif ctype == ColumnType.AGE:
                issues.extend(self._validate_age(col, series))
            elif ctype == ColumnType.LOCATION:
                issues.extend(self._validate_location(col, series))
            elif ctype == ColumnType.GENDER:
                issues.extend(self._validate_gender(col, series))
                
        # Cross-column validation
        issues.extend(self._validate_consistency(df, type_map))
        return issues

    def _validate_name(self, col: str, series: pd.Series) -> List[ValidationIssue]:
        issues = []
        # Check for single word names
        single_word = series.dropna().astype(str).apply(lambda x: len(x.strip().split()) < 2)
        if single_word.any():
            issues.append(ValidationIssue(
                col, ColumnType.PERSON_NAME, "WARNING", "NAME_format", 
                "Single word names found (potential missing surname)", 
                int(single_word.sum()), series[single_word].head(3).tolist(), "Review manually"
            ))
        return issues

    def _validate_phone(self, col: str, series: pd.Series) -> List[ValidationIssue]:
        issues = []
        # Pattern check (Generic 10-digit)
        pattern = r'^\d{10}$'
        invalid = series.dropna().astype(str).apply(lambda x: not bool(re.search(pattern, re.sub(r'\D', '', x))))
        if invalid.any():
            issues.append(ValidationIssue(
                col, ColumnType.PHONE_NUMBER, "ERROR", "PHONE_format", 
                "Invalid phone format (expected 10 digits)", 
                int(invalid.sum()), series[invalid].head(3).tolist(), "Standardize format"
            ))
        return issues

    def _validate_email(self, col: str, series: pd.Series) -> List[ValidationIssue]:
        issues = []
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        invalid = series.dropna().astype(str).str.lower().apply(lambda x: not bool(re.match(pattern, x)))
        if invalid.any():
            issues.append(ValidationIssue(
                col, ColumnType.EMAIL, "ERROR", "EMAIL_format", 
                "Invalid email format", 
                int(invalid.sum()), series[invalid].head(3).tolist(), "Check for typos"
            ))
        return issues

    def _validate_dob(self, col: str, series: pd.Series) -> List[ValidationIssue]:
        issues = []
        # Check for future dates
        try:
            dates = pd.to_datetime(series, errors='coerce')
            future = dates > pd.Timestamp.now()
            if future.any():
                issues.append(ValidationIssue(
                    col, ColumnType.DATE_OF_BIRTH, "ERROR", "DOB_range", 
                    "Future dates detected in DOB", 
                    int(future.sum()), series[future].head(3).tolist(), "Correction needed"
                ))
        except: pass
        return issues

    def _validate_age(self, col: str, series: pd.Series) -> List[ValidationIssue]:
        issues = []
        try:
            nums = pd.to_numeric(series, errors='coerce')
            invalid = (nums < 0) | (nums > 120)
            if invalid.any():
                issues.append(ValidationIssue(
                    col, ColumnType.AGE, "ERROR", "AGE_range", 
                    "Age out of valid range (0-120)", 
                    int(invalid.sum()), series[invalid].head(3).tolist(), "Check data source"
                ))
        except: pass
        return issues

    def _validate_location(self, col: str, series: pd.Series) -> List[ValidationIssue]:
        issues = []
        # Check for inconsistent casing
        valid_strings = series.dropna().astype(str)
        if len(valid_strings) == 0: return issues
        
        # Check title case ratio
        title_case_ratio = valid_strings.apply(lambda x: x.istitle()).mean()
        if 0.1 < title_case_ratio < 0.9: # Mixed casing usage
            issues.append(ValidationIssue(
                col, ColumnType.LOCATION, "INFO", "LOCATION_style", 
                "Inconsistent casing detected (mix of Title/Lower/Upper)", 
                int(len(valid_strings) * (1-title_case_ratio)), 
                valid_strings[~valid_strings.apply(lambda x: x.istitle())].head(3).tolist(), 
                "Standardize to Title Case"
            ))
        return issues

    def _validate_gender(self, col: str, series: pd.Series) -> List[ValidationIssue]:
        issues = []
        valid = {'male', 'female', 'other', 'm', 'f', 'o', 'unknown'}
        invalid = series.dropna().astype(str).str.lower().apply(lambda x: x not in valid)
        if invalid.any():
            issues.append(ValidationIssue(
                col, ColumnType.GENDER, "WARNING", "GENDER_standard", 
                "Non-standard gender values", 
                int(invalid.sum()), series[invalid].head(3).tolist(), "Map to standard values"
            ))
        return issues

    def _validate_consistency(self, df: pd.DataFrame, type_map: Dict) -> List[ValidationIssue]:
        issues = []
        age_cols = [c for c, m in type_map.items() if m.detected_type == ColumnType.AGE]
        dob_cols = [c for c, m in type_map.items() if m.detected_type == ColumnType.DATE_OF_BIRTH]
        
        if age_cols and dob_cols:
            age_col = age_cols[0]
            dob_col = dob_cols[0]
            
            try:
                # Calculate expected age
                dob = pd.to_datetime(df[dob_col], errors='coerce')
                reported_age = pd.to_numeric(df[age_col], errors='coerce')
                
                # Filter valid pairs
                mask = dob.notna() & reported_age.notna()
                if not mask.any():
                    return issues
                
                now = pd.Timestamp.now()
                # Rough calculation: (Now - DOB) / 365.25
                expected_age = (now - dob[mask]).dt.days / 365.25
                
                # Check absolute difference > 2 years (generous buffer)
                diff = abs(expected_age - reported_age[mask])
                mismatch = diff > 2
                
                if mismatch.any():
                    count = mismatch.sum()
                    examples = []
                    # Get examples: {DOB: x, Age: y}
                    idx_mismatch = mismatch[mismatch].index[:3]
                    for idx in idx_mismatch:
                        examples.append({
                            "DOB": str(df.loc[idx, dob_col]),
                            "Age": str(df.loc[idx, age_col]),
                            "Expected": f"{expected_age[idx]:.1f}"
                        })
                        
                    issues.append(ValidationIssue(
                        f"{dob_col} <> {age_col}", 
                        ColumnType.UNKNOWN, # Virtual type for relationships
                        "WARNING", "CROSS_DOB_AGE_MISMATCH",
                        f"Age does not match DOB (>{count} inconsistencies)",
                        int(count), examples, "Re-derive Age from DOB"
                    ))
            except Exception as e:
                pass 
        
        # Name vs Email Consistency
        name_cols = [c for c, m in type_map.items() if m.detected_type == ColumnType.PERSON_NAME]
        email_cols = [c for c, m in type_map.items() if m.detected_type == ColumnType.EMAIL]
        
        if name_cols and email_cols:
            name_col = name_cols[0]
            email_col = email_cols[0]
            
            try:
                # Check if email contains parts of name
                mismatches = []
                for idx, row in df.iterrows():
                    name = str(row[name_col]).lower()
                    email = str(row[email_col]).lower()
                    if pd.isna(row[name_col]) or pd.isna(row[email_col]): continue
                    if '@' not in email: continue
                    
                    email_user = email.split('@')[0]
                    # Simple heuristic: at least one name token (len>2) should be in email user part
                    name_tokens = [t for t in re.split(r'\s+', name) if len(t) > 2]
                    
                    if name_tokens and not any(t in email_user for t in name_tokens):
                        # Also check reverse: is email user part in name? (e.g. jdoe in John Doe)
                        # Very basic check: first char of first name + last name
                        if len(name_tokens) >= 2:
                            initials_last = name_tokens[0][0] + name_tokens[-1]
                            if initials_last in email_user: continue
                            
                        mismatches.append(f"{row[name_col]} <-> {row[email_col]}")
                
                if len(mismatches) > 0:
                    issues.append(ValidationIssue(
                        f"{name_col} <> {email_col}",
                        ColumnType.UNKNOWN,
                        "INFO", "CROSS_NAME_EMAIL_MISMATCH",
                        f"Email does not appear to match Name ({len(mismatches)} potential)",
                        len(mismatches), mismatches[:3], "Manual verification required"
                    ))
            except: pass
                
        return issues
