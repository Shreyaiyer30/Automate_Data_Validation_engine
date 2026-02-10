import pandas as pd
import re
from typing import Dict, List, Any
from src.semantic_pipeline.types import ColumnType, TypeMetadata

class ColumnTypeDetector:
    """
    Dynamically infers semantic types for DataFrame columns based on headers and value patterns.
    """
    
    HEADER_KEYWORDS = {
        ColumnType.PERSON_NAME: [r'name', r'fullname', r'person', r'customer', r'client'],
        ColumnType.PHONE_NUMBER: [r'phone', r'mobile', r'contact', r'tel', r'cell'],
        ColumnType.EMAIL: [r'email', r'mail', r'e-mail'],
        ColumnType.DATE_OF_BIRTH: [r'dob', r'birth', r'born', r'bday'],
        ColumnType.AGE: [r'age', r'years', r'yr', r'old'],
        ColumnType.LOCATION: [r'city', r'state', r'country', r'address', r'location', r'place'],
        ColumnType.GENDER: [r'gender', r'sex', r'm/f'],
    }
    
    REGEX_PATTERNS = {
        ColumnType.EMAIL: r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        ColumnType.PHONE_NUMBER: r'^(\+?\d{1,3}[-.\s]?)?(\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}$', # Generic international
        ColumnType.DATE_OF_BIRTH: r'^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$|^\d{2}-\d{2}-\d{4}$', # ISO/Common
    }

    def detect_types(self, df: pd.DataFrame) -> Dict[str, TypeMetadata]:
        results = {}
        for col in df.columns:
            results[col] = self._infer_column(col, df[col])
        return results

    def _infer_column(self, col_name: str, series: pd.Series) -> TypeMetadata:
        matches = []
        name = str(col_name).lower().strip()
        
        # 1. Header Analysis
        for ctype, patterns in self.HEADER_KEYWORDS.items():
            for pat in patterns:
                if re.search(pat, name):
                    matches.append((ctype, 0.4, f"Header match: {pat}"))
                    break
        
        # 2. Value Analysis (Sampled)
        non_null = series.dropna().astype(str)
        if len(non_null) > 0:
            sample = non_null.sample(min(100, len(non_null)), random_state=42)
            
            # Regex checks
            for ctype, pattern in self.REGEX_PATTERNS.items():
                match_count = sample.apply(lambda x: bool(re.match(pattern, x.strip()))).sum()
                ratio = match_count / len(sample)
                if ratio > 0.5:
                    matches.append((ctype, ratio * 0.6, f"Regex match ratio: {ratio:.2f}"))

            # Boolean check
            bool_tokens = {'true', 'false', 'yes', 'no', 'y', 'n', '0', '1'}
            bool_ratio = sample.apply(lambda x: x.lower() in bool_tokens).mean()
            if bool_ratio > 0.8:
                matches.append((ColumnType.BOOLEAN, bool_ratio, "Boolean token analysis"))
            
            # Numeric check (if not already phone/date)
            try:
                pd.to_numeric(sample)
                matches.append((ColumnType.NUMERIC, 0.5, "Numeric conversion success"))
            except: pass
            
            # Categorical check
            unique_ratio = series.nunique() / len(series)
            if unique_ratio < 0.2 and len(series) > 20: 
                matches.append((ColumnType.CATEGORICAL, 0.6, "Low cardinality"))
        
        # Date parsing check (expensive, do last if needed)
        # ... logic for deeper date checks ...

        if not matches:
            return TypeMetadata(ColumnType.UNKNOWN, 0.0, ["No patterns matched"])
            
        # Aggregate scores
        best_type, best_score, best_reasons = self._select_best_match(matches)
        return TypeMetadata(best_type, min(1.0, best_score), best_reasons)

    def _select_best_match(self, matches):
        # Weighted scoring logic...
        # Prefer specific types over generic (Phone > Numeric)
        scores = {}
        reasons = {}
        
        for ctype, score, reason in matches:
            # Semantic boost
            if ctype in [ColumnType.EMAIL, ColumnType.PHONE_NUMBER, ColumnType.DATE_OF_BIRTH, 
                         ColumnType.AGE, ColumnType.PERSON_NAME, ColumnType.GENDER, ColumnType.LOCATION]:
                score *= 1.5 # Stronger boost for semantic types over generic NUMERIC/CATEGORICAL
            
            scores[ctype] = scores.get(ctype, 0) + score
            reasons.setdefault(ctype, []).append(reason)
            
        best = max(scores.items(), key=lambda x: x[1])
        return best[0], best[1], reasons[best[0]]
