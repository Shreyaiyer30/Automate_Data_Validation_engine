import pandas as pd
import numpy as np
import re
from typing import Dict, Any, Tuple, List
from src.semantic_pipeline.types import ColumnType, ChangeLog

class CleaningOperations:
    """
    Standardization and cleaning logic per type.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}

    def clean(self, df: pd.DataFrame, type_map: Dict[str, Any]) -> Tuple[pd.DataFrame, List[ChangeLog]]:
        df_clean = df.copy()
        logs = []
        
        for col, meta in type_map.items():
            if col not in df_clean.columns: continue
            
            ctype = meta.detected_type
            initial_sample = df_clean[col].head(3).tolist()
            
            if ctype == ColumnType.PERSON_NAME:
                df_clean[col], log = self._clean_name(col, df_clean[col])
            elif ctype == ColumnType.PHONE_NUMBER:
                df_clean[col], log = self._clean_phone(col, df_clean[col])
            elif ctype == ColumnType.EMAIL:
                df_clean[col], log = self._clean_email(col, df_clean[col])
            elif ctype == ColumnType.DATE_OF_BIRTH:
                df_clean[col], log = self._clean_date(col, df_clean[col])
            elif ctype == ColumnType.AGE:
                df_clean[col], log = self._clean_age(col, df_clean[col])
            elif ctype == ColumnType.LOCATION:
                df_clean[col], log = self._clean_location(col, df_clean[col])
            elif ctype == ColumnType.GENDER:
                df_clean[col], log = self._clean_gender(col, df_clean[col])
            else:
                log = None
                
            if log and log.rows_changed > 0:
                logs.append(log)
                
        return df_clean, logs

    def _clean_name(self, col: str, series: pd.Series) -> Tuple[pd.Series, ChangeLog]:
        # Fill NaN temporary with placeholder? No, just handle in map.
        def clean_val(x):
            if pd.isna(x): return x
            s = str(x).strip().title()
            s = re.sub(r'[^\w\s\-\']', '', s)
            return s if s else None

        cleaned = series.apply(clean_val)
        changes = (series.fillna('').astype(str) != cleaned.fillna('').astype(str)).sum()
        return cleaned, ChangeLog(col, "Standardize Name (Title Case, Strip)", changes)

    def _clean_phone(self, col: str, series: pd.Series) -> Tuple[pd.Series, ChangeLog]:
        region = self.config.get('phone', {}).get('default_country', 'IN')
        
        def clean_val(x):
            if pd.isna(x): return x
            s = re.sub(r'\D', '', str(x))
            
            # Simple heuristic for IN/US based on config
            if region == 'IN' and len(s) == 10: 
                return f"+91-{s}"
            elif region == 'US' and len(s) == 10:
                return f"+1-{s}"
                
            return s if s else None
            
        cleaned = series.apply(clean_val)
        changes = (series.fillna('').astype(str) != cleaned.fillna('').astype(str)).sum()
        return cleaned, ChangeLog(col, f"Format Phone (Digits, Region: {region})", changes)

    def _clean_email(self, col: str, series: pd.Series) -> Tuple[pd.Series, ChangeLog]:
        def clean_val(x):
            if pd.isna(x): return x
            s = str(x).strip().lower()
            return s if s else None

        cleaned = series.apply(clean_val)
        changes = (series.fillna('').astype(str).str.lower() != cleaned.fillna('').astype(str)).sum()
        return cleaned, ChangeLog(col, "Normalize Email", changes)

    def _clean_date(self, col: str, series: pd.Series) -> Tuple[pd.Series, ChangeLog]:
        # Try multiple strategies
        def parse_date(x):
            if pd.isna(x) or str(x).lower().strip() in ['nan', 'nat', '']: return pd.NaT
            # Remove time?
            x = str(x).strip()
            for fmt in ['%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                try:
                    return pd.to_datetime(x, format=fmt)
                except: continue
            try:
                # Fallback to inference
                return pd.to_datetime(x)
            except:
                return pd.NaT

        # Ensure datetime dtype before using .dt accessor
        parsed = pd.to_datetime(series.apply(parse_date), errors='coerce')
        cleaned = parsed.dt.strftime('%Y-%m-%d')
        changes = (series.fillna('').astype(str) != cleaned.fillna('').astype(str)).sum()
        return cleaned, ChangeLog(col, "Standardize Date (ISO)", changes)

    def _clean_age(self, col: str, series: pd.Series) -> Tuple[pd.Series, ChangeLog]:
        cleaned = pd.to_numeric(series, errors='coerce')
        # Fill NaN? Or leave it?
        changes = (series.notna() & cleaned.isna()).sum() # Failed conversions
        return cleaned, ChangeLog(col, "Numeric Conversion", changes)

    def _clean_location(self, col: str, series: pd.Series) -> Tuple[pd.Series, ChangeLog]:
        cleaned = series.astype(str).str.strip().str.title()
        changes = (series.fillna('') != cleaned).sum()
        return cleaned, ChangeLog(col, "Title Case Location", changes)

    def _clean_gender(self, col: str, series: pd.Series) -> Tuple[pd.Series, ChangeLog]:
        mapping = {
            'm': 'Male', 'male': 'Male', 'man': 'Male',
            'f': 'Female', 'female': 'Female', 'woman': 'Female',
            'o': 'Other', 'other': 'Other'
        }
        cleaned = series.astype(str).str.lower().map(mapping).fillna('Unknown')
        changes = (series.fillna('').astype(str).str.lower() != cleaned.str.lower()).sum()
        return cleaned, ChangeLog(col, "Standardize Gender", changes)
