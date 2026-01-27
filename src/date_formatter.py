# src/date_formatter.py
import pandas as pd
import numpy as np
from typing import List, Optional
import re
from datetime import datetime

class DateFormatter:
    """Handle date format detection and conversion"""
    
    # Common date patterns
    DATE_PATTERNS = {
        'MM/DD/YYYY': r'^\d{1,2}/\d{1,2}/\d{4}$',
        'DD/MM/YYYY': r'^\d{1,2}/\d{1,2}/\d{4}$',
        'YYYY-MM-DD': r'^\d{4}-\d{1,2}-\d{1,2}$',
        'MM-DD-YYYY': r'^\d{1,2}-\d{1,2}-\d{4}$',
        'DD-MM-YYYY': r'^\d{1,2}-\d{1,2}-\d{4}$'
    }
    
    @staticmethod
    def detect_date_format(series: pd.Series) -> Optional[str]:
        """Detect the date format in a series"""
        if series.empty:
            return None
        
        # Sample some values
        sample = series.dropna().head(100)
        
        for pattern_name, pattern in DateFormatter.DATE_PATTERNS.items():
            matches = sample.astype(str).str.match(pattern)
            if matches.mean() > 0.8:  # 80% match rate
                return pattern_name
        
        return None
    
    @staticmethod
    def convert_dates(df: pd.DataFrame, target_format: str = '%d-%m-%Y') -> pd.DataFrame:
        """Convert dates in DataFrame to target format"""
        df_processed = df.copy()
        
        for col in df.columns:
            # Skip if column name suggests it's not a date
            if any(keyword in col.lower() for keyword in ['id', 'code', 'number', 'value', 'amount']):
                continue
                
            # Try to parse dates
            try:
                # First attempt with dayfirst=False (MM/DD/YYYY)
                dates_mmddyyyy = pd.to_datetime(df[col], errors='coerce', dayfirst=False)
                
                # Second attempt with dayfirst=True (DD/MM/YYYY)
                dates_ddmmyyyy = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                
                # Choose the one with more valid dates
                valid_mmddyyyy = dates_mmddyyyy.notna().sum()
                valid_ddmmyyyy = dates_ddmmyyyy.notna().sum()
                
                if valid_mmddyyyy > 0 or valid_ddmmyyyy > 0:
                    # Use whichever has more valid dates
                    if valid_mmddyyyy >= valid_ddmmyyyy:
                        dates = dates_mmddyyyy
                        detected_format = "MM/DD/YYYY" if valid_mmddyyyy > 0 else "unknown"
                    else:
                        dates = dates_ddmmyyyy
                        detected_format = "DD/MM/YYYY" if valid_ddmmyyyy > 0 else "unknown"
                    
                    # Format to target
                    if target_format == '%d-%m-%Y':
                        df_processed[col] = dates.dt.strftime('%d-%m-%Y')
                    elif target_format == '%Y-%m-%d':
                        df_processed[col] = dates.dt.strftime('%Y-%m-%d')
                    elif target_format == 'datetime':
                        df_processed[col] = dates  # Keep as datetime object
                    else:
                        df_processed[col] = dates.dt.strftime(target_format)
                    
                    print(f"Formatted {col}: {detected_format} -> {target_format} ({valid_mmddyyyy+valid_ddmmyyyy} dates)")
                
            except Exception as e:
                print(f"Could not parse dates in {col}: {e}")
                continue
        
        return df_processed
    
    @staticmethod
    def is_date_column(series: pd.Series, threshold: float = 0.5) -> bool:
        """Check if a column contains dates"""
        if series.empty:
            return False
        
        # Sample values
        sample = series.dropna().head(100)
        if sample.empty:
            return False
        
        # Try to parse as dates
        try:
            parsed = pd.to_datetime(sample, errors='coerce')
            valid_ratio = parsed.notna().sum() / len(sample)
            return valid_ratio >= threshold
        except:
            return False

    @staticmethod
    def parse_date_column(series: pd.Series, format: str) -> pd.Series:
        """
        Parse a column with an explicit date format.
        """
        return pd.to_datetime(series, format=format, errors='coerce')