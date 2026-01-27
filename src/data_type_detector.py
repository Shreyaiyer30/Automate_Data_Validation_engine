# src/data_type_detector.py
import pandas as pd
import numpy as np
import re
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime

class DataTypeDetector:
    """Intelligent data type detection and conversion"""
    
    # Patterns for different data types
    PATTERNS = {
        'integer': r'^-?\d+$',
        'float': r'^-?\d*\.\d+$',
        'scientific': r'^-?\d+\.?\d*[eE][+-]?\d+$',
        'currency': r'^[+-]?[\$€£¥]?\s?[\d,.]+(\.\d{2})?\s?[\$€£¥]?$',
        'percent': r'^-?\d+(\.\d+)?%$',
        'boolean': r'^(true|false|yes|no|0|1|t|f|y|n)$',
        'time_24h': r'^\d{1,2}:\d{2}(:\d{2})?$',
        'time_12h': r'^\d{1,2}:\d{2}\s?(AM|PM|am|pm)$',
        'date_mmddyyyy': r'^\d{1,2}/\d{1,2}/\d{4}$',
        'date_ddmmyyyy': r'^\d{1,2}-\d{1,2}-\d{4}$',
        'date_yyyymmdd': r'^\d{4}-\d{1,2}-\d{1,2}$',
        'quoted_number': r'^["\']\s*-?[\d,.]+\s*["\']$',
    }
    
    @staticmethod
    def detect_column_type(series: pd.Series) -> Dict[str, Any]:
        """Detect the most likely data type for a column"""
        if series.empty:
            return {'type': 'unknown', 'confidence': 0.0}
        
        # Sample data for analysis
        sample = series.dropna().astype(str).str.strip().head(100)
        if sample.empty:
            return {'type': 'unknown', 'confidence': 0.0}
        
        # Test patterns
        results = {}
        
        for type_name, pattern in DataTypeDetector.PATTERNS.items():
            regex = re.compile(pattern, re.IGNORECASE)
            matches = sample.apply(lambda x: bool(regex.match(str(x))))
            match_rate = matches.mean()
            results[type_name] = match_rate
        
        # Additional detection for numeric columns
        try:
            # Try to convert to numeric
            numeric_series = pd.to_numeric(sample, errors='coerce')
            numeric_count = numeric_series.notna().sum()
            numeric_rate = numeric_count / len(sample)
            
            if numeric_rate > 0.8:
                # Check if all are integers
                if (numeric_series.dropna() % 1 == 0).all():
                    results['integer_numeric'] = numeric_rate
                else:
                    results['float_numeric'] = numeric_rate
        except:
            pass
        
        # Find best match
        if results:
            best_type = max(results.items(), key=lambda x: x[1])
            if best_type[1] > 0.6:  # Lowered to 60% confidence threshold
                return {
                    'type': best_type[0],
                    'confidence': best_type[1],
                    'all_results': results
                }
        
        # Check if it's categorical
        unique_ratio = series.nunique() / len(series) if len(series) > 0 else 1.0
        if unique_ratio < 0.3 and len(series) > 10:
            return {'type': 'categorical', 'confidence': 1 - unique_ratio}
        
        # Default to string
        return {'type': 'string', 'confidence': 1.0}
    
    @staticmethod
    def convert_column(series: pd.Series, target_type: str = None) -> pd.Series:
        """Convert column to appropriate data type"""
        if series.empty:
            return series
        
        # If target_type not specified, auto-detect
        if not target_type:
            detection = DataTypeDetector.detect_column_type(series)
            target_type = detection['type']
        
        # Convert based on detected type
        if target_type in ['integer', 'integer_numeric']:
            return DataTypeDetector._convert_to_integer(series)
        elif target_type in ['float', 'float_numeric', 'scientific']:
            return DataTypeDetector._convert_to_float(series)
        elif target_type == 'currency':
            return DataTypeDetector._convert_to_currency(series)
        elif target_type == 'percent':
            return DataTypeDetector._convert_to_percent(series)
        elif target_type == 'boolean':
            return DataTypeDetector._convert_to_boolean(series)
        elif target_type in ['time_24h', 'time_12h']:
            return DataTypeDetector._convert_to_time(series)
        elif 'date' in target_type or 'datetime' in target_type:
            return DataTypeDetector._convert_to_datetime(series)
        elif target_type == 'quoted_number':
            return DataTypeDetector._convert_quoted_numbers(series)
        else:
            # For categorical or string, ensure proper formatting
            return series.astype(str).str.strip()
    
    @staticmethod
    def _convert_quoted_numbers(series: pd.Series) -> pd.Series:
        """Convert quoted numbers like \"475\" to numeric"""
        def clean_quoted(value):
            if pd.isna(value):
                return value
            
            str_val = str(value).strip()
            # Remove quotes
            str_val = re.sub(r'^["\']|["\']$', '', str_val)
            
            # Try to convert to numeric
            try:
                # Check if it's a float
                if '.' in str_val:
                    return float(str_val)
                else:
                    return int(str_val)
            except:
                return value
        
        return series.apply(clean_quoted)
    
    @staticmethod
    def _convert_to_integer(series: pd.Series) -> pd.Series:
        """Convert to integer, handling various formats"""
        def clean_int(value):
            if pd.isna(value):
                return value
            
            str_val = str(value).strip()
            
            # Remove quotes
            str_val = re.sub(r'^["\']|["\']$', '', str_val)
            
            # Remove thousand separators
            str_val = str_val.replace(',', '')
            
            # Remove currency symbols
            str_val = re.sub(r'[$€£¥]', '', str_val)
            
            # Remove whitespace
            str_val = str_val.replace(' ', '')
            
            try:
                return int(float(str_val))  # Handle \"475.0\"
            except:
                # Try to extract number from string
                match = re.search(r'-?\d+', str_val)
                if match:
                    try:
                        return int(match.group())
                    except:
                        pass
                return np.nan
        
        result = series.apply(clean_int)
        return pd.to_numeric(result, errors='coerce')
    
    @staticmethod
    def _convert_to_float(series: pd.Series) -> pd.Series:
        """Convert to float, handling various formats"""
        def clean_float(value):
            if pd.isna(value):
                return value
            
            str_val = str(value).strip()
            
            # Remove quotes
            str_val = re.sub(r'^["\']|["\']$', '', str_val)
            
            # Handle European decimal (1.234,56 -> 1234.56)
            if re.match(r'^\d{1,3}(\.\d{3})*,\d+$', str_val):
                str_val = str_val.replace('.', '').replace(',', '.')
            # Handle US format with commas (1,234.56)
            else:
                str_val = str_val.replace(',', '')
            
            # Remove currency symbols
            str_val = re.sub(r'[$€£¥]', '', str_val)
            
            # Handle percentages
            if '%' in str_val:
                str_val = str_val.replace('%', '')
                try:
                    return float(str_val) / 100
                except:
                    pass
            
            try:
                return float(str_val)
            except:
                # Try scientific notation
                try:
                    return float(str_val.replace(' ', ''))
                except:
                    return np.nan
        
        result = series.apply(clean_float)
        return pd.to_numeric(result, errors='coerce')
    
    @staticmethod
    def _convert_to_currency(series: pd.Series) -> pd.Series:
        """Convert currency strings to float"""
        def clean_currency(value):
            if pd.isna(value):
                return value
            
            str_val = str(value).strip()
            
            # Remove quotes
            str_val = re.sub(r'^["\']|["\']$', '', str_val)
            
            # Remove currency symbols
            str_val = re.sub(r'[$€£¥]', '', str_val)
            
            # Handle European format (1.234,56)
            if re.match(r'^\d{1,3}(\.\d{3})*,\d+$', str_val):
                str_val = str_val.replace('.', '').replace(',', '.')
            # Handle US format (1,234.56)
            else:
                str_val = str_val.replace(',', '')
            
            # Remove whitespace
            str_val = str_val.replace(' ', '')
            
            try:
                return float(str_val)
            except:
                return np.nan
        
        result = series.apply(clean_currency)
        return pd.to_numeric(result, errors='coerce')
    
    @staticmethod
    def _convert_to_percent(series: pd.Series) -> pd.Series:
        """Convert percentages to float (0.25 for 25%)"""
        def clean_percent(value):
            if pd.isna(value):
                return value
            
            str_val = str(value).strip()
            
            # Remove quotes
            str_val = re.sub(r'^["\']|["\']$', '', str_val)
            
            # Remove percent sign
            str_val = str_val.replace('%', '')
            
            # Remove whitespace
            str_val = str_val.replace(' ', '')
            
            try:
                num = float(str_val)
                # If number is > 1, assume it's already in percent form (25 -> 0.25)
                if num > 1:
                    return num / 100
                return num
            except:
                return np.nan
        
        result = series.apply(clean_percent)
        return pd.to_numeric(result, errors='coerce')
    
    @staticmethod
    def _convert_to_boolean(series: pd.Series) -> pd.Series:
        """Convert various boolean representations to True/False"""
        bool_map = {
            'true': True, 'false': False,
            'yes': True, 'no': False,
            't': True, 'f': False,
            'y': True, 'n': False,
            '1': True, '0': False,
            'on': True, 'off': False
        }
        
        def clean_bool(value):
            if pd.isna(value):
                return value
            
            str_val = str(value).strip().lower()
            
            # Remove quotes
            str_val = re.sub(r'^["\']|["\']$', '', str_val)
            
            return bool_map.get(str_val, bool(str_val))
        
        return series.apply(clean_bool)
    
    @staticmethod
    def _convert_to_time(series: pd.Series) -> pd.Series:
        """Convert time strings to datetime.time"""
        def clean_time(value):
            if pd.isna(value):
                return value
            
            str_val = str(value).strip()
            
            # Remove quotes
            str_val = re.sub(r'^["\']|["\']$', '', str_val)
            
            try:
                # Try parsing as time
                if 'am' in str_val.lower() or 'pm' in str_val.lower():
                    return pd.to_datetime(str_val, format='%I:%M %p').time()
                else:
                    return pd.to_datetime(str_val, format='%H:%M').time()
            except:
                return value
        
        return series.apply(clean_time)
    
    @staticmethod
    def _convert_to_datetime(series: pd.Series) -> pd.Series:
        """Convert date strings to datetime"""
        # Common date formats to try
        date_formats = [
            '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S', '%d-%m-%Y %H:%M', '%m/%d/%Y %I:%M %p',
            '%Y%m%d', '%d%m%Y', '%m%d%Y'
        ]
        
        def parse_date(value):
            if pd.isna(value):
                return value
            
            str_val = str(value).strip()
            
            # Remove quotes
            str_val = re.sub(r'^["\']|["\']$', '', str_val)
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(str_val, fmt)
                except:
                    continue
            
            # Try pandas flexible parser
            try:
                return pd.to_datetime(str_val, errors='raise')
            except:
                return np.nan
        
        result = series.apply(parse_date)
        return result
