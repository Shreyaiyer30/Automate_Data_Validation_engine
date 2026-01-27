import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import re

class DynamicRuleGenerator:
    """Generate cleaning rules dynamically based on data analysis"""
    
    @staticmethod
    def detect_column_patterns(df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """Detect patterns and issues in a column"""
        patterns = {
            'has_missing': False,
            'has_special_chars': False,
            'has_duplicates': False,
            'has_outliers': False,
            'data_type': str(df[column].dtype),
            'unique_count': 0,
            'sample_values': []
        }
        
        series = df[column]
        
        # Check for missing values
        patterns['has_missing'] = series.isnull().any()
        patterns['missing_count'] = int(series.isnull().sum())
        patterns['missing_percentage'] = float((series.isnull().sum() / len(series)) * 100)
        
        # Check for special characters (for string columns)
        if series.dtype == 'object':
            # Sample values for inspection
            patterns['sample_values'] = series.dropna().head(5).tolist()
            
            # Check for special characters
            special_chars = series.dropna().apply(lambda x: bool(re.search(r'[?@#$%^&*()_+=\[\]{}|;:",.<>?/\\]', str(x))))
            patterns['has_special_chars'] = bool(special_chars.any())
            patterns['special_char_count'] = int(special_chars.sum())
            
            # Check for duplicates
            patterns['has_duplicates'] = bool(series.duplicated().any())
            patterns['duplicate_count'] = int(series.duplicated().sum())
            patterns['unique_count'] = int(series.nunique())
        
        # For numeric columns
        elif pd.api.types.is_numeric_dtype(series):
            # Check for outliers using IQR
            series_no_na = series.dropna()
            if len(series_no_na) > 10:
                Q1 = series_no_na.quantile(0.25)
                Q3 = series_no_na.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = (series_no_na < lower_bound) | (series_no_na > upper_bound)
                patterns['has_outliers'] = bool(outliers.any())
                patterns['outlier_count'] = int(outliers.sum())
            
            # Statistical summary
            patterns['min'] = float(series.min()) if pd.notna(series.min()) else None
            patterns['max'] = float(series.max()) if pd.notna(series.max()) else None
            patterns['mean'] = float(series.mean()) if pd.notna(series.mean()) else None
            patterns['std'] = float(series.std()) if pd.notna(series.std()) else None
        
        return patterns
    
    @staticmethod
    def generate_cleaning_rules(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Generate cleaning rules dynamically for each column"""
        rules = {}
        
        for col in df.columns:
            col_rules = {}
            patterns = DynamicRuleGenerator.detect_column_patterns(df, col)
            
            # String columns
            if df[col].dtype == 'object':
                # Check if it's a date column
                if DynamicRuleGenerator._is_date_column(df[col]):
                    col_rules.update({
                        'convert_to_date': True,
                        'date_format': 'auto_detect',
                        'handle_missing': 'fill_with_none'
                    })
                
                # Check if it's a categorical column
                elif patterns['unique_count'] < 50 and patterns['unique_count'] > 0:
                    col_rules.update({
                        'clean_string': True,
                        'strip_spaces': True,
                        'remove_special_chars': True,
                        'handle_missing': 'fill_with_mode',
                        'convert_to_category': True
                    })
                
                # Text column with many unique values
                else:
                    col_rules.update({
                        'clean_string': True,
                        'strip_spaces': True,
                        'remove_special_chars': patterns['has_special_chars'],
                        'handle_missing': 'fill_with_unknown',
                        'remove_duplicates': patterns['has_duplicates']
                    })
            
            # Numeric columns
            elif pd.api.types.is_numeric_dtype(df[col]):
                col_rules.update({
                    'handle_missing': 'impute_with_median',
                    'detect_outliers': True,
                    'outlier_method': 'iqr',
                    'fix_outliers': 'cap',
                    'normalize': bool(patterns['std'] > (patterns['mean'] * 10)) if patterns.get('std') is not None and patterns.get('mean') is not None else False
                })
                
                # Add range checks for common numeric fields
                col_name_lower = col.lower()
                if any(keyword in col_name_lower for keyword in ['age', 'year', 'duration']):
                    col_rules['range_check'] = DynamicRuleGenerator._get_range_for_column(col)
                elif any(keyword in col_name_lower for keyword in ['score', 'rating', 'percentage']):
                    col_rules['range_check'] = {'min': 0, 'max': 100}
                elif any(keyword in col_name_lower for keyword in ['price', 'cost', 'amount', 'salary']):
                    col_rules['range_check'] = {'min': 0, 'max': None}
            
            # Boolean columns (0/1 or True/False)
            elif df[col].dtype == 'bool' or df[col].nunique() == 2:
                col_rules.update({
                    'convert_to_bool': True,
                    'handle_missing': 'fill_with_mode'
                })
            
            # Apply rules based on detected issues
            if patterns['has_missing']:
                col_rules['missing_threshold'] = patterns['missing_percentage']
            
            if patterns.get('has_outliers', False):
                col_rules['outlier_threshold'] = (patterns['outlier_count'] / len(df[col])) * 100
            
            rules[col] = col_rules
        
        return rules
    
    @staticmethod
    def _is_date_column(series: pd.Series, threshold: float = 0.3) -> bool:
        """Check if column contains dates"""
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
    def _get_range_for_column(column_name: str) -> Dict[str, Optional[float]]:
        """Get reasonable range for common column types"""
        col_lower = column_name.lower()
        
        ranges = {
            'age': {'min': 0, 'max': 120},
            'year': {'min': 1900, 'max': 2100},
            'duration': {'min': 0, 'max': 500},  # minutes
            'month': {'min': 1, 'max': 12},
            'day': {'min': 1, 'max': 31},
            'hour': {'min': 0, 'max': 24},
            'minute': {'min': 0, 'max': 60},
            'second': {'min': 0, 'max': 60},
            'score': {'min': 0, 'max': 100},
            'rating': {'min': 1, 'max': 5},
            'percentage': {'min': 0, 'max': 100},
            'probability': {'min': 0, 'max': 1}
        }
        
        # Find matching range
        for key, value in ranges.items():
            if key in col_lower:
                return value
        
        # Default no range
        return {'min': None, 'max': None}
    
    @staticmethod
    def suggest_column_renaming(df: pd.DataFrame) -> Dict[str, str]:
        """Suggest better column names"""
        name_mapping = {}
        
        common_patterns = {
            r'(\w+)[_\-]?id$': lambda m: f"{m.group(1)}_id",
            r'(\w+)[_\-]?name$': lambda m: f"{m.group(1)}_name",
            r'(\w+)[_\-]?date$': lambda m: f"{m.group(1)}_date",
            r'(\w+)[_\-]?time$': lambda m: f"{m.group(1)}_time",
            r'num[_\-]?(\w+)$': lambda m: f"{m.group(1)}_count",
            r'total[_\-]?(\w+)$': lambda m: f"{m.group(1)}_total",
            r'avg[_\-]?(\w+)$': lambda m: f"{m.group(1)}_average",
            r'pct[_\-]?(\w+)$': lambda m: f"{m.group(1)}_percentage"
        }
        
        for col in df.columns:
            original = col.lower().strip()
            new_name = original
            
            # Replace special characters
            new_name = re.sub(r'[^a-z0-9_]', '_', new_name)
            
            # Remove multiple underscores
            new_name = re.sub(r'_+', '_', new_name)
            
            # Remove leading/trailing underscores
            new_name = new_name.strip('_')
            
            # Apply common patterns
            for pattern, replacer in common_patterns.items():
                match = re.match(pattern, new_name)
                if match:
                    new_name = replacer(match)
                    break
            
            # Capitalize for readability
            new_name = '_'.join(word.capitalize() for word in new_name.split('_'))
            
            if new_name != col:
                name_mapping[col] = new_name
        
        return name_mapping
