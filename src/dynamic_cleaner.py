import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import re
from scipy import stats
from .rule_generator import DynamicRuleGenerator

class DynamicDataCleaner:
    """Apply dynamic cleaning rules to datasets"""
    
    def __init__(self, rule_generator=None):
        self.rule_generator = rule_generator or DynamicRuleGenerator()
        self.applied_rules = {}
        self.cleaning_report = {}
    
    def clean_dataframe(self, df: pd.DataFrame, custom_rules: Optional[Dict] = None) -> pd.DataFrame:
        """Clean dataframe using dynamic or custom rules"""
        df_cleaned = df.copy()
        self.cleaning_report = {
            'columns_cleaned': [],
            'transformations_applied': {},
            'issues_found': {},
            'summary': {}
        }
        
        # Generate or use custom rules
        if custom_rules:
            rules = custom_rules
        else:
            rules = self.rule_generator.generate_cleaning_rules(df)
        
        self.applied_rules = rules
        
        # 0. Global cleaning: Remove Duplicates
        # This is a critical fix requested by the user
        if any(col_rules.get('remove_duplicates', False) for col_rules in rules.values()) or True: # Default to true for now as per original logic
            initial_rows = len(df_cleaned)
            df_cleaned = df_cleaned.drop_duplicates()
            self.cleaning_report['summary']['duplicates_removed'] = initial_rows - len(df_cleaned)
        
        # Apply rules to each column
        for col, col_rules in rules.items():
            if col not in df_cleaned.columns:
                continue
            
            original_dtype = str(df_cleaned[col].dtype)
            transformations = []
            issues = []
            
            try:
                # Apply string cleaning rules
                if col_rules.get('clean_string', False):
                    df_cleaned[col] = self._clean_string_column(df_cleaned[col], col_rules)
                    transformations.append('string_cleaned')
                
                # Handle missing values
                if 'handle_missing' in col_rules:
                    method = col_rules['handle_missing']
                    if method != 'do_nothing':
                        missing_count = int(df_cleaned[col].isnull().sum())
                        if missing_count > 0:
                            df_cleaned[col] = self._handle_missing(df_cleaned[col], method)
                            transformations.append(f'missing_handled({missing_count})')
                
                # Convert data types
                if col_rules.get('convert_to_date', False):
                    df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors='coerce')
                    transformations.append('converted_to_date')
                
                elif col_rules.get('convert_to_bool', False):
                    df_cleaned[col] = self._convert_to_bool(df_cleaned[col])
                    transformations.append('converted_to_bool')
                
                elif col_rules.get('convert_to_category', False):
                    df_cleaned[col] = df_cleaned[col].astype('category')
                    transformations.append('converted_to_category')
                
                # Apply range checks for numeric columns
                if 'range_check' in col_rules and pd.api.types.is_numeric_dtype(df_cleaned[col]):
                    range_issues = self._check_range(df_cleaned[col], col_rules['range_check'])
                    if range_issues > 0:
                        issues.append(f'range_violations({range_issues})')
                
                # Handle outliers
                if col_rules.get('detect_outliers', False) and pd.api.types.is_numeric_dtype(df_cleaned[col]):
                    outlier_count = self._detect_outliers(df_cleaned[col], col_rules.get('outlier_method', 'iqr'))
                    if outlier_count > 0:
                        if col_rules.get('fix_outliers') == 'cap':
                            df_cleaned[col] = self._fix_outliers(df_cleaned[col], col_rules.get('outlier_method', 'iqr'))
                            transformations.append(f'outliers_fixed({outlier_count})')
                        else:
                            issues.append(f'outliers_detected({outlier_count})')
                
                # Normalize if needed
                if col_rules.get('normalize', False) and pd.api.types.is_numeric_dtype(df_cleaned[col]):
                    df_cleaned[col] = self._normalize_column(df_cleaned[col])
                    transformations.append('normalized')
                
                # Record if transformations were applied
                if transformations:
                    self.cleaning_report['columns_cleaned'].append(col)
                    self.cleaning_report['transformations_applied'][col] = transformations
                
                if issues:
                    self.cleaning_report['issues_found'][col] = issues
                
                # Record dtype change
                new_dtype = str(df_cleaned[col].dtype)
                if original_dtype != new_dtype:
                    transformations.append(f'dtype_changed({original_dtype}→{new_dtype})')
                    
            except Exception as e:
                self.cleaning_report['issues_found'][col] = [f'cleaning_error: {str(e)}']
                continue
        
        # Generate summary
        self._generate_summary(df, df_cleaned)
        
        return df_cleaned
    
    def _clean_string_column(self, series: pd.Series, rules: Dict) -> pd.Series:
        """Clean string column based on rules"""
        cleaned = series.copy().astype(str)
        
        # Strip whitespace
        if rules.get('strip_spaces', True):
            cleaned = cleaned.str.strip()
        
        # Remove special characters
        if rules.get('remove_special_chars', False):
            cleaned = cleaned.str.replace(r'[?@#$%^&*()_+=\[\]{}|;:",.<>?/\\]', '', regex=True)
        
        # Remove extra spaces
        cleaned = cleaned.str.replace(r'\s+', ' ', regex=True)
        
        return cleaned
    
    def _handle_missing(self, series: pd.Series, method: str) -> pd.Series:
        """Handle missing values using specified method"""
        if method == 'impute_with_mean' and pd.api.types.is_numeric_dtype(series):
            return series.fillna(series.mean())
        elif method == 'impute_with_median' and pd.api.types.is_numeric_dtype(series):
            return series.fillna(series.median())
        elif method == 'fill_with_mode':
            mode_val = series.mode()
            return series.fillna(mode_val[0] if not mode_val.empty else 'Unknown')
        elif method == 'fill_with_unknown':
            return series.fillna('Unknown')
        elif method == 'fill_with_none':
            return series.fillna(pd.NaT if series.dtype == 'datetime64[ns]' else None)
        elif method == 'forward_fill':
            return series.ffill()
        elif method == 'backward_fill':
            return series.bfill()
        else:
            return series.fillna(series.median() if pd.api.types.is_numeric_dtype(series) else 'Unknown')
    
    def _convert_to_bool(self, series: pd.Series) -> pd.Series:
        """Convert column to boolean type"""
        # Common boolean patterns
        bool_map = {
            'true': True, 'false': False,
            'yes': True, 'no': False,
            '1': True, '0': False,
            't': True, 'f': False,
            'y': True, 'n': False,
            'True': True, 'False': False
        }
        
        cleaned = series.copy().astype(str).str.lower().str.strip()
        return cleaned.map(bool_map).fillna(series)
    
    def _check_range(self, series: pd.Series, range_check: Dict) -> int:
        """Check for values outside specified range"""
        violations = 0
        
        if 'min' in range_check and range_check['min'] is not None:
            violations += int((series < range_check['min']).sum())
        
        if 'max' in range_check and range_check['max'] is not None:
            violations += int((series > range_check['max']).sum())
        
        return violations
    
    def _detect_outliers(self, series: pd.Series, method: str = 'iqr') -> int:
        """Detect outliers in numeric column"""
        series_no_na = series.dropna()
        
        if len(series_no_na) < 10:
            return 0
        
        if method == 'iqr':
            Q1 = series_no_na.quantile(0.25)
            Q3 = series_no_na.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            return int(((series_no_na < lower_bound) | (series_no_na > upper_bound)).sum())
        
        elif method == 'zscore':
            z_scores = np.abs(stats.zscore(series_no_na))
            return int((z_scores > 3).sum())
        
        return 0
    
    def _fix_outliers(self, series: pd.Series, method: str = 'iqr') -> pd.Series:
        """Fix outliers by capping"""
        series_no_na = series.dropna()
        
        if len(series_no_na) < 10:
            return series
        
        if method == 'iqr':
            Q1 = series_no_na.quantile(0.25)
            Q3 = series_no_na.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            return series.clip(lower_bound, upper_bound)
        
        return series
    
    def _normalize_column(self, series: pd.Series) -> pd.Series:
        """Normalize numeric column (0-1 scaling)"""
        if series.min() == series.max():
            return series
        
        return (series - series.min()) / (series.max() - series.min())
    
    def _generate_summary(self, df_before: pd.DataFrame, df_after: pd.DataFrame):
        """Generate cleaning summary report"""
        summary = self.cleaning_report['summary']
        summary.update({
            'original_shape': [int(x) for x in df_before.shape],
            'cleaned_shape': [int(x) for x in df_after.shape],
            'columns_cleaned_count': len(self.cleaning_report['columns_cleaned']),
            'missing_values_before': int(df_before.isnull().sum().sum()),
            'missing_values_after': int(df_after.isnull().sum().sum()),
            'duplicates_before': int(df_before.duplicated().sum()),
            'duplicates_after': int(df_after.duplicated().sum()),
            'data_type_changes': self._count_dtype_changes(df_before, df_after)
        })
    
    def _count_dtype_changes(self, df_before: pd.DataFrame, df_after: pd.DataFrame) -> int:
        """Count number of columns with changed data types"""
        changes = 0
        for col in df_before.columns:
            if col in df_after.columns:
                if str(df_before[col].dtype) != str(df_after[col].dtype):
                    changes += 1
        return changes
    
    def get_cleaning_report(self) -> Dict:
        """Get detailed cleaning report"""
        return self.cleaning_report
    
    def get_applied_rules(self) -> Dict:
        """Get rules that were applied"""
        return self.applied_rules
