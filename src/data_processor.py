import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any, Optional
import io
from .config import load_config, get_column_rules, get_settings
from .validation_rules import validate_dataframe
from .outlier_detector import detect_outliers_zscore
from .data_corrector import correct_data, clean_dates
from .report_generator import setup_logger, generate_quality_report, log_audit
from .date_formatter import DateFormatter
from .data_type_detector import DataTypeDetector

class DataProcessor:
    def __init__(self, settings: Dict = None, rules: Dict = None):
        self.settings = settings or {}
        self.rules = rules or {}
        # Add logger initialization if needed
        self.logger = None  # Initialize logger
    
    def _auto_type_conversion(self, df: pd.DataFrame) -> pd.DataFrame:
        """Automatically detect and convert data types."""
        df_fixed = df.copy()
        conf_threshold = self.settings.get('type_detection_confidence', 0.8)
        
        for col in df_fixed.columns:
            try:
                # Skip if already numeric/datetime
                if pd.api.types.is_numeric_dtype(df_fixed[col]) or pd.api.types.is_datetime64_any_dtype(df_fixed[col]):
                    continue
                
                detection = DataTypeDetector.detect_column_type(df_fixed[col])
                detected_type = detection['type']
                confidence = detection['confidence']
                
                if confidence >= conf_threshold:
                    if 'integer' in detected_type and self.settings.get('convert_quoted_numbers', True):
                        df_fixed[col] = DataTypeDetector.convert_column(df_fixed[col], 'integer')
                    elif ('float' in detected_type or 'currency' in detected_type) and self.settings.get('convert_currency', True):
                        df_fixed[col] = DataTypeDetector.convert_column(df_fixed[col], 'float')
                    elif 'percent' in detected_type and self.settings.get('convert_percentages', True):
                        df_fixed[col] = DataTypeDetector.convert_column(df_fixed[col], 'percent')
                    elif 'boolean' in detected_type and self.settings.get('convert_booleans', True):
                        df_fixed[col] = DataTypeDetector.convert_column(df_fixed[col], 'boolean')
                    elif ('date' in detected_type or 'time' in detected_type) and self.settings.get('convert_dates', True):
                        # Use our existing _format_dates_column later
                        pass
                elif self.settings.get('convert_quoted_numbers', True):
                    # Fallback for quoted numbers if confidence low but matches quoted pattern
                    df_fixed[col] = DataTypeDetector.convert_column(df_fixed[col], 'quoted_number')
                    
            except Exception as e:
                # Make sure log_audit function is defined or imported
                log_audit(self.logger, "TYPE_CONVERSION_ERROR", {"column": col, "error": str(e)})
                
        return df_fixed

    def detect_data_types_report(self, df: pd.DataFrame) -> Dict:
        """Generate report of detected data types for the UI."""
        type_report = {}
        for col in df.columns:
            detection = DataTypeDetector.detect_column_type(df[col])
            type_report[col] = {
                'current_dtype': str(df[col].dtype),
                'detected_type': detection['type'],
                'confidence': float(detection['confidence']),
                'sample_values': df[col].dropna().head(5).tolist(),
                'missing_count': int(df[col].isna().sum()),
                'unique_count': int(df[col].nunique())
            }
        return type_report

    def _format_dates_column(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Format date columns using multiple parsing strategies."""
        df_processed = df.copy()
        target_format = self.settings.get('date_format', '%d-%m-%Y')
        date_log = {}
        
        cols_to_check = [c for c in df.columns if df[c].dtype == 'object' or pd.api.types.is_datetime64_any_dtype(df[c])] if self.settings.get('detect_dates', True) else [col for col, r in self.rules.items() if r.get('dtype') == 'datetime64[ns]']

        for col in cols_to_check:
            if col not in df.columns: 
                continue
            if not DateFormatter.is_date_column(df[col], threshold=0.15): 
                continue
            try:
                d_us = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y')
                d_eu = pd.to_datetime(df[col], errors='coerce', format='%d/%m/%Y')
                d_mx = pd.to_datetime(df[col], errors='coerce', dayfirst=False)
                opts = [
                    (d_us.notna().sum(), d_us, "MM/DD/YYYY"), 
                    (d_eu.notna().sum(), d_eu, "DD/MM/YYYY"), 
                    (d_mx.notna().sum(), d_mx, "Auto-detect")
                ]
                best_v, best_d, best_l = max(opts, key=lambda x: x[0])
                if best_v > 0:
                    df_processed[col] = best_d.dt.strftime(target_format) if target_format != 'datetime' else best_d
                    date_log[col] = {"detected": best_l, "count": int(best_v), "applied": target_format}
            except Exception as e:
                log_audit(self.logger, "DATE_FORMAT_ERROR", {"column": col, "error": str(e)})
        return df_processed, date_log

    def detect_date_columns(self, df: pd.DataFrame) -> List[str]:
        return [col for col in df.columns if DateFormatter.is_date_column(df[col], threshold=0.3)]
    
    def analyze_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze data quality and return statistics for the UI.
        This method is called by the dynamic rules editor.
        """
        quality_report = {
            'summary': {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'total_missing': df.isna().sum().sum(),
                'total_duplicates': df.duplicated().sum(),
                'memory_usage': df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
            },
            'columns': {},
            'suggestions': []
        }
        
        # Analyze each column
        for col in df.columns:
            col_stats = {
                'dtype': str(df[col].dtype),
                'missing_count': int(df[col].isna().sum()),
                'missing_percentage': float(df[col].isna().mean() * 100),
                'unique_count': int(df[col].nunique()),
                'unique_percentage': float(df[col].nunique() / len(df) * 100),
                'sample_values': df[col].dropna().head(5).tolist()
            }
            
            # Add basic statistics for numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_stats.update({
                    'min': float(df[col].min()) if not df[col].isna().all() else None,
                    'max': float(df[col].max()) if not df[col].isna().all() else None,
                    'mean': float(df[col].mean()) if not df[col].isna().all() else None,
                    'median': float(df[col].median()) if not df[col].isna().all() else None,
                    'std': float(df[col].std()) if not df[col].isna().all() else None
                })
            
            quality_report['columns'][col] = col_stats
            
            # Generate suggestions based on data quality issues
            if col_stats['missing_percentage'] > 20:
                quality_report['suggestions'].append({
                    'type': 'missing_data',
                    'column': col,
                    'message': f"High missing values ({col_stats['missing_percentage']:.1f}%) in column '{col}'",
                    'severity': 'high'
                })
            
            if col_stats['unique_percentage'] > 95:
                quality_report['suggestions'].append({
                    'type': 'high_cardinality',
                    'column': col,
                    'message': f"High cardinality ({col_stats['unique_count']} unique values) in column '{col}'",
                    'severity': 'medium'
                })
                
            if col_stats['unique_count'] == 1:
                quality_report['suggestions'].append({
                    'type': 'constant_column',
                    'column': col,
                    'message': f"Constant value in column '{col}'",
                    'severity': 'low'
                })
        
        # Check for duplicate rows
        if quality_report['summary']['total_duplicates'] > 0:
            dup_percentage = quality_report['summary']['total_duplicates'] / len(df) * 100
            quality_report['suggestions'].append({
                'type': 'duplicates',
                'column': None,
                'message': f"Found {quality_report['summary']['total_duplicates']} duplicate rows ({dup_percentage:.1f}%)",
                'severity': 'medium' if dup_percentage < 10 else 'high'
            })
        
        # Add type detection to the report
        type_report = self.detect_data_types_report(df)
        quality_report['type_detection'] = type_report
        
        return quality_report
    def robust_read_file(self, uploaded_file) -> pd.DataFrame:
        """
        Robustly read uploaded file with multiple format support.
        
        Args:
            uploaded_file: Streamlit uploaded file object
            
        Returns:
            pandas DataFrame
            
        Raises:
            ValueError: If file format is not supported
        """
        import io
        
        # Get file extension
        file_name = uploaded_file.name.lower()
        
        try:
            # Reset file pointer to beginning
            uploaded_file.seek(0)
            
            if file_name.endswith('.csv'):
                # Try different encodings for CSV
                encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
                for encoding in encodings:
                    try:
                        uploaded_file.seek(0)
                        df = pd.read_csv(
                            io.BytesIO(uploaded_file.read()),
                            encoding=encoding,
                            on_bad_lines='skip',
                            low_memory=False
                        )
                        return df
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        if encoding == encodings[-1]:
                            raise e
                        
            elif file_name.endswith('.xlsx') or file_name.endswith('.xls'):
                uploaded_file.seek(0)
                # Read Excel file
                df = pd.read_excel(
                    io.BytesIO(uploaded_file.read()),
                    sheet_name=0,
                    engine='openpyxl' if file_name.endswith('.xlsx') else 'xlrd'
                )
                return df
                
            elif file_name.endswith('.json'):
                uploaded_file.seek(0)
                df = pd.read_json(io.BytesIO(uploaded_file.read()))
                return df
                
            elif file_name.endswith('.parquet'):
                uploaded_file.seek(0)
                df = pd.read_parquet(io.BytesIO(uploaded_file.read()))
                return df
                
            elif file_name.endswith('.txt') or file_name.endswith('.tsv'):
                uploaded_file.seek(0)
                # Try tab-separated for .tsv, space-separated for .txt
                separator = '\t' if file_name.endswith('.tsv') else None
                df = pd.read_csv(
                    io.BytesIO(uploaded_file.read()),
                    sep=separator,
                    encoding='utf-8',
                    on_bad_lines='skip'
                )
                return df
                
            else:
                raise ValueError(f"Unsupported file format: {file_name}")
                
        except Exception as e:
            # Log the error
            if hasattr(self, 'logger') and self.logger:
                log_audit(self.logger, "FILE_READ_ERROR", {
                    "file_name": uploaded_file.name,
                    "error": str(e)
                })
            
            # Try one last attempt with default settings
            try:
                uploaded_file.seek(0)
                if file_name.endswith('.csv') or file_name.endswith('.txt') or file_name.endswith('.tsv'):
                    df = pd.read_csv(
                        io.BytesIO(uploaded_file.read()),
                        encoding='utf-8',
                        error_bad_lines=False,
                        warn_bad_lines=False
                    )
                    return df
                else:
                    raise e
            except Exception:
                raise ValueError(f"Failed to read file {uploaded_file.name}: {str(e)}")

    def process_data(self, uploaded_file) -> Tuple[pd.DataFrame, Dict]:
        """
        Main processing pipeline: read, clean, and generate report for uploaded file.
    
        Args:
            uploaded_file: Streamlit uploaded file object
        
        Returns:
            Tuple of (cleaned_dataframe, report_dict)
        """
        try:
            # 1. Read the file
            df = self.robust_read_file(uploaded_file)
            
            # 2. Store raw data for comparison
            df_raw = df.copy()
            
            # 3. Apply automatic type conversion
            df = self._auto_type_conversion(df)
            
            # 4. Format dates if detected
            if self.settings.get('detect_dates', True):
                df, date_log = self._format_dates_column(df)
            else:
                date_log = {}
            
            # 5. Apply data correction rules
            if self.rules:
                df = self._apply_rules(df)
            
            # 6. Detect and handle outliers if enabled
            if self.settings.get('detect_outliers', False):
                outlier_report = self._handle_outliers(df)
            else:
                outlier_report = {}

            # 7. Generate quality report
            quality_report = self._generate_processing_report(df_raw, df, date_log, outlier_report)
            
            return df, quality_report
        
        except Exception as e:
            # Log error
            if hasattr(self, 'logger') and self.logger:
                log_audit(self.logger, "PROCESS_DATA_ERROR", {
                    "file_name": uploaded_file.name if uploaded_file else "unknown",
                    "error": str(e)
                })
            
            # Re-raise the exception for the UI to handle
            raise

    def _apply_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply cleaning rules to the dataframe.
    
        Args:
            df: Input dataframe
        
        Returns:
            Cleaned dataframe
        """
        df_processed = df.copy()
    
        for col, rules in self.rules.items():
            if col not in df_processed.columns:
                continue
            
            # Apply rule-based transformations
            if rules.get('handle_missing'):
                df_processed[col] = self._handle_missing_values(df_processed[col], rules['handle_missing'])
            
            # Apply other rule-based transformations
            if rules.get('remove_outliers', False):
                df_processed[col] = self._remove_outliers(df_processed[col])
            
            if rules.get('standardize_case', False) and pd.api.types.is_string_dtype(df_processed[col]):
                df_processed[col] = df_processed[col].str.lower()
            
            if rules.get('trim_whitespace', False) and pd.api.types.is_string_dtype(df_processed[col]):
                df_processed[col] = df_processed[col].str.strip()
            
            if rules.get('remove_special_chars', False) and pd.api.types.is_string_dtype(df_processed[col]):
                import re
                df_processed[col] = df_processed[col].apply(lambda x: re.sub(r'[^a-zA-Z0-9\s.,-]', '', str(x)) if pd.notna(x) else x)
        
        return df_processed
    
    def _handle_missing_values(self, series: pd.Series, method: str) -> pd.Series:
        """
        Handle missing values based on specified method.
    
        Args:
            series: Input pandas Series
            method: Method to handle missing values
        
        Returns:
            Series with handled missing values
        """
        if method == 'do_nothing':
            return series
        elif method == 'fill_with_mode':
            return series.fillna(series.mode().iloc[0] if not series.mode().empty else series.mean())
        elif method == 'fill_with_unknown':
            return series.fillna('Unknown')
        elif method == 'impute_with_mean':
            return series.fillna(series.mean())
        elif method == 'impute_with_median':
            return series.fillna(series.median())
        elif method == 'forward_fill':
            return series.ffill()
        elif method == 'backward_fill':
            return series.bfill()
        else:
            return series
    
    def _remove_outliers(self, series: pd.Series) -> pd.Series:
        """
        Remove outliers using IQR method.
    
        Args:
            series: Input pandas Series
        
        Returns:
            Series with outliers removed (set to NaN)
        """
        if not pd.api.types.is_numeric_dtype(series):
            return series
        
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
    
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
    
        # Replace outliers with NaN
        series_clean = series.copy()
        series_clean[(series_clean < lower_bound) | (series_clean > upper_bound)] = np.nan
    
        return series_clean
    
    def _handle_outliers(self, df: pd.DataFrame) -> Dict:
        """
        Detect and handle outliers in numeric columns.
    
        Args:
            df: Input dataframe
        
        Returns:
            Dictionary with outlier report
        """
        outlier_report = {}
    
        numeric_cols = df.select_dtypes(include=[np.number]).columns
    
        for col in numeric_cols:
            # Use IQR method to detect outliers
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)][col]
        
        if not outliers.empty:
            outlier_report[col] = {
                'count': int(len(outliers)),
                'percentage': float(len(outliers) / len(df) * 100),
                'min_outlier': float(outliers.min()),
                'max_outlier': float(outliers.max())
            }
    
        return outlier_report
    
    def _generate_processing_report(self, df_raw: pd.DataFrame, df_cleaned: pd.DataFrame, 
                               date_log: Dict, outlier_report: Dict) -> Dict:
        """
        Generate comprehensive processing report.

        Args:
            df_raw: Original dataframe
            df_cleaned: Cleaned dataframe
            date_log: Date formatting log
            outlier_report: Outlier detection report

        Returns:
            Dictionary with processing report
        """
        report = {
            'summary': {
                'total_rows_before': int(len(df_raw)),
                'total_rows_after': int(len(df_cleaned)),
                'total_columns_before': int(len(df_raw.columns)),
                'total_columns_after': int(len(df_cleaned.columns)),
                'total_missing_before': int(df_raw.isna().sum().sum()),
                'total_missing_after': int(df_cleaned.isna().sum().sum()),
                'total_outliers_detected': sum(info.get('count', 0) for info in outlier_report.values()),
                'date_columns_formatted': len(date_log),
                'memory_usage_before': float(df_raw.memory_usage(deep=True).sum() / 1024 / 1024),  # MB
                'memory_usage_after': float(df_cleaned.memory_usage(deep=True).sum() / 1024 / 1024),  # MB
            },
            'date_formatting': date_log,
            'outliers': outlier_report,
            'columns': {},
            'quality_score': 0
        }
    
        # Calculate quality score
        quality_score = 100
    
        # Deduct for missing values
        total_cells = len(df_raw) * len(df_raw.columns)
        if total_cells > 0:
            missing_pct_before = report['summary']['total_missing_before'] / total_cells * 100
            if missing_pct_before > 50:
                quality_score -= 30
            elif missing_pct_before > 25:
                quality_score -= 20
            elif missing_pct_before > 10:
                quality_score -= 10
    
        # Add for missing value reduction
        if report['summary']['total_missing_before'] > 0 and report['summary']['total_missing_after'] < report['summary']['total_missing_before']:
            reduction = (report['summary']['total_missing_before'] - report['summary']['total_missing_after']) / report['summary']['total_missing_before'] * 100
            quality_score += min(20, reduction * 0.2)
    
        # Deduct for outliers
        if len(df_raw) > 0:
            outlier_pct = report['summary']['total_outliers_detected'] / len(df_raw) * 100
            if outlier_pct > 10:
                quality_score -= 15
            elif outlier_pct > 5:
                quality_score -= 10
            elif outlier_pct > 1:
                quality_score -= 5
    
        report['summary']['quality_score'] = max(0, min(100, quality_score))
        report['quality_score'] = report['summary']['quality_score']
    
        # Add column-wise statistics
        for col in df_cleaned.columns:
            col_info = {
                'dtype': str(df_cleaned[col].dtype),
                'missing_before': int(df_raw[col].isna().sum()),
                'missing_after': int(df_cleaned[col].isna().sum()),
                'unique_before': int(df_raw[col].nunique()),
                'unique_after': int(df_cleaned[col].nunique()),
        }
        
        # Only add min/max for numeric columns
        # if pd.api.types.is_numeric_dtype(df_cleaned[col]):
        #     # Safely calculate min/max with error handling
        #     try:
        #         if not df_raw[col].isna().all():
        #             col_info['min_before'] = float(df_raw[col].min())
        #             col_info['max_before'] = float(df_raw[col].max())
        #         if not df_cleaned[col].isna().all():
        #             col_info['min_after'] = float(df_cleaned[col].min())
        #             col_info['max_after'] = float(df_cleaned[col].max())
        #     except (TypeError, ValueError):
        #         # If conversion fails, skip these stats
        #         pass
        # Safely compute numeric statistics
        raw_numeric = pd.to_numeric(df_raw[col], errors='coerce')
        clean_numeric = pd.to_numeric(df_cleaned[col], errors='coerce')

        if raw_numeric.notna().any():
            col_info['min_before'] = float(raw_numeric.min())
            col_info['max_before'] = float(raw_numeric.max())

        if clean_numeric.notna().any():
            col_info['min_after'] = float(clean_numeric.min())
            col_info['max_after'] = float(clean_numeric.max())

        report['columns'][col] = col_info
    
        return report