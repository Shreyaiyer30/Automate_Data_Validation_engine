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
    def __init__(self, config_path=None):
        self.config = load_config(config_path)
        self.rules = get_column_rules(self.config)
        self.settings = get_settings(self.config)
        
        # Add default data type settings
        type_defaults = {
            'fix_dtypes': True,
            'auto_detect_types': True,
            'convert_quoted_numbers': True,
            'convert_currency': True,
            'convert_percentages': True,
            'convert_dates': True,
            'convert_times': True,
            'convert_booleans': True,
            'confidence_threshold': 0.6
        }
        for k, v in type_defaults.items():
            if k not in self.settings:
                self.settings[k] = v
                
        # Add default date settings
        date_defaults = {
            'format_dates': True,
            'date_format': '%d-%m-%Y',
            'detect_dates': True
        }
        for k, v in date_defaults.items():
            if k not in self.settings:
                self.settings[k] = v
                
        self.logger = setup_logger()

    @staticmethod
    def robust_read_file(input_data):
        """Robustly read CSV or Excel files with encoding detection."""
        filename = getattr(input_data, 'name', '').lower() if hasattr(input_data, 'name') else str(input_data).lower()
        
        if filename.endswith(('.xlsx', '.xls')):
            try:
                return pd.read_excel(input_data)
            except Exception as e:
                raise ValueError(f"Failed to read Excel file: {e}")

        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'utf-16', 'utf-8-sig', 'cp1252']
        is_file_like = hasattr(input_data, 'seek')
        
        for enc in encodings:
            try:
                if is_file_like: input_data.seek(0)
                return pd.read_csv(input_data, encoding=enc)
            except (UnicodeDecodeError, pd.errors.ParserError, pd.errors.EmptyDataError):
                continue
        
        if is_file_like: input_data.seek(0)
        try:
            return pd.read_csv(input_data, encoding='utf-8', sep=None, engine='python')
        except:
            raise ValueError("Could not decode or parse the file.")

    def process_data(self, input_data):
        """Orchestrate the validation and cleaning process."""
        df = self.robust_read_file(input_data)
        log_audit(self.logger, "DATA_LOADED", {"rows": len(df), "cols": len(df.columns)})

        # 1. Validation
        validation_report = validate_dataframe(df, self.rules)
        
        # 2. Smart Type Fixing (NEW)
        if self.settings.get('fix_dtypes', True):
            df = self._fix_data_types(df)

        # 3. Detect Outliers
        threshold = self.settings.get('outlier_zscore_threshold', 3.0)
        outliers_report = detect_outliers_zscore(df, threshold)

        # 4. Correct Data (Basic + Structural + Text)
        df_cleaned, correction_summary = correct_data(df, self.rules, self.settings)
        
        # 5. Advanced Date Formatting
        if self.settings.get('format_dates', True):
            df_cleaned, date_log = self._format_dates_column(df_cleaned)
            correction_summary["date_corrections"] = date_log
        
        # 6. Profiling (Statistical context for UI) - Do after cleaning
        numeric_df = df_cleaned.select_dtypes(include=[np.number])
        profiling_report = {
            "columns": df_cleaned.columns.tolist(),
            "dtypes": df_cleaned.dtypes.astype(str).to_dict(),
            "shape": [int(x) for x in df_cleaned.shape],
            "statistics": {}
        }
        
        if not numeric_df.empty:
            stats = numeric_df.agg(['mean', 'median', 'std']).fillna(0).to_dict()
            for col, metrics in stats.items():
                mode_vals = numeric_df[col].mode()
                mode_val = mode_vals[0] if not mode_vals.empty else 0
                metrics['mode'] = float(mode_val) if isinstance(mode_val, (int, float, np.number)) else str(mode_val)
                profiling_report["statistics"][col] = {k: float(v) if isinstance(v, (int, float, np.number)) else v for k, v in metrics.items()}

        log_audit(self.logger, "DATA_CLEANED", correction_summary)

        # 7. Generate Report
        quality_report = generate_quality_report(validation_report, correction_summary, outliers_report)
        quality_report["profiling"] = profiling_report

        return df_cleaned, quality_report

    def _fix_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Intelligently fix data types with smart detection."""
        df_fixed = df.copy()
        conf_threshold = self.settings.get('confidence_threshold', 0.7)
        
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
            if col not in df.columns: continue
            if not DateFormatter.is_date_column(df[col], threshold=0.15): continue
            try:
                d_us = pd.to_datetime(df[col], errors='coerce', format='%m/%d/%Y'); d_eu = pd.to_datetime(df[col], errors='coerce', format='%d/%m/%Y'); d_mx = pd.to_datetime(df[col], errors='coerce', dayfirst=False)
                opts = [(d_us.notna().sum(), d_us, "MM/DD/YYYY"), (d_eu.notna().sum(), d_eu, "DD/MM/YYYY"), (d_mx.notna().sum(), d_mx, "Auto-detect")]
                best_v, best_d, best_l = max(opts, key=lambda x: x[0])
                if best_v > 0:
                    df_processed[col] = best_d.dt.strftime(target_format) if target_format != 'datetime' else best_d
                    date_log[col] = {"detected": best_l, "count": int(best_v), "applied": target_format}
            except Exception as e:
                log_audit(self.logger, "DATE_FORMAT_ERROR", {"column": col, "error": str(e)})
        return df_processed, date_log

    def detect_date_columns(self, df: pd.DataFrame) -> List[str]:
        return [col for col in df.columns if DateFormatter.is_date_column(df[col], threshold=0.3)]