"""
Data Type Detection and Conversion Stage
Automatically detects and converts column data types with enhanced error handling
and support for numeric text, dates, and categorical data.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
import re

# Import from your project structure
from src.engine.stages.base_stage import BaseStage, StageState
from src.engine.pipeline_stage import PipelineStage  # or BaseStage if used
from src.engine.state import PipelineState

print("detect_types.py loaded OK")
print("pd:", pd)
print("Dict:", Dict, "Any:", Any, "Tuple:", Tuple)
print("BaseStage:", BaseStage)
print("StageState:", StageState)

class DetectTypesStage(BaseStage):
    """
    Pipeline stage to automatically detect, infer, and cast column data types.
    
    Features:
    - Smart numeric text detection (with currency symbols, commas, etc.)
    - Date/time parsing with multiple format support
    - Boolean value detection
    - Categorical data identification
    - Confidence-based conversion with fallbacks
    - Comprehensive mutation logging
    """
    
    @property
    def name(self) -> str:
        return "TYPE_DETECTION"
    
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, StageState]:
        """
        Detect and convert column data types using intelligent inference.
        
        Args:
            df: Input DataFrame
            config: Configuration dictionary with optional settings:
                - min_numeric_confidence: Minimum confidence for numeric conversion (default: 0.1)
                - date_formats: List of date formats to try (default: auto-detect)
                - preserve_original: Keep original columns alongside converted ones (default: False)
                - handle_currency: Attempt to detect and convert currency values (default: True)
        
        Returns:
            Tuple of (processed DataFrame, stage state)
        """
        # Create a working copy
        df = df.copy()
        state = StageState.PASS
        
        # Configuration with defaults
        min_numeric_confidence = config.get('min_numeric_confidence', 0.1)
        preserve_original = config.get('preserve_original', False)
        handle_currency = config.get('handle_currency', True)
        date_formats = config.get('date_formats', None)
        
        # Track conversions for reporting
        conversions = {
            'numeric': [],
            'datetime': [],
            'boolean': [],
            'categorical': []
        }
        
        for col in df.columns:
            original_dtype = str(df[col].dtype)
            
            # Skip if already numeric (except object type that might be numeric strings)
            if pd.api.types.is_numeric_dtype(df[col]) and df[col].dtype != 'object':
                continue
            
            # 1. Attempt numeric conversion (including currency, percentages, etc.)
            if self._should_attempt_numeric_conversion(df[col], original_dtype):
                numeric_result = self._convert_to_numeric(
                    df[col], 
                    min_confidence=min_numeric_confidence,
                    handle_currency=handle_currency
                )
                
                if numeric_result['converted']:
                    if preserve_original:
                        df[f"{col}_original"] = df[col].copy()
                    
                    df[col] = numeric_result['series']
                    conversions['numeric'].append(col)
                    state = StageState.WARN if state != StageState.FAIL else state
                    
                    self.logger.log_mutation(
                        self.name,
                        "numeric_conversion",
                        {
                            "column": col,
                            "original_type": original_dtype,
                            "new_type": str(df[col].dtype),
                            "confidence": numeric_result['confidence'],
                            "note": "Textual numerics converted (including symbols/currency)",
                            "sample_values": df[col].head(3).tolist() if len(df) > 0 else []
                        }
                    )
                    continue
            
            # 2. Attempt datetime conversion
            if self._should_attempt_datetime_conversion(df[col], original_dtype):
                datetime_result = self._convert_to_datetime(
                    df[col],
                    date_formats=date_formats
                )
                
                if datetime_result['converted']:
                    if preserve_original:
                        df[f"{col}_original"] = df[col].copy()
                    
                    df[col] = datetime_result['series']
                    conversions['datetime'].append(col)
                    state = StageState.WARN if state != StageState.FAIL else state
                    
                    # Add formatted date columns for display/export
                    try:
                        df[f"{col}_short"] = df[col].dt.strftime('%d-%m-%Y')
                        df[f"{col}_long"] = df[col].dt.strftime('%d %B %Y')
                        df[f"{col}_iso"] = df[col].dt.strftime('%Y-%m-%d')
                        
                        self.logger.log_mutation(
                            self.name,
                            "datetime_conversion",
                            {
                                "column": col,
                                "original_type": original_dtype,
                                "new_type": "datetime64[ns]",
                                "detected_format": datetime_result.get('detected_format', 'auto'),
                                "confidence": datetime_result['confidence'],
                                "note": "Date values detected and converted with formatted versions",
                                "formats_added": [f"{col}_short", f"{col}_long", f"{col}_iso"]
                            }
                        )
                    except Exception as e:
                        self.logger.log_warning(
                            self.name,
                            f"Failed to add date formats for column {col}: {str(e)}"
                        )
                    continue
            
            # 3. Attempt boolean conversion
            if self._should_attempt_boolean_conversion(df[col]):
                boolean_result = self._convert_to_boolean(df[col])
                
                if boolean_result['converted']:
                    if preserve_original:
                        df[f"{col}_original"] = df[col].copy()
                    
                    df[col] = boolean_result['series']
                    conversions['boolean'].append(col)
                    state = StageState.WARN if state != StageState.FAIL else state
                    
                    self.logger.log_mutation(
                        self.name,
                        "boolean_conversion",
                        {
                            "column": col,
                            "original_type": original_dtype,
                            "new_type": "bool",
                            "confidence": boolean_result['confidence'],
                            "mapping": boolean_result.get('mapping', {}),
                            "note": "Boolean-like values detected and converted"
                        }
                    )
                    continue
            
            # 4. Check for categorical data
            if self._should_convert_to_categorical(df[col]):
                unique_ratio = df[col].nunique() / len(df[col]) if len(df[col]) > 0 else 1
                if unique_ratio < 0.5:  # Less than 50% unique values
                    df[col] = pd.Categorical(df[col])
                    conversions['categorical'].append(col)
                    
                    self.logger.log_mutation(
                        self.name,
                        "categorical_conversion",
                        {
                            "column": col,
                            "original_type": original_dtype,
                            "new_type": "category",
                            "unique_count": df[col].nunique(),
                            "unique_ratio": unique_ratio,
                            "note": "Converted to categorical for memory efficiency"
                        }
                    )
        
        # Final inference pass for any remaining columns
        df = df.infer_objects()
        
        # Log summary
        if any(conversions.values()):
            self.logger.log_info(
                self.name,
                f"Type detection completed: "
                f"Numeric: {len(conversions['numeric'])}, "
                f"Datetime: {len(conversions['datetime'])}, "
                f"Boolean: {len(conversions['boolean'])}, "
                f"Categorical: {len(conversions['categorical'])}"
            )
        
        return df, state
    
    # ============================================================================
    # HELPER METHODS
    # ============================================================================
    
    def _should_attempt_numeric_conversion(self, series: pd.Series, dtype: str) -> bool:
        """Determine if a series should be attempted for numeric conversion."""
        if dtype != 'object':
            return False
        
        # Check if series appears to contain numeric-like strings
        sample_size = min(100, len(series))
        if sample_size == 0:
            return False
        
        sample = series.head(sample_size).dropna().astype(str)
        if len(sample) == 0:
            return False
        
        # Count numeric-like patterns
        numeric_patterns = [
            r'^\s*[-+]?\d*\.?\d+\s*$',  # Basic numbers
            r'^\s*[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*$',  # Numbers with commas
            r'^\s*[$£€¥₹]\s*[-+]?\d*\.?\d+\s*$',  # Currency
            r'^\s*[-+]?\d*\.?\d+\s*%$',  # Percentages
            r'^\s*[-+]?\d*\.?\d+[eE][+-]?\d+\s*$',  # Scientific notation
        ]
        
        numeric_count = 0
        for val in sample:
            val_str = str(val).strip()
            for pattern in numeric_patterns:
                if re.match(pattern, val_str):
                    numeric_count += 1
                    break
        
        return numeric_count / len(sample) >= 0.1  # At least 10% appear numeric
    
    def _convert_to_numeric(self, series: pd.Series, min_confidence: float = 0.1, 
                           handle_currency: bool = True) -> Dict[str, Any]:
        """
        Convert a series to numeric type with confidence scoring.
        
        Returns:
            Dictionary with keys: converted (bool), series (pd.Series), confidence (float)
        """
        result = {
            'converted': False,
            'series': series,
            'confidence': 0.0
        }
        
        try:
            # Create sanitized version
            sanitized = series.astype(str).str.strip()
            
            if handle_currency:
                # Remove currency symbols and other non-numeric characters (keep decimal, minus, plus, e/E)
                sanitized = sanitized.str.replace(r'[^\d\.\-+eE,]', '', regex=True)
                # Replace commas used as thousand separators (but not decimal points)
                sanitized = sanitized.str.replace(r'(?<=\d),(?=\d{3})', '', regex=True)
            
            # Convert to numeric
            numeric_series = pd.to_numeric(sanitized, errors='coerce')
            
            # Calculate confidence
            valid_count = numeric_series.notna().sum()
            total_count = len(series)
            confidence = valid_count / total_count if total_count > 0 else 0
            
            if confidence >= min_confidence and valid_count > 0:
                result['converted'] = True
                result['series'] = numeric_series
                result['confidence'] = confidence
                
                # For very high confidence, try to infer more specific numeric type
                if confidence > 0.9:
                    if (numeric_series % 1 == 0).all():
                        # All integers
                        max_val = numeric_series.max()
                        if pd.notna(max_val):
                            if max_val < 128:
                                result['series'] = numeric_series.astype('int8')
                            elif max_val < 32768:
                                result['series'] = numeric_series.astype('int16')
                            elif max_val < 2147483648:
                                result['series'] = numeric_series.astype('int32')
                            else:
                                result['series'] = numeric_series.astype('int64')
        
        except Exception as e:
            self.logger.log_debug(self.name, f"Numeric conversion failed: {str(e)}")
        
        return result
    
    def _should_attempt_datetime_conversion(self, series: pd.Series, dtype: str) -> bool:
        """Determine if a series should be attempted for datetime conversion."""
        if dtype not in ['object', 'string']:
            return False
        
        # Quick check: does the series contain date-like strings?
        sample_size = min(50, len(series))
        if sample_size == 0:
            return False
        
        sample = series.head(sample_size).dropna().astype(str).str.strip()
        if len(sample) == 0:
            return False
        
        # Date patterns to check
        date_patterns = [
            r'\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}',  # DD/MM/YYYY or similar
            r'\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}',    # YYYY/MM/DD
            r'\d{1,2}\s+[A-Za-z]{3,}\s+\d{4}',       # DD Month YYYY
            r'[A-Za-z]{3,}\s+\d{1,2},\s+\d{4}',      # Month DD, YYYY
        ]
        
        date_like_count = 0
        for val in sample:
            for pattern in date_patterns:
                if re.search(pattern, val):
                    date_like_count += 1
                    break
        
        return date_like_count / len(sample) >= 0.3  # At least 30% appear date-like
    
    def _convert_to_datetime(self, series: pd.Series, date_formats: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Convert a series to datetime with format detection.
        
        Returns:
            Dictionary with keys: converted (bool), series (pd.Series), confidence (float), detected_format (str)
        """
        result = {
            'converted': False,
            'series': series,
            'confidence': 0.0,
            'detected_format': None
        }
        
        try:
            if date_formats:
                # Try specified formats
                for fmt in date_formats:
                    try:
                        dt_series = pd.to_datetime(series, format=fmt, errors='coerce')
                        valid_count = dt_series.notna().sum()
                        if valid_count > 0:
                            confidence = valid_count / len(series)
                            if confidence > result['confidence']:
                                result['converted'] = True
                                result['series'] = dt_series
                                result['confidence'] = confidence
                                result['detected_format'] = fmt
                    except:
                        continue
            
            # Fallback to pandas auto-detection
            dt_series = pd.to_datetime(series, errors='coerce', infer_datetime_format=True)
            valid_count = dt_series.notna().sum()
            confidence = valid_count / len(series) if len(series) > 0 else 0
            
            if confidence > result['confidence']:
                result['converted'] = True
                result['series'] = dt_series
                result['confidence'] = confidence
                result['detected_format'] = 'auto'
        
        except Exception as e:
            self.logger.log_debug(self.name, f"Datetime conversion failed: {str(e)}")
        
        return result
    
    def _should_attempt_boolean_conversion(self, series: pd.Series) -> bool:
        """Check if series contains boolean-like values."""
        if series.dtype == 'bool':
            return False
        
        sample_size = min(100, len(series))
        if sample_size == 0:
            return False
        
        sample = series.head(sample_size).dropna().astype(str).str.strip().str.lower()
        
        # Common boolean representations
        boolean_values = {
            'true', 'false', 'yes', 'no', 'y', 'n', '1', '0', 
            't', 'f', 'on', 'off', 'enabled', 'disabled'
        }
        
        boolean_like = sum(1 for val in sample if val in boolean_values)
        return boolean_like / len(sample) >= 0.8  # At least 80% boolean-like
    
    def _convert_to_boolean(self, series: pd.Series) -> Dict[str, Any]:
        """Convert a series to boolean type."""
        result = {
            'converted': False,
            'series': series,
            'confidence': 0.0,
            'mapping': {}
        }
        
        try:
            # Create mapping for common boolean representations
            mapping = {
                'true': True, 'false': False,
                'yes': True, 'no': False,
                'y': True, 'n': False,
                '1': True, '0': False,
                't': True, 'f': False,
                'on': True, 'off': False,
                'enabled': True, 'disabled': False,
                True: True, False: False
            }
            
            # Convert
            str_series = series.astype(str).str.strip().str.lower()
            bool_series = str_series.map(mapping)
            
            # Calculate confidence
            valid_count = bool_series.notna().sum()
            confidence = valid_count / len(series) if len(series) > 0 else 0
            
            if confidence >= 0.8:  # High confidence threshold for booleans
                result['converted'] = True
                result['series'] = bool_series
                result['confidence'] = confidence
                result['mapping'] = {k: v for k, v in mapping.items() if k in str_series.unique()}
        
        except Exception as e:
            self.logger.log_debug(self.name, f"Boolean conversion failed: {str(e)}")
        
        return result
    
    def _should_convert_to_categorical(self, series: pd.Series) -> bool:
        """Determine if a series should be converted to categorical."""
        if pd.api.types.is_categorical_dtype(series):
            return False
        
        # Check cardinality
        unique_count = series.nunique()
        total_count = len(series)
        
        if total_count == 0:
            return False
        
        # Convert if low cardinality and not already numeric/datetime
        unique_ratio = unique_count / total_count
        is_low_cardinality = unique_ratio < 0.5
        is_object_type = series.dtype == 'object' or pd.api.types.is_string_dtype(series)
        
        return is_low_cardinality and is_object_type and unique_count <= 100


# Optional: Factory function for easy instantiation
def create_type_detection_stage(config: Optional[Dict[str, Any]] = None) -> DetectTypesStage:
    """
    Factory function to create a type detection stage with optional configuration.
    
    Args:
        config: Optional configuration dictionary
    
    Returns:
        Configured DetectTypesStage instance
    """
    stage = DetectTypesStage()
    if config:
        # Apply any stage-specific configuration here if needed
        pass
    return stage