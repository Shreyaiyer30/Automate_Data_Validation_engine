"""
Universal Data File Loader - Streamlit Optimized
Handles CSV, Excel, Parquet formats with Streamlit UploadedFile support.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union, Dict, Any, Optional, Tuple, List, BinaryIO
import chardet
import warnings
import io
import json
from datetime import datetime
import logging
import mimetypes

logger = logging.getLogger(__name__)

class FileLoaderError(Exception):
    """Custom exception for file loading errors."""
    pass

def detect_file_type_from_name(filename: str) -> str:
    """Detect file type from extension for Streamlit UploadedFile."""
    filename_lower = filename.lower()
    
    if filename_lower.endswith(('.csv', '.tsv', '.txt')):
        return 'csv'
    elif filename_lower.endswith(('.xlsx', '.xls')):
        return 'excel'
    elif filename_lower.endswith(('.parquet', '.parq')):
        return 'parquet'
    elif filename_lower.endswith('.json'):
        return 'json'
    else:
        # Try to guess from content
        return 'unknown'

def load_csv_streamlit(uploaded_file, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Load CSV file from Streamlit UploadedFile."""
    metadata = {
        'format': 'csv',
        'filename': uploaded_file.name,
        'size': uploaded_file.size,
        'warnings': [],
        'errors': []
    }
    
    try:
        # Reset file pointer
        uploaded_file.seek(0)
        
        # Try to detect encoding
        try:
            sample = uploaded_file.read(10000)
            uploaded_file.seek(0)
            result = chardet.detect(sample)
            encoding = result['encoding']
            metadata['encoding_detected'] = encoding
            metadata['encoding_confidence'] = result['confidence']
        except:
            encoding = 'utf-8'
            metadata['encoding_detected'] = 'unknown'
        
        # Try different reading strategies
        df = None
        strategies = [
            {'engine': 'c', 'low_memory': False, 'encoding': encoding},
            {'engine': 'python', 'encoding': encoding},
            {'engine': 'python', 'encoding': 'latin-1'},
            {'engine': 'python', 'encoding': 'utf-8'},
        ]
        
        for i, strategy in enumerate(strategies):
            try:
                uploaded_file.seek(0)
                strategy.update(kwargs)
                df = pd.read_csv(uploaded_file, **strategy)
                metadata['reading_strategy'] = f"strategy_{i}"
                break
            except Exception as e:
                if i == len(strategies) - 1:
                    raise
                continue
        
        metadata['success'] = True
        metadata['shape'] = df.shape
        metadata['columns'] = list(df.columns)
        metadata['dtypes'] = df.dtypes.astype(str).to_dict()
        
        return df, metadata
        
    except Exception as e:
        metadata['success'] = False
        metadata['errors'].append(str(e))
        raise FileLoaderError(f"Failed to load CSV: {str(e)}")

def load_excel_streamlit(uploaded_file, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Load Excel file from Streamlit UploadedFile."""
    metadata = {
        'format': 'excel',
        'filename': uploaded_file.name,
        'size': uploaded_file.size,
        'warnings': [],
        'errors': []
    }
    
    try:
        uploaded_file.seek(0)
        
        # Determine engine based on file extension
        filename_lower = uploaded_file.name.lower()
        if filename_lower.endswith('.xls'):
            engine = 'xlrd'
        else:
            engine = 'openpyxl'
        
        # Load Excel
        df = pd.read_excel(uploaded_file, engine=engine, **kwargs)
        
        metadata['success'] = True
        metadata['shape'] = df.shape
        metadata['engine'] = engine
        metadata['columns'] = list(df.columns)
        
        return df, metadata
        
    except Exception as e:
        metadata['success'] = False
        metadata['errors'].append(str(e))
        raise FileLoaderError(f"Failed to load Excel: {str(e)}")

def load_parquet_streamlit(uploaded_file, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Load Parquet file from Streamlit UploadedFile."""
    metadata = {
        'format': 'parquet',
        'filename': uploaded_file.name,
        'size': uploaded_file.size,
        'warnings': [],
        'errors': []
    }
    
    try:
        uploaded_file.seek(0)
        
        # Load Parquet
        df = pd.read_parquet(uploaded_file, **kwargs)
        
        metadata['success'] = True
        metadata['shape'] = df.shape
        metadata['columns'] = list(df.columns)
        
        return df, metadata
        
    except Exception as e:
        metadata['success'] = False
        metadata['errors'].append(str(e))
        raise FileLoaderError(f"Failed to load Parquet: {str(e)}")

def load_json_streamlit(uploaded_file, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Load JSON file from Streamlit UploadedFile."""
    metadata = {
        'format': 'json',
        'filename': uploaded_file.name,
        'size': uploaded_file.size,
        'warnings': [],
        'errors': []
    }
    
    try:
        uploaded_file.seek(0)
        
        # Read content
        content = uploaded_file.read()
        if isinstance(content, bytes):
            content = content.decode('utf-8')
        
        # Load JSON
        df = pd.read_json(io.StringIO(content), **kwargs)
        
        metadata['success'] = True
        metadata['shape'] = df.shape
        metadata['columns'] = list(df.columns)
        
        return df, metadata
        
    except Exception as e:
        metadata['success'] = False
        metadata['errors'].append(str(e))
        raise FileLoaderError(f"Failed to load JSON: {str(e)}")

def load_data_streamlit(uploaded_file, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Universal data loader for Streamlit UploadedFile objects.
    
    Args:
        uploaded_file: Streamlit UploadedFile object
        **kwargs: Format-specific parameters
        
    Returns:
        Tuple of (DataFrame, metadata dictionary)
    """
    import time
    start_time = time.time()
    
    metadata = {
        'timestamp': datetime.now().isoformat(),
        'filename': uploaded_file.name,
        'size_bytes': uploaded_file.size,
        'size_mb': round(uploaded_file.size / (1024 * 1024), 2),
        'warnings': [],
        'errors': []
    }
    
    try:
        # Detect file type from extension
        file_type = detect_file_type_from_name(uploaded_file.name)
        metadata['detected_type'] = file_type
        
        # Load based on file type
        if file_type == 'csv':
            df, format_metadata = load_csv_streamlit(uploaded_file, **kwargs)
        elif file_type == 'excel':
            df, format_metadata = load_excel_streamlit(uploaded_file, **kwargs)
        elif file_type == 'parquet':
            df, format_metadata = load_parquet_streamlit(uploaded_file, **kwargs)
        elif file_type == 'json':
            df, format_metadata = load_json_streamlit(uploaded_file, **kwargs)
        else:
            raise FileLoaderError(f"Unsupported file type: {file_type}")
        
        # Merge metadata
        metadata.update(format_metadata)
        
        # Validate DataFrame
        validation = validate_dataframe_basic(df)
        metadata['validation'] = validation
        
        # Calculate load time
        metadata['load_time_seconds'] = round(time.time() - start_time, 2)
        
        logger.info(f"Loaded {uploaded_file.name}: {df.shape} in {metadata['load_time_seconds']}s")
        
        return df, metadata
        
    except Exception as e:
        metadata['errors'].append(str(e))
        metadata['load_time_seconds'] = round(time.time() - start_time, 2)
        logger.error(f"Failed to load {uploaded_file.name}: {str(e)}")
        raise

def validate_dataframe_basic(df: pd.DataFrame) -> Dict[str, Any]:
    """Basic DataFrame validation."""
    validation = {
        'is_valid': True,
        'rows': len(df),
        'columns': len(df.columns),
        'missing_total': int(df.isna().sum().sum()),
        'missing_percentage': round((df.isna().sum().sum() / (len(df) * len(df.columns))) * 100, 2) if len(df) > 0 else 0,
        'duplicates': int(df.duplicated().sum()),
        'duplicate_percentage': round((df.duplicated().sum() / len(df)) * 100, 2) if len(df) > 0 else 0,
        'warnings': [],
        'errors': []
    }
    
    # Check for empty DataFrame
    if df.empty:
        validation['is_valid'] = False
        validation['errors'].append("DataFrame is empty")
    
    # Check for no columns
    if len(df.columns) == 0:
        validation['is_valid'] = False
        validation['errors'].append("DataFrame has no columns")
    
    # Check for duplicate column names
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        validation['warnings'].append(f"Duplicate column names: {duplicate_cols}")
    
    # Check memory usage
    try:
        mem_usage = df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
        validation['memory_mb'] = round(mem_usage, 2)
        if mem_usage > 100:
            validation['warnings'].append(f"Large DataFrame: {mem_usage:.2f} MB")
    except:
        pass
    
    return validation

def load_from_path(file_path: Union[str, Path], **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Load data from file path."""
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileLoaderError(f"File not found: {file_path}")
    
    metadata = {
        'timestamp': datetime.now().isoformat(),
        'file_path': str(file_path),
        'size_bytes': file_path.stat().st_size,
        'size_mb': round(file_path.stat().st_size / (1024 * 1024), 2),
        'warnings': [],
        'errors': []
    }
    
    import time
    start_time = time.time()
    
    try:
        # Determine file type from extension
        file_type = detect_file_type_from_name(file_path.name)
        metadata['detected_type'] = file_type
        
        # Load based on file type
        if file_type == 'csv':
            df = pd.read_csv(file_path, **kwargs)
        elif file_type == 'excel':
            df = pd.read_excel(file_path, **kwargs)
        elif file_type == 'parquet':
            df = pd.read_parquet(file_path, **kwargs)
        elif file_type == 'json':
            df = pd.read_json(file_path, **kwargs)
        else:
            # Try to auto-detect
            try:
                df = pd.read_csv(file_path, **kwargs)
                file_type = 'csv'
            except:
                try:
                    df = pd.read_excel(file_path, **kwargs)
                    file_type = 'excel'
                except:
                    raise FileLoaderError(f"Could not determine file type for: {file_path}")
        
        metadata['format'] = file_type
        metadata['success'] = True
        metadata['shape'] = df.shape
        metadata['columns'] = list(df.columns)
        
        # Validate
        validation = validate_dataframe_basic(df)
        metadata['validation'] = validation
        
        metadata['load_time_seconds'] = round(time.time() - start_time, 2)
        
        return df, metadata
        
    except Exception as e:
        metadata['success'] = False
        metadata['errors'].append(str(e))
        metadata['load_time_seconds'] = round(time.time() - start_time, 2)
        raise FileLoaderError(f"Failed to load {file_path}: {str(e)}")

def load_data(file_input, **kwargs) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Universal loader that handles both Streamlit UploadedFile and file paths.
    
    Args:
        file_input: Streamlit UploadedFile or file path string/Path
        **kwargs: Format-specific parameters
        
    Returns:
        Tuple of (DataFrame, metadata)
    """
    # Check if it's a Streamlit UploadedFile
    if hasattr(file_input, 'read') and hasattr(file_input, 'name'):
        return load_data_streamlit(file_input, **kwargs)
    elif isinstance(file_input, (str, Path)):
        return load_from_path(file_input, **kwargs)
    else:
        raise FileLoaderError(f"Unsupported input type: {type(file_input)}")

def get_data_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """Get comprehensive summary of DataFrame."""
    summary = {
        'basic': {
            'rows': len(df),
            'columns': len(df.columns),
            'total_cells': len(df) * len(df.columns),
            'memory_mb': round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2)
        },
        'missing': {
            'total_missing': int(df.isna().sum().sum()),
            'missing_percentage': round((df.isna().sum().sum() / (len(df) * len(df.columns))) * 100, 2),
            'missing_by_column': df.isna().sum().to_dict(),
            'missing_percentage_by_column': (df.isna().sum() / len(df) * 100).round(2).to_dict()
        },
        'duplicates': {
            'exact_duplicates': int(df.duplicated().sum()),
            'exact_duplicate_percentage': round((df.duplicated().sum() / len(df)) * 100, 2)
        },
        'dtypes': {
            str(dtype): count 
            for dtype, count in df.dtypes.value_counts().items()
        },
        'column_types': {
            'numeric': len(df.select_dtypes(include=[np.number]).columns),
            'text': len(df.select_dtypes(include=['object']).columns),
            'datetime': len(df.select_dtypes(include=['datetime']).columns),
            'boolean': len(df.select_dtypes(include=['bool']).columns),
            'categorical': len(df.select_dtypes(include=['category']).columns)
        }
    }
    
    # Add numeric column statistics
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        summary['numeric_stats'] = {}
        for col in numeric_cols:
            if df[col].notna().sum() > 0:
                summary['numeric_stats'][col] = {
                    'mean': round(float(df[col].mean()), 2),
                    'std': round(float(df[col].std()), 2),
                    'min': round(float(df[col].min()), 2),
                    'max': round(float(df[col].max()), 2),
                    'median': round(float(df[col].median()), 2),
                    'missing': int(df[col].isna().sum()),
                    'missing_percentage': round((df[col].isna().sum() / len(df)) * 100, 2)
                }
    
    return summary

def safe_load(uploaded_file, fallback_to_simple=True, **kwargs):
    """
    Safe loader with fallback to simple pandas functions.
    
    Args:
        uploaded_file: Streamlit UploadedFile
        fallback_to_simple: If True, fall back to simple pandas loading if advanced loader fails
        **kwargs: Additional parameters
        
    Returns:
        Tuple of (DataFrame, metadata)
    """
    try:
        # Try advanced loader first
        return load_data_streamlit(uploaded_file, **kwargs)
    except Exception as e:
        if not fallback_to_simple:
            raise
        
        logger.warning(f"Advanced loader failed, falling back to simple loader: {str(e)}")
        
        # Fall back to simple loading
        uploaded_file.seek(0)
        filename = uploaded_file.name.lower()
        
        if filename.endswith('.csv'):
            df = pd.read_csv(uploaded_file, **kwargs)
        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file, **kwargs)
        elif filename.endswith('.parquet'):
            df = pd.read_parquet(uploaded_file, **kwargs)
        elif filename.endswith('.json'):
            df = pd.read_json(uploaded_file, **kwargs)
        else:
            # Try CSV as last resort
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, **kwargs)
        
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'filename': uploaded_file.name,
            'format': 'simple_fallback',
            'success': True,
            'shape': df.shape,
            'warnings': [f"Used simple loader due to: {str(e)}"],
            'errors': []
        }
        
        return df, metadata

# Convenience function for common use cases
def smart_load(file_input, **kwargs):
    """
    Smart loading with intelligent defaults.
    
    Args:
        file_input: File path or UploadedFile
        **kwargs: Additional arguments
        
    Returns:
        Tuple of (DataFrame, metadata)
    """
    defaults = {
        'fallback_to_simple': True,
    }
    defaults.update(kwargs)
    
    return load_data(file_input, **defaults)