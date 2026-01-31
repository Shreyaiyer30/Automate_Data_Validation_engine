"""
Utility modules for the Automated Data Validation Engine.
"""

# File loader functions
from .file_loader import (
    load_data,
    smart_load,
    safe_load,
    load_data_streamlit,
    load_from_path,
    FileLoaderError,
    get_data_summary,
    validate_dataframe_basic
)

# Helper functions
from .helpers import (
    generate_sample_data,
    calculate_missing_stats,
    find_duplicate_rows,
    detect_outliers
)

# Version information
__version__ = "1.0.0"
__author__ = "Automated Data Validation Engine"

# Export all public functions
__all__ = [
    # File loader
    'load_data',
    'smart_load',
    'safe_load',
    'load_data_streamlit',
    'load_from_path',
    'FileLoaderError',
    'get_data_summary',
    'validate_dataframe_basic',
    
    # Helpers
    'generate_sample_data',
    'calculate_missing_stats',
    'find_duplicate_rows',
    'detect_outliers',
]