"""
Utility modules for the Automated Data Validation Engine.
"""

# File loader functions
from .file_loader import (
    EnhancedDataLoader,
    CSVLoader,
    ExcelLoader,
    ParquetLoader,
    JSONLoader,
    FileFormat,
    CompressionFormat,
    FileMetadata,
    ValidationResult,
    FileLoaderError,
    FileTooLargeError,
    InvalidFormatError,
    ValidationError,
    quick_load,
    load_and_validate,
    validate_dataframe
)

# Helper functions
from .helpers import (
    generate_sample_data,
    get_data_summary,
    get_column_stats,
    calculate_missing_stats,
    find_duplicate_rows,
    detect_outliers
)

# Version information
__version__ = "2.0.0"
__author__ = "Data Engineering Team"

# Export all public functions
__all__ = [
    # File loader
    'EnhancedDataLoader',
    'CSVLoader',
    'ExcelLoader',
    'ParquetLoader',
    'JSONLoader',
    'FileFormat',
    'CompressionFormat',
    'FileMetadata',
    'ValidationResult',
    'FileLoaderError',
    'FileTooLargeError',
    'InvalidFormatError',
    'ValidationError',
    'quick_load',
    'load_and_validate',
    'validate_dataframe',
    
    # Helpers
    'generate_sample_data',
    'get_data_summary',
    'calculate_missing_stats',
    'find_duplicate_rows',
    'detect_outliers',
]