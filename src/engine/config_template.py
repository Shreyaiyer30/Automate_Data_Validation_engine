"""
Example configuration for the Enterprise Cleaning Engine.
This template demonstrates all available options for the 10 cleaning stages.
"""

DEFAULT_CLEANING_CONFIG = {
    "column_cleanup": {
        "header_case": "Title Case", # "Title Case" or "Snake Case"
        "remove_constant": True
    },
    "duplicates": {
        "remove_duplicate_cols": True,
        "row_strategy": "first", # "first", "last", "drop_all"
        "key_columns": [], # List of columns to check for duplicates
        "timestamp_column": None # Use latest record if provided
    },
    "numeric": {
        "columns": [] # Automatically detects numeric if empty
    },
    "text_categorical": {
        "columns": [],
        "case_normalization": "Title Case", # "Title Case", "upper", "lower", "sentence case", "none"
        "remove_special_chars": False,
        "categorical_mapping": {} # { "ColName": { "M": "Male", "F": "Female" } }
    },
    "boolean": {
        "columns": [] # Columns to standardize to True/False
    },
    "dates": {
        "columns": {} # { "ColName": "%Y-%m-%d" }
    },
    "ranges": {
        "numeric_columns": {}, # { "Col": { "min": 0, "max": 100, "action": "clip" } }
        "date_columns": {},
        "no_future_dates": True
    },
    "missing_values": {
        "numeric_strategy": "mean", # mean, median, mode, zero, custom, nan, drop
        "categorical_strategy": "mode", # mode, N/A, nan, drop
        "date_strategy": "ffill", # ffill, bfill, nan, drop
        "custom_numeric_fill": 0
    },
    "scaling": {
        "method": "none", # minmax, zscore, none
        "columns": []
    }
}
