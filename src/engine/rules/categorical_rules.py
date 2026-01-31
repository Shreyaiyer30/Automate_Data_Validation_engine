import pandas as pd

def detect_inconsistent_categories(series: pd.Series, threshold: float = 0.8) -> dict:
    """Detect potential typos in categorical data using string similarity (placeholder)."""
    # In a full implementation, we'd use Levenshtein distance
    # For now, we look for case-insensitive matches that differ in capitalization
    counts = series.str.lower().value_counts()
    issues = {}
    return issues

def validate_enum_values(series: pd.Series, allowed_values: list) -> pd.Series:
    """Identify values not in the allowed list."""
    return ~series.isin(allowed_values)
