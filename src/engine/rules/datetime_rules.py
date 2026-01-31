import pandas as pd

def validate_date_sequence(series: pd.Series) -> bool:
    """Check if dates are in monotonic increasing order."""
    return series.is_monotonic_increasing

def get_date_range_bounds(series: pd.Series) -> tuple:
    """Get min and max dates."""
    return series.min(), series.max()
