import pandas as pd
import numpy as np

def calculate_outlier_bounds(series: pd.Series, method: str = 'iqr') -> tuple:
    """Calculate lower and upper bounds for outliers."""
    if method == 'iqr':
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        return q1 - 1.5 * iqr, q3 + 1.5 * iqr
    else: # z-score
        mean = series.mean()
        std = series.std()
        return mean - 3 * std, mean + 3 * std

def validate_numeric_range(series: pd.Series, min_val: float = None, max_val: float = None) -> pd.Series:
    """Identify values outside valid range."""
    mask = pd.Series(False, index=series.index)
    if min_val is not None:
        mask |= (series < min_val)
    if max_val is not None:
        mask |= (series > max_val)
    return mask
