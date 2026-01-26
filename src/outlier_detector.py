import pandas as pd
import numpy as np
from scipy import stats

def detect_outliers_zscore(df, threshold=3.0):
    """
    Detect outliers in numerical columns using Z-score.
    Returns a dictionary with outlier counts per column.
    """
    outliers_report = {}
    
    # Select only numeric columns
    numeric_df = df.select_dtypes(include=[np.number])
    
    for col in numeric_df.columns:
        # Drop NaNs for Z-score calculation
        data = numeric_df[col].dropna()
        if data.empty:
            continue
            
        z_scores = np.abs(stats.zscore(data))
        outlier_mask = z_scores > threshold
        count = int(outlier_mask.sum())
        
        if count > 0:
            outliers_report[col] = count
            
    return outliers_report

def get_outlier_indices(series, threshold=3.0):
    """
    Get the indices of outliers in a series.
    """
    if not np.issubdtype(series.dtype, np.number):
        return pd.Index([])
        
    data = series.dropna()
    if data.empty:
        return pd.Index([])
        
    z_scores = np.abs(stats.zscore(data))
    return data.index[z_scores > threshold]
