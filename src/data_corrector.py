import pandas as pd
import numpy as np
from .outlier_detector import get_outlier_indices

def correct_data(df, rules, settings):
    """
    Apply automated corrections to the DataFrame.
    Returns cleaned DataFrame and a summary of corrections.
    """
    df_cleaned = df.copy()
    summary = {
        "missing_filled": 0,
        "duplicates_removed": 0,
        "outliers_capped": 0
    }

    # 1. Remove Duplicates
    if settings.get('remove_duplicates', True):
        initial_rows = len(df_cleaned)
        df_cleaned = df_cleaned.drop_duplicates()
        summary["duplicates_removed"] = initial_rows - len(df_cleaned)

    # 2. Fix Missing Values
    strategy = settings.get('missing_value_strategy', 'auto')
    
    for col in df_cleaned.columns:
        if df_cleaned[col].isnull().any():
            col_rules = rules.get(col, {})
            dtype = col_rules.get('dtype', str(df_cleaned[col].dtype))
            
            fill_val = None
            if strategy == 'auto':
                if 'int' in dtype or 'float' in dtype:
                    fill_val = df_cleaned[col].mean()
                else:
                    fill_val = df_cleaned[col].mode()[0] if not df_cleaned[col].mode().empty else None
            elif strategy == 'mean':
                fill_val = df_cleaned[col].mean()
            elif strategy == 'mode':
                fill_val = df_cleaned[col].mode()[0] if not df_cleaned[col].mode().empty else None
            
            if fill_val is not None:
                count = df_cleaned[col].isnull().sum()
                df_cleaned[col] = df_cleaned[col].fillna(fill_val)
                summary["missing_filled"] += int(count)

    # 3. Cap Outliers
    threshold = settings.get('outlier_zscore_threshold', 3.0)
    numeric_cols = df_cleaned.select_dtypes(include=[np.number]).columns
    
    for col in numeric_cols:
        outlier_indices = get_outlier_indices(df_cleaned[col], threshold)
        if not outlier_indices.empty:
            count = len(outlier_indices)
            # Cap values at threshold limits
            mean = df_cleaned[col].mean()
            std = df_cleaned[col].std()
            
            upper_limit = mean + threshold * std
            lower_limit = mean - threshold * std
            
            df_cleaned.loc[outlier_indices, col] = df_cleaned.loc[outlier_indices, col].apply(
                lambda x: upper_limit if x > upper_limit else lower_limit if x < lower_limit else x
            )
            summary["outliers_capped"] += int(count)

    return df_cleaned, summary
