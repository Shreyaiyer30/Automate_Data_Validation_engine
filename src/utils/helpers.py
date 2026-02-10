"""
Helper functions for data processing.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union, Tuple
import random
import string
from datetime import datetime, timedelta
import json
import hashlib
import logging
from typing import Union
from pathlib import Path

logger = logging.getLogger(__name__)

def generate_sample_data(rows: int = 100, 
                        columns: Optional[List[str]] = None,
                        include_problems: bool = True) -> pd.DataFrame:
    """
    Generate sample data for testing.
    
    Args:
        rows: Number of rows to generate
        columns: List of column names (or None for default)
        include_problems: Include data quality issues for testing
        
    Returns:
        Sample DataFrame
    """
    if columns is None:
        columns = [
            'id', 'name', 'age', 'salary', 'department', 
            'join_date', 'is_active', 'rating', 'city', 'email'
        ]
    
    np.random.seed(42)
    random.seed(42)
    
    data = {}
    
    # Generate columns with various data types
    if 'id' in columns:
        data['id'] = range(1, rows + 1)
    
    if 'name' in columns:
        first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis']
        data['name'] = [
            f"{random.choice(first_names)} {random.choice(last_names)}" 
            for _ in range(rows)
        ]
    
    if 'age' in columns:
        # Include some outliers and missing values
        ages = np.random.normal(35, 10, rows).astype(int)
        ages = np.clip(ages, 0, 100)
        
        if include_problems:
            # Add some outliers
            ages[0] = 150  # Too high
            ages[1] = -5   # Negative
            
            # Add some missing values
            for i in range(5):
                ages[random.randint(2, rows-1)] = np.nan
        
        data['age'] = ages
    
    if 'salary' in columns:
        salaries = np.random.lognormal(10, 0.5, rows).astype(int)
        
        if include_problems:
            # Add some extreme values
            salaries[2] = 10000000  # Too high
            salaries[3] = 0         # Zero
            
            # Add some missing values
            for i in range(3):
                salaries[random.randint(4, rows-1)] = np.nan
        
        data['salary'] = salaries
    
    if 'department' in columns:
        departments = ['IT', 'HR', 'Finance', 'Marketing', 'Sales', 'Operations']
        data['department'] = [random.choice(departments) for _ in range(rows)]
        
        if include_problems:
            # Add some invalid values
            data['department'][5] = 'Unknown'
            data['department'][6] = ''  # Empty string
            data['department'][7] = None  # None
    
    if 'join_date' in columns:
        start_date = datetime(2010, 1, 1)
        dates = [
            start_date + timedelta(days=random.randint(0, 3650)) 
            for _ in range(rows)
        ]
        
        if include_problems:
            # Add some invalid dates
            dates[8] = '2020-13-01'  # Invalid month
            dates[9] = ''  # Empty string
            dates[10] = None  # None
        
        data['join_date'] = dates
    
    if 'is_active' in columns:
        data['is_active'] = [random.choice([True, False]) for _ in range(rows)]
        
        if include_problems:
            # Add some invalid boolean-like values
            data['is_active'][11] = 'Yes'
            data['is_active'][12] = 1
            data['is_active'][13] = None
    
    if 'rating' in columns:
        ratings = np.random.choice([1, 2, 3, 4, 5], rows, p=[0.1, 0.15, 0.5, 0.15, 0.1])
        
        if include_problems:
            # Add some out-of-range values
            ratings[14] = 6
            ratings[15] = 0
            ratings[16] = np.nan
        
        data['rating'] = ratings
    
    if 'city' in columns:
        cities = ['New York', 'London', 'Tokyo', 'Paris', 'Berlin', 'Sydney', 'Toronto']
        data['city'] = [random.choice(cities) for _ in range(rows)]
    
    if 'email' in columns:
        domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'company.com']
        data['email'] = [
            f"user{i}@{random.choice(domains)}" 
            for i in range(rows)
        ]
        
        if include_problems:
            # Add some invalid emails
            data['email'][17] = 'invalid-email'
            data['email'][18] = '@nodomain.com'
            data['email'][19] = ''
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Ensure all requested columns are present
    for col in columns:
        if col not in df.columns:
            # Add placeholder column
            df[col] = [None] * rows
    
    # Add duplicate rows if requested
    if include_problems and rows > 10:
        # Duplicate first few rows
        duplicate_indices = random.sample(range(rows), min(5, rows // 10))
        for idx in duplicate_indices:
            df = pd.concat([df, df.iloc[[idx]]], ignore_index=True)
    
    return df

def get_data_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get comprehensive summary of DataFrame.
    
    Args:
        df: DataFrame to summarize
        
    Returns:
        Dictionary with summary statistics
    """
    summary = {
        'basic': {
            'rows': df.shape[0],
            'columns': df.shape[1],
            'total_cells': df.shape[0] * df.shape[1],
            'memory_mb': round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2)
        },
        'missing': {
            'total_missing': int(df.isna().sum().sum()),
            'missing_pct': round((df.isna().sum().sum() / (df.shape[0] * df.shape[1])) * 100, 2),
            'by_column': df.isna().sum().to_dict()
        },
        'data_types': {
            str(dtype): count 
            for dtype, count in df.dtypes.value_counts().items()
        },
        'numeric_stats': {},
        'categorical_stats': {}
    }
    
    # Numeric column statistics
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].notna().sum() > 0:
            summary['numeric_stats'][col] = {
                'mean': round(float(df[col].mean()), 2),
                'std': round(float(df[col].std()), 2),
                'min': round(float(df[col].min()), 2),
                'max': round(float(df[col].max()), 2),
                'missing': int(df[col].isna().sum()),
                'missing_pct': round((df[col].isna().sum() / len(df)) * 100, 2)
            }
    
    # Categorical column statistics
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    for col in categorical_cols:
        if df[col].notna().sum() > 0:
            value_counts = df[col].value_counts()
            summary['categorical_stats'][col] = {
                'unique_values': int(df[col].nunique()),
                'top_value': value_counts.index[0] if len(value_counts) > 0 else None,
                'top_count': int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                'missing': int(df[col].isna().sum()),
                'missing_pct': round((df[col].isna().sum() / len(df)) * 100, 2)
            }
    
    # Duplicate statistics
    summary['duplicates'] = {
        'exact_duplicates': int(df.duplicated().sum()),
        'exact_duplicate_pct': round((df.duplicated().sum() / len(df)) * 100, 2) if len(df) > 0 else 0
    }
    
    return summary

def get_column_stats(series: pd.Series) -> Dict[str, Any]:
    """Calculate detailed statistics for a single column."""
    stats = {
        'dtype': str(series.dtype),
        'null_count': int(series.isna().sum()),
        'null_pct': round((series.isna().sum() / len(series) * 100), 2) if len(series) > 0 else 0,
        'nunique': int(series.nunique())
    }
    
    if pd.api.types.is_numeric_dtype(series):
        non_null = series.dropna()
        if not non_null.empty:
            stats.update({
                'min': float(non_null.min()),
                'max': float(non_null.max()),
                'mean': float(non_null.mean()),
                'median': float(non_null.median()),
                'std': float(non_null.std())
            })
            
            # Outliers count
            q1 = non_null.quantile(0.25)
            q3 = non_null.quantile(0.75)
            iqr = q3 - q1
            outliers = non_null[(non_null < (q1 - 1.5 * iqr)) | (non_null > (q3 + 1.5 * iqr))]
            stats['outlier_count'] = int(len(outliers))
        else:
            stats.update({'min': 0, 'max': 0, 'mean': 0, 'outlier_count': 0})
            
    return stats

def calculate_missing_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate detailed missing value statistics.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary with missing value statistics
    """
    missing_by_col = df.isna().sum()
    total_rows = len(df)
    
    stats = {
        'overall': {
            'total_missing': int(missing_by_col.sum()),
            'missing_pct': round((missing_by_col.sum() / (total_rows * len(df.columns))) * 100, 2)
        },
        'by_column': {},
        'columns_to_drop': [],
        'columns_to_impute': [],
        'critical_columns': []
    }
    
    for col in df.columns:
        missing_count = int(missing_by_col[col])
        missing_pct = round((missing_count / total_rows) * 100, 2) if total_rows > 0 else 0
        
        stats['by_column'][col] = {
            'missing_count': missing_count,
            'missing_pct': missing_pct,
            'dtype': str(df[col].dtype),
            'suggestion': ''
        }
        
        # Suggest actions based on missing percentage
        if missing_pct > 50:
            stats['columns_to_drop'].append(col)
            stats['by_column'][col]['suggestion'] = 'Drop column (high missing)'
        elif missing_pct > 20:
            stats['columns_to_impute'].append(col)
            stats['by_column'][col]['suggestion'] = 'Consider imputation'
        elif missing_pct > 0:
            stats['by_column'][col]['suggestion'] = 'Check for patterns'
        else:
            stats['by_column'][col]['suggestion'] = 'Complete'
    
    # Find rows with high missing percentage
    missing_by_row = df.isna().sum(axis=1)
    rows_with_missing = (missing_by_row > 0).sum()
    rows_high_missing = (missing_by_row / len(df.columns) > 0.5).sum()
    
    stats['rows'] = {
        'with_missing': int(rows_with_missing),
        'with_missing_pct': round((rows_with_missing / total_rows) * 100, 2) if total_rows > 0 else 0,
        'high_missing': int(rows_high_missing),
        'high_missing_pct': round((rows_high_missing / total_rows) * 100, 2) if total_rows > 0 else 0
    }
    
    return stats

def find_duplicate_rows(df: pd.DataFrame, 
                       subset: Optional[List[str]] = None,
                       keep: str = 'first') -> Dict[str, Any]:
    """
    Find duplicate rows in DataFrame.
    
    Args:
        df: DataFrame to check
        subset: Columns to consider for duplicates
        keep: 'first', 'last', or False
        
    Returns:
        Dictionary with duplicate information
    """
    if subset is None:
        subset = df.columns.tolist()
    
    # Find duplicates
    duplicates = df.duplicated(subset=subset, keep=keep)
    duplicate_count = duplicates.sum()
    
    result = {
        'total_duplicates': int(duplicate_count),
        'duplicate_pct': round((duplicate_count / len(df)) * 100, 2) if len(df) > 0 else 0,
        'duplicate_indices': df[duplicates].index.tolist() if keep != False else [],
        'unique_rows': len(df) - duplicate_count,
        'subset_used': subset
    }
    
    # If keep=False, we need to identify all duplicates
    if keep == False:
        all_duplicates = df.duplicated(subset=subset, keep=False)
        duplicate_groups = {}
        
        for idx in df[all_duplicates].index:
            # Create hash of the row values for grouping
            row_tuple = tuple(str(val) for val in df.loc[idx, subset])
            row_hash = hashlib.md5(str(row_tuple).encode()).hexdigest()[:8]
            
            if row_hash not in duplicate_groups:
                duplicate_groups[row_hash] = {
                    'count': 0,
                    'indices': [],
                    'sample_row': df.loc[idx, subset].to_dict()
                }
            
            duplicate_groups[row_hash]['count'] += 1
            duplicate_groups[row_hash]['indices'].append(idx)
        
        result['duplicate_groups'] = duplicate_groups
        result['unique_groups'] = len(duplicate_groups)
    
    return result

def detect_outliers(df: pd.DataFrame,
                   method: str = 'iqr',
                   threshold: float = 1.5) -> Dict[str, Any]:
    """
    Detect outliers in numeric columns.
    
    Args:
        df: DataFrame to analyze
        method: 'iqr' or 'zscore'
        threshold: Threshold for outlier detection
        
    Returns:
        Dictionary with outlier information
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    results = {
        'method': method,
        'threshold': threshold,
        'by_column': {},
        'summary': {
            'columns_with_outliers': 0,
            'total_outliers': 0
        }
    }
    
    for col in numeric_cols:
        if df[col].notna().sum() < 2:
            continue  # Need at least 2 values for outlier detection
        
        col_data = df[col].dropna()
        
        if method == 'iqr':
            # IQR method
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
            outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
            
            results['by_column'][col] = {
                'method': 'iqr',
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound),
                'outlier_count': int(outliers),
                'outlier_pct': round((outliers / len(df)) * 100, 2),
                'min_value': float(col_data.min()),
                'max_value': float(col_data.max()),
                'q1': float(Q1),
                'q3': float(Q3),
                'outlier_indices': df[df[col].notna() & ((df[col] < lower_bound) | (df[col] > upper_bound))].index.tolist()
            }
        
        elif method == 'zscore':
            # Z-score method
            mean = col_data.mean()
            std = col_data.std()
            
            if std == 0:
                continue  # No variation
            
            z_scores = (df[col] - mean) / std
            outliers = (abs(z_scores) > threshold).sum()
            
            results['by_column'][col] = {
                'method': 'zscore',
                'mean': float(mean),
                'std': float(std),
                'zscore_threshold': threshold,
                'outlier_count': int(outliers),
                'outlier_pct': round((outliers / len(df)) * 100, 2),
                'min_zscore': float(z_scores.min()),
                'max_zscore': float(z_scores.max()),
                'outlier_indices': df[abs(z_scores) > threshold].index.tolist()
            }
        
        if outliers > 0:
            results['summary']['columns_with_outliers'] += 1
            results['summary']['total_outliers'] += outliers
    
    return results

def convert_to_json_serializable(obj: Any) -> Any:
    """
    Convert object to JSON serializable format.
    
    Args:
        obj: Object to convert
        
    Returns:
        JSON serializable object
    """
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    elif pd.isna(obj):
        return None
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, (set, tuple)):
        return list(obj)
    else:
        return obj

def save_summary_to_file(summary: Dict[str, Any], 
                        file_path: Union[str, Path]) -> None:
    """
    Save summary dictionary to JSON file.
    
    Args:
        summary: Summary dictionary
        file_path: Path to save JSON file
    """
    # Convert to JSON serializable format
    serializable_summary = {}
    for key, value in summary.items():
        if isinstance(value, dict):
            serializable_summary[key] = {
                k: convert_to_json_serializable(v) 
                for k, v in value.items()
            }
        else:
            serializable_summary[key] = convert_to_json_serializable(value)
    
    # Save to file
    with open(file_path, 'w') as f:
        json.dump(serializable_summary, f, indent=2, default=str)
    
    logger.info(f"Summary saved to {file_path}")