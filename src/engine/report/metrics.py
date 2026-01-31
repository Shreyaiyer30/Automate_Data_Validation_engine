"""
Quality Metrics Calculation
Advanced metrics for data quality assessment.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

def calculate_quality_metrics(df: pd.DataFrame, 
                             reference_df: Optional[pd.DataFrame] = None,
                             config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Calculate comprehensive quality metrics.
    
    Args:
        df: DataFrame to analyze
        reference_df: Reference DataFrame for comparison
        config: Configuration for metric calculations
        
    Returns:
        Dictionary of quality metrics
    """
    metrics = {
        'completeness': {},
        'validity': {},
        'consistency': {},
        'uniqueness': {},
        'accuracy': {},
        'timeliness': {},
        'overall': {}
    }
    
    # Completeness Metrics
    metrics['completeness'] = calculate_completeness_metrics(df, config)
    
    # Validity Metrics
    metrics['validity'] = calculate_validity_metrics(df, config)
    
    # Consistency Metrics
    metrics['consistency'] = calculate_consistency_metrics(df, config)
    
    # Uniqueness Metrics
    metrics['uniqueness'] = calculate_uniqueness_metrics(df, config)
    
    # Accuracy Metrics (if reference data provided)
    if reference_df is not None:
        metrics['accuracy'] = calculate_accuracy_metrics(df, reference_df, config)
    
    # Calculate overall scores
    metrics['overall'] = calculate_overall_scores(metrics)
    
    return metrics

def calculate_completeness_metrics(df: pd.DataFrame, 
                                  config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Calculate completeness metrics."""
    total_cells = len(df) * len(df.columns) if len(df) > 0 else 0
    missing_cells = df.isna().sum().sum()
    
    metrics = {
        'missing_cells': int(missing_cells),
        'missing_percentage': round((missing_cells / total_cells) * 100, 2) if total_cells > 0 else 0,
        'complete_rows': int((df.isna().sum(axis=1) == 0).sum()),
        'complete_rows_percentage': round(((df.isna().sum(axis=1) == 0).sum() / len(df)) * 100, 2) if len(df) > 0 else 0,
        'complete_columns': int((df.isna().sum() == 0).sum()),
        'complete_columns_percentage': round(((df.isna().sum() == 0).sum() / len(df.columns)) * 100, 2) if len(df.columns) > 0 else 0,
        'sparsity': round(missing_cells / total_cells, 4) if total_cells > 0 else 0
    }
    
    # Column-level completeness
    column_completeness = {}
    for col in df.columns:
        missing = df[col].isna().sum()
        total = len(df)
        column_completeness[col] = {
            'missing': int(missing),
            'missing_percentage': round((missing / total) * 100, 2) if total > 0 else 0,
            'completeness_score': round((1 - (missing / total)) * 100, 2) if total > 0 else 0
        }
    
    metrics['column_completeness'] = column_completeness
    
    # Row-level completeness
    row_completeness = df.isna().sum(axis=1).value_counts().sort_index().to_dict()
    metrics['row_missing_distribution'] = {
        f'{k}_missing': v for k, v in row_completeness.items()
    }
    
    # Calculate completeness score (0-100)
    if total_cells > 0:
        completeness_score = (1 - (missing_cells / total_cells)) * 100
    else:
        completeness_score = 0
    
    metrics['completeness_score'] = round(completeness_score, 2)
    
    return metrics

def calculate_validity_metrics(df: pd.DataFrame, 
                              config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Calculate validity metrics."""
    metrics = {
        'valid_rows': 0,
        'valid_rows_percentage': 0,
        'validation_issues': {},
        'validity_score': 0
    }
    
    # This is a basic implementation
    # In a real system, you'd check against validation rules
    
    # Check for obvious validity issues
    issues = {}
    
    # Check numeric columns for non-numeric values
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        # Check for infinite values
        infinite = np.isinf(df[col]).sum()
        if infinite > 0:
            issues[f'{col}_infinite'] = int(infinite)
    
    # Check string columns for suspicious patterns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        # Check for empty strings that might be missing values
        empty_strings = (df[col] == '').sum()
        if empty_strings > 0:
            issues[f'{col}_empty_strings'] = int(empty_strings)
    
    metrics['validation_issues'] = issues
    
    # Calculate validity score
    total_cells = len(df) * len(df.columns) if len(df) > 0 else 0
    if total_cells > 0:
        total_issues = sum(issues.values())
        validity_score = (1 - (total_issues / total_cells)) * 100
        metrics['validity_score'] = round(max(0, validity_score), 2)
    
    return metrics

def calculate_consistency_metrics(df: pd.DataFrame, 
                                 config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Calculate consistency metrics."""
    metrics = {
        'type_consistency': {},
        'format_consistency': {},
        'value_consistency': {},
        'consistency_score': 0
    }
    
    # Type consistency check
    type_consistency = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        unique_types = set(type(val).__name__ for val in df[col].dropna().iloc[:100])
        type_consistency[col] = {
            'dtype': dtype,
            'mixed_types': len(unique_types) > 1,
            'unique_type_count': len(unique_types)
        }
    
    metrics['type_consistency'] = type_consistency
    
    # Format consistency (for string columns)
    format_consistency = {}
    string_cols = df.select_dtypes(include=['object']).columns
    
    for col in string_cols[:5]:  # Limit to first 5 string columns
        sample = df[col].dropna().iloc[:100]
        if len(sample) > 0:
            # Check length consistency
            lengths = sample.str.len()
            length_std = lengths.std()
            format_consistency[col] = {
                'length_std': round(float(length_std), 2),
                'consistent_length': length_std < 5  # Arbitrary threshold
            }
    
    metrics['format_consistency'] = format_consistency
    
    # Calculate consistency score
    total_cols = len(df.columns)
    if total_cols > 0:
        # Penalize mixed types and inconsistent formats
        mixed_type_count = sum(1 for v in type_consistency.values() if v['mixed_types'])
        inconsistent_format_count = sum(1 for v in format_consistency.values() if not v.get('consistent_length', True))
        
        consistency_score = 100 - (mixed_type_count * 10) - (inconsistent_format_count * 5)
        metrics['consistency_score'] = round(max(0, consistency_score), 2)
    
    return metrics

def calculate_uniqueness_metrics(df: pd.DataFrame, 
                                config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Calculate uniqueness metrics."""
    exact_duplicates = df.duplicated().sum()
    total_rows = len(df)
    
    metrics = {
        'exact_duplicates': int(exact_duplicates),
        'exact_duplicate_percentage': round((exact_duplicates / total_rows) * 100, 2) if total_rows > 0 else 0,
        'unique_rows': int(total_rows - exact_duplicates),
        'unique_rows_percentage': round(((total_rows - exact_duplicates) / total_rows) * 100, 2) if total_rows > 0 else 0,
        'column_uniqueness': {}
    }
    
    # Column-level uniqueness
    for col in df.columns:
        unique_values = df[col].nunique()
        total_values = len(df[col].dropna())
        
        metrics['column_uniqueness'][col] = {
            'unique_values': int(unique_values),
            'total_values': int(total_values),
            'uniqueness_ratio': round(unique_values / total_values, 4) if total_values > 0 else 0,
            'cardinality': 'high' if unique_values / total_values > 0.9 else 
                          'medium' if unique_values / total_values > 0.5 else 
                          'low'
        }
    
    # Calculate uniqueness score
    if total_rows > 0:
        uniqueness_score = (1 - (exact_duplicates / total_rows)) * 100
    else:
        uniqueness_score = 0
    
    metrics['uniqueness_score'] = round(uniqueness_score, 2)
    
    return metrics

def calculate_accuracy_metrics(df: pd.DataFrame, 
                              reference_df: pd.DataFrame,
                              config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Calculate accuracy metrics against reference data."""
    metrics = {
        'matching_rows': 0,
        'matching_columns': 0,
        'accuracy_score': 0,
        'discrepancies': {}
    }
    
    # This is a simplified implementation
    # In reality, you'd need to match rows and compare values
    
    # Check if DataFrames have same columns
    common_cols = set(df.columns).intersection(set(reference_df.columns))
    metrics['matching_columns'] = len(common_cols)
    
    # For numeric columns in common, calculate correlation
    numeric_common = [col for col in common_cols 
                     if pd.api.types.is_numeric_dtype(df[col]) and 
                     pd.api.types.is_numeric_dtype(reference_df[col])]
    
    correlations = {}
    for col in numeric_common[:10]:  # Limit to first 10 numeric columns
        try:
            # Align data (simplified - assumes same indices)
            corr = df[col].corr(reference_df[col])
            correlations[col] = round(float(corr), 4)
        except:
            correlations[col] = None
    
    metrics['numeric_correlations'] = correlations
    
    # Calculate accuracy score (simplified)
    if len(common_cols) > 0:
        # Average correlation as proxy for accuracy
        valid_corrs = [c for c in correlations.values() if c is not None]
        if valid_corrs:
            accuracy_score = (sum(valid_corrs) / len(valid_corrs)) * 100
            metrics['accuracy_score'] = round(max(0, accuracy_score), 2)
    
    return metrics

def calculate_overall_scores(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate overall scores from individual metrics."""
    # Weighted average of component scores
    weights = {
        'completeness': 0.30,
        'validity': 0.25,
        'consistency': 0.20,
        'uniqueness': 0.15,
        'accuracy': 0.10
    }
    
    overall_score = 0
    component_scores = {}
    
    for component, weight in weights.items():
        component_score = metrics.get(component, {}).get(f'{component}_score', 0)
        component_scores[component] = component_score
        overall_score += component_score * weight
    
    return {
        'overall_score': round(overall_score, 2),
        'component_scores': component_scores,
        'weights': weights
    }

def generate_metrics_summary(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Generate human-readable summary of metrics."""
    summary = {
        'overall_quality': metrics.get('overall', {}).get('overall_score', 0),
        'component_scores': {},
        'key_metrics': {},
        'recommendations': []
    }
    
    # Component scores
    for component in ['completeness', 'validity', 'consistency', 'uniqueness']:
        score = metrics.get(component, {}).get(f'{component}_score', 0)
        summary['component_scores'][component] = {
            'score': score,
            'rating': 'excellent' if score >= 90 else 
                     'good' if score >= 80 else 
                     'fair' if score >= 70 else 
                     'poor' if score >= 60 else 
                     'very poor'
        }
    
    # Key metrics
    completeness = metrics.get('completeness', {})
    uniqueness = metrics.get('uniqueness', {})
    
    summary['key_metrics'] = {
        'missing_percentage': completeness.get('missing_percentage', 0),
        'complete_rows_percentage': completeness.get('complete_rows_percentage', 0),
        'duplicate_percentage': uniqueness.get('exact_duplicate_percentage', 0),
        'unique_rows_percentage': uniqueness.get('unique_rows_percentage', 0)
    }
    
    # Generate recommendations
    if completeness.get('missing_percentage', 0) > 20:
        summary['recommendations'].append({
            'type': 'warning',
            'message': 'High percentage of missing values',
            'suggestion': 'Review data sources and imputation strategy'
        })
    
    if uniqueness.get('exact_duplicate_percentage', 0) > 10:
        summary['recommendations'].append({
            'type': 'warning',
            'message': 'High duplicate rate',
            'suggestion': 'Review deduplication strategy'
        })
    
    overall_score = summary['overall_quality']
    if overall_score >= 90:
        summary['overall_rating'] = 'Excellent'
    elif overall_score >= 80:
        summary['overall_rating'] = 'Good'
    elif overall_score >= 70:
        summary['overall_rating'] = 'Fair'
    elif overall_score >= 60:
        summary['overall_rating'] = 'Poor'
    else:
        summary['overall_rating'] = 'Very Poor'
    
    return summary