"""
Report Builder - Compiles comprehensive quality reports.
Aggregates metrics, issues, fixes, and calculates quality scores.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from pathlib import Path
import json
import logging
from dataclasses import dataclass, asdict
from ..engine.config import EngineConfig

logger = logging.getLogger(__name__)

@dataclass
class ReportMetrics:
    """Metrics for the quality report."""
    completeness_score: float = 0.0
    validity_score: float = 0.0
    consistency_score: float = 0.0
    uniqueness_score: float = 0.0
    overall_score: float = 0.0
    
    def to_dict(self):
        return asdict(self)

class ReportBuilder:
    """
    Builds comprehensive data quality reports from pipeline execution.
    """
    
    def __init__(self):
        self.report_data = {}
    
    def build_report(self, initial_df: pd.DataFrame, final_df: pd.DataFrame,
                    step_logs: List[Dict], config: EngineConfig) -> Dict[str, Any]:
        """
        Build a complete quality report.
        
        Args:
            initial_df: Original DataFrame
            final_df: Cleaned DataFrame
            step_logs: Logs from each pipeline stage
            config: Configuration dictionary
            
        Returns:
            Comprehensive report dictionary
        """
        logger.info("Building comprehensive quality report")
        
        # Calculate metrics
        metrics = self._calculate_quality_metrics(initial_df, final_df, step_logs)
        summary = self._build_summary(initial_df, final_df, step_logs, config)
        recommendations = self._generate_recommendations(final_df, step_logs, config)
        visualizations = self._prepare_visualization_data(initial_df, final_df)
        
        report = {
            "report_id": f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics.to_dict(),
            "summary": summary,
            "initial_stats": self._calculate_stats(initial_df, "initial"),
            "final_stats": self._calculate_stats(final_df, "final"),
            "step_logs": step_logs,
            "recommendations": recommendations,
            "visualization_data": visualizations,
            "status": self._determine_status(step_logs),
            "config_summary": self._summarize_config(config),
            "execution_summary": self._extract_execution_summary(step_logs)
        }
        
        logger.info(f"Report built with overall score: {metrics.overall_score:.2f}")
        return report
    
    def _calculate_stats(self, df: pd.DataFrame, label: str) -> Dict[str, Any]:
        """Calculate statistics for a DataFrame."""
        try:
            missing_by_col = df.isna().sum()
            missing_pct_by_col = (missing_by_col / len(df)) * 100 if len(df) > 0 else 0
            
            stats = {
                "label": label,
                "rows": len(df),
                "columns": len(df.columns),
                "total_cells": len(df) * len(df.columns),
                "missing_total": int(missing_by_col.sum()),
                "missing_percentage": round((missing_by_col.sum() / (len(df) * len(df.columns))) * 100, 2) if len(df) > 0 else 0,
                "missing_by_column": missing_by_col.to_dict(),
                "missing_percentage_by_column": missing_pct_by_col.to_dict(),
                "duplicates": int(df.duplicated().sum()),
                "duplicate_percentage": round((df.duplicated().sum() / len(df)) * 100, 2) if len(df) > 0 else 0,
                "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
                "dtypes": df.dtypes.astype(str).to_dict(),
                "numeric_columns": len(df.select_dtypes(include=[np.number]).columns),
                "text_columns": len(df.select_dtypes(include=['object']).columns),
                "datetime_columns": len(df.select_dtypes(include=['datetime']).columns),
                "categorical_columns": len(df.select_dtypes(include=['category']).columns)
            }
            
            # Add basic numeric column statistics
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            numeric_stats = {}
            for col in numeric_cols:
                if df[col].notna().sum() > 0:
                    numeric_stats[col] = {
                        'mean': round(float(df[col].mean()), 2),
                        'std': round(float(df[col].std()), 2),
                        'min': round(float(df[col].min()), 2),
                        'max': round(float(df[col].max()), 2),
                        'median': round(float(df[col].median()), 2),
                        'q1': round(float(df[col].quantile(0.25)), 2),
                        'q3': round(float(df[col].quantile(0.75)), 2)
                    }
            stats['numeric_statistics'] = numeric_stats
            
            return stats
            
        except Exception as e:
            logger.error(f"Error calculating stats: {str(e)}")
            return {
                "label": label,
                "rows": len(df),
                "columns": len(df.columns),
                "error": str(e)
            }
    
    def _calculate_quality_metrics(self, initial_df: pd.DataFrame, 
                                 final_df: pd.DataFrame, 
                                 step_logs: List[Dict]) -> ReportMetrics:
        """
        Calculate overall quality metrics.
        
        Scoring criteria:
        - Completeness (40%): Percentage of non-null values
        - Validity (30%): Passed validation checks
        - Consistency (20%): Successful type enforcement
        - Uniqueness (10%): Duplicate removal
        """
        metrics = ReportMetrics()
        
        # 1. Completeness Score (40 points)
        if len(final_df) > 0 and len(final_df.columns) > 0:
            total_cells = len(final_df) * len(final_df.columns)
            if total_cells > 0:
                missing_cells = final_df.isna().sum().sum()
                completeness_pct = (1 - (missing_cells / total_cells)) * 100
                metrics.completeness_score = min(40, completeness_pct * 0.4)  # 40% of total
            else:
                metrics.completeness_score = 0
        else:
            metrics.completeness_score = 0
        
        # 2. Validity Score (30 points) - from verification stage
        verification_log = next((log for log in step_logs if log.get('step') == 'verify'), None)
        if verification_log:
            if verification_log.get('passed', False):
                metrics.validity_score = 30
            else:
                # Calculate based on issues and warnings
                issues = len(verification_log.get('issues', []))
                warnings = len(verification_log.get('warnings', []))
                
                if issues == 0 and warnings == 0:
                    metrics.validity_score = 30
                elif issues == 0:
                    # Only warnings, deduct based on severity
                    metrics.validity_score = 30 - (warnings * 0.5)
                elif warnings == 0:
                    # Only issues, more severe deduction
                    metrics.validity_score = 30 - (issues * 2)
                else:
                    # Mixed issues and warnings
                    metrics.validity_score = 30 - (issues * 1.5) - (warnings * 0.5)
                
                # Ensure non-negative
                metrics.validity_score = max(0, metrics.validity_score)
        else:
            metrics.validity_score = 20  # Default if no verification
        
        # 3. Consistency Score (20 points) - from schema enforcement
        schema_log = next((log for log in step_logs if log.get('step') == 'enforce_schema'), None)
        if schema_log:
            conversions = schema_log.get('type_conversions', 0)
            errors = len(schema_log.get('errors', []))
            
            # Reward conversions, penalize errors
            metrics.consistency_score = min(20, (conversions * 0.5) - (errors * 1))
            metrics.consistency_score = max(0, metrics.consistency_score)
        else:
            metrics.consistency_score = 10  # Default
        
        # 4. Uniqueness Score (10 points) - from deduplication
        dup_log = next((log for log in step_logs if log.get('step') == 'deduplicate'), None)
        if dup_log:
            duplicates_removed = dup_log.get('duplicates_removed', 0)
            if duplicates_removed > 0:
                metrics.uniqueness_score = 10  # Full points for removing duplicates
            else:
                # Check if there were duplicates to begin with
                initial_dups = initial_df.duplicated().sum()
                if initial_dups == 0:
                    metrics.uniqueness_score = 10  # No duplicates to begin with
                else:
                    metrics.uniqueness_score = 5  # Had duplicates but didn't remove
        else:
            # Check initial duplicates
            initial_dups = initial_df.duplicated().sum()
            if initial_dups == 0:
                metrics.uniqueness_score = 10
            else:
                metrics.uniqueness_score = 3
        
        # Calculate overall score
        metrics.overall_score = round(
            metrics.completeness_score + 
            metrics.validity_score + 
            metrics.consistency_score + 
            metrics.uniqueness_score, 2
        )
        
        return metrics
    
    def _build_summary(self, initial_df: pd.DataFrame, 
                      final_df: pd.DataFrame,
                      step_logs: List[Dict],
                      config: EngineConfig) -> Dict[str, Any]:
        """Build a human-readable summary."""
        rows_removed = len(initial_df) - len(final_df)
        cols_removed = len(initial_df.columns) - len(final_df.columns)
        
        # Count total fixes across all stages
        total_fixes = 0
        fixes_by_type = {
            'missing_values': 0,
            'duplicates': 0,
            'outliers': 0,
            'type_conversions': 0,
            'text_cleaning': 0,
            'other': 0
        }
        
        for log in step_logs:
            stage = log.get('step', '')
            
            # Missing values
            if 'imputed_values' in log:
                imputed = log['imputed_values']
                if isinstance(imputed, dict):
                    fixes = sum(len(v) for v in imputed.values() if isinstance(v, list))
                elif isinstance(imputed, list):
                    fixes = len(imputed)
                else:
                    fixes = imputed if isinstance(imputed, int) else 0
                total_fixes += fixes
                fixes_by_type['missing_values'] += fixes
            
            # Duplicates
            if 'duplicates_removed' in log:
                fixes = log['duplicates_removed']
                total_fixes += fixes
                fixes_by_type['duplicates'] += fixes
            
            # Outliers
            if 'outliers_handled' in log:
                fixes = log['outliers_handled']
                total_fixes += fixes
                fixes_by_type['outliers'] += fixes
            
            # Type conversions
            if 'type_conversions' in log:
                fixes = log['type_conversions']
                total_fixes += fixes
                fixes_by_type['type_conversions'] += fixes
            
            # Text cleaning
            if 'text_values_cleaned' in log:
                fixes = log['text_values_cleaned']
                total_fixes += fixes
                fixes_by_type['text_cleaning'] += fixes
            
            # Other fixes
            if 'rows_affected' in log:
                fixes = log['rows_affected']
                if fixes > 0 and stage not in ['deduplicate', 'handle_missing', 'handle_outliers']:
                    total_fixes += fixes
                    fixes_by_type['other'] += fixes
        
        # Calculate improvement percentages
        initial_missing = initial_df.isna().sum().sum()
        final_missing = final_df.isna().sum().sum()
        missing_improvement = 0
        if initial_missing > 0:
            missing_improvement = ((initial_missing - final_missing) / initial_missing) * 100
        
        initial_dups = initial_df.duplicated().sum()
        final_dups = final_df.duplicated().sum()
        duplicate_improvement = 0
        if initial_dups > 0:
            duplicate_improvement = ((initial_dups - final_dups) / initial_dups) * 100
        
        return {
            "rows_removed": rows_removed,
            "rows_retained": len(final_df),
            "retention_rate": round((len(final_df) / len(initial_df)) * 100, 2) if len(initial_df) > 0 else 0,
            "columns_removed": cols_removed,
            "columns_retained": len(final_df.columns),
            "total_fixes_applied": total_fixes,
            "fixes_by_type": fixes_by_type,
            "stages_executed": len([log for log in step_logs if log.get('status') == 'completed']),
            "missing_improvement_pct": round(missing_improvement, 2),
            "duplicate_improvement_pct": round(duplicate_improvement, 2),
            "execution_time_seconds": self._calculate_execution_time(step_logs)
        }
    
    def _generate_recommendations(self, df: pd.DataFrame, 
                                  step_logs: List[Dict],
                                  config: EngineConfig) -> List[Dict[str, Any]]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Check for high missing values
        for col in df.columns:
            missing_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
            if missing_pct > 20:
                recommendations.append({
                    'type': 'warning',
                    'category': 'missing_values',
                    'message': f"Column '{col}' has {missing_pct:.1f}% missing values",
                    'action': 'Consider reviewing data source or imputation strategy',
                    'severity': 'medium' if missing_pct <= 50 else 'high'
                })
        
        # Check verification issues
        verification_log = next((log for log in step_logs if log.get('step') == 'verify'), None)
        if verification_log and not verification_log.get('passed', True):
            issues = verification_log.get('issues', [])
            warnings = verification_log.get('warnings', [])
            
            if issues:
                recommendations.append({
                    'type': 'error',
                    'category': 'validation',
                    'message': f"Validation failed with {len(issues)} critical issues",
                    'action': 'Review validation rules and data quality before export',
                    'severity': 'high'
                })
            
            if warnings:
                recommendations.append({
                    'type': 'warning',
                    'category': 'validation',
                    'message': f"Validation completed with {len(warnings)} warnings",
                    'action': 'Review warnings and consider adjustments',
                    'severity': 'low'
                })
        
        # Check for low retention
        initial_rows = step_logs[0].get('input_shape', [0, 0])[0] if step_logs else len(df)
        retention = (len(df) / initial_rows) * 100 if initial_rows > 0 else 100
        if retention < 70:
            recommendations.append({
                'type': 'warning',
                'category': 'data_loss',
                'message': f"Only {retention:.1f}% of rows retained",
                'action': 'Review cleaning rules to avoid excessive data loss',
                'severity': 'medium'
            })
        
        # Check for remaining duplicates
        remaining_dups = df.duplicated().sum()
        if remaining_dups > 0:
            dup_pct = (remaining_dups / len(df)) * 100 if len(df) > 0 else 0
            recommendations.append({
                'type': 'warning',
                'category': 'duplicates',
                'message': f"{remaining_dups} duplicate rows remain ({dup_pct:.1f}%)",
                'action': 'Review deduplication strategy',
                'severity': 'low' if dup_pct < 5 else 'medium'
            })
        
        # Check numeric column distributions
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].notna().sum() > 10:  # Need enough data
                skewness = df[col].skew()
                if abs(skewness) > 1:
                    recommendations.append({
                        'type': 'info',
                        'category': 'distribution',
                        'message': f"Column '{col}' has skewed distribution (skewness: {skewness:.2f})",
                        'action': 'Consider transformation for better analysis',
                        'severity': 'low'
                    })
        
        # If no issues found
        if not recommendations:
            recommendations.append({
                'type': 'success',
                'category': 'overall',
                'message': 'Data quality is good',
                'action': 'Ready for analysis or export',
                'severity': 'none'
            })
        
        # Sort by severity
        severity_order = {'high': 3, 'medium': 2, 'low': 1, 'none': 0}
        recommendations.sort(key=lambda x: severity_order.get(x['severity'], 0), reverse=True)
        
        return recommendations
    
    def _prepare_visualization_data(self, initial_df: pd.DataFrame, 
                                   final_df: pd.DataFrame) -> Dict[str, Any]:
        """Prepare data for visualizations."""
        viz_data = {
            'missing_values_comparison': {},
            'duplicate_comparison': {},
            'column_type_distribution': {},
            'data_quality_timeline': []
        }
        
        # Missing values comparison
        initial_missing = initial_df.isna().sum()
        final_missing = final_df.isna().sum()
        
        common_cols = set(initial_df.columns).intersection(set(final_df.columns))
        for col in common_cols:
            initial_pct = (initial_missing[col] / len(initial_df)) * 100 if len(initial_df) > 0 else 0
            final_pct = (final_missing[col] / len(final_df)) * 100 if len(final_df) > 0 else 0
            
            viz_data['missing_values_comparison'][col] = {
                'initial': round(initial_pct, 2),
                'final': round(final_pct, 2),
                'improvement': round(initial_pct - final_pct, 2)
            }
        
        # Duplicate comparison
        viz_data['duplicate_comparison'] = {
            'initial': int(initial_df.duplicated().sum()),
            'final': int(final_df.duplicated().sum()),
            'initial_pct': round((initial_df.duplicated().sum() / len(initial_df)) * 100, 2) if len(initial_df) > 0 else 0,
            'final_pct': round((final_df.duplicated().sum() / len(final_df)) * 100, 2) if len(final_df) > 0 else 0
        }
        
        # Column type distribution
        viz_data['column_type_distribution']['initial'] = {
            str(dtype): count 
            for dtype, count in initial_df.dtypes.value_counts().items()
        }
        viz_data['column_type_distribution']['final'] = {
            str(dtype): count 
            for dtype, count in final_df.dtypes.value_counts().items()
        }
        
        return viz_data
    
    def _determine_status(self, step_logs: List[Dict]) -> str:
        """Determine overall pipeline status."""
        # Check for verification stage
        verification_log = next((log for log in step_logs if log.get('step') == 'verify'), None)
        
        if verification_log:
            if verification_log.get('passed', False):
                return "SUCCESS"
            else:
                return "FAILED_VALIDATION"
        
        # Check for critical stage failures
        for log in step_logs:
            if log.get('status') == 'failed' and log.get('critical', False):
                return "FAILED_CRITICAL_STAGE"
        
        # Check for any errors
        for log in step_logs:
            if log.get('errors'):
                return "COMPLETED_WITH_ERRORS"
        
        # Check if all stages completed
        completed_stages = [log for log in step_logs if log.get('status') == 'completed']
        if len(completed_stages) == len(step_logs):
            return "SUCCESS"
        else:
            return "PARTIAL_SUCCESS"
    
    def _summarize_config(self, config: EngineConfig) -> Dict[str, Any]:
        """Summarize configuration for the report."""
        return {
            'schema': {
                'required_columns': len(config.schema.required_columns),
                'data_types_defined': len(config.schema.data_types)
            },
            'missing_values': {
                'strategies': config.missing_values.strategies,
                'thresholds': config.missing_values.thresholds
            },
            'outliers': {
                'method': config.outliers.method,
                'strategy': config.outliers.strategy
            },
            'duplicates': {
                'remove_full_duplicates': config.duplicates.remove_full_row_duplicates,
                'keep_strategy': config.duplicates.keep
            },
            'text_cleaning': {
                'strip_whitespace': config.text_cleaning.strip_whitespace,
                'normalize_case': config.text_cleaning.normalize_case
            },
            'performance': {
                'max_file_size_mb': config.max_file_size_mb,
                'parallel_processing': config.enable_parallel_processing
            }
        }
    
    def _extract_execution_summary(self, step_logs: List[Dict]) -> Dict[str, Any]:
        """Extract execution summary from step logs."""
        summary = {
            'total_stages': len(step_logs),
            'successful_stages': len([log for log in step_logs if log.get('status') == 'completed']),
            'failed_stages': len([log for log in step_logs if log.get('status') == 'failed']),
            'stage_durations': {},
            'stage_statuses': {}
        }
        
        total_duration = 0
        for log in step_logs:
            stage_name = log.get('step', 'unknown')
            duration = log.get('duration_seconds', 0)
            status = log.get('status', 'unknown')
            
            summary['stage_durations'][stage_name] = duration
            summary['stage_statuses'][stage_name] = status
            total_duration += duration
        
        summary['total_duration_seconds'] = round(total_duration, 2)
        
        return summary
    
    def _calculate_execution_time(self, step_logs: List[Dict]) -> float:
        """Calculate total execution time from step logs."""
        total_time = 0
        for log in step_logs:
            total_time += log.get('duration_seconds', 0)
        return round(total_time, 2)
    
    def export_to_dict(self, report: Dict) -> Dict:
        """Export report as a dictionary (for JSON serialization)."""
        return report
    
    def export_to_markdown(self, report: Dict) -> str:
        """Export report as markdown."""
        metrics = report.get('metrics', {})
        summary = report.get('summary', {})
        recommendations = report.get('recommendations', [])
        
        md = f"""# Data Quality Report

**Report ID:** {report.get('report_id', 'N/A')}  
**Generated:** {report.get('timestamp', 'N/A')}  
**Status:** {report.get('status', 'N/A')}  
**Overall Quality Score:** {metrics.get('overall_score', 0):.1f}/100

## Quality Metrics

| Metric | Score | Description |
|--------|-------|-------------|
| Completeness | {metrics.get('completeness_score', 0):.1f} | Non-null values and data retention |
| Validity | {metrics.get('validity_score', 0):.1f} | Schema compliance and validation |
| Consistency | {metrics.get('consistency_score', 0):.1f} | Data type consistency |
| Uniqueness | {metrics.get('uniqueness_score', 0):.1f} | Duplicate removal |

## Summary

| Metric | Initial | Final | Improvement |
|--------|---------|-------|-------------|
| Rows | {report['initial_stats'].get('rows', 0)} | {report['final_stats'].get('rows', 0)} | {summary.get('retention_rate', 0):.1f}% retained |
| Columns | {report['initial_stats'].get('columns', 0)} | {report['final_stats'].get('columns', 0)} | {summary.get('columns_removed', 0)} removed |
| Missing Values | {report['initial_stats'].get('missing_total', 0)} | {report['final_stats'].get('missing_total', 0)} | {summary.get('missing_improvement_pct', 0):.1f}% improvement |
| Duplicates | {report['initial_stats'].get('duplicates', 0)} | {report['final_stats'].get('duplicates', 0)} | {summary.get('duplicate_improvement_pct', 0):.1f}% improvement |

**Total Fixes Applied:** {summary.get('total_fixes_applied', 0)}  
**Stages Executed:** {summary.get('stages_executed', 0)}  
**Execution Time:** {summary.get('execution_time_seconds', 0):.2f} seconds

## Fixes by Type

"""
        
        fixes_by_type = summary.get('fixes_by_type', {})
        for fix_type, count in fixes_by_type.items():
            if count > 0:
                md += f"- **{fix_type.replace('_', ' ').title()}**: {count}\n"
        
        md += "\n## Recommendations\n\n"
        
        for i, rec in enumerate(recommendations, 1):
            severity_emoji = {
                'high': '🔴',
                'medium': '🟡', 
                'low': '🟢',
                'none': '✅'
            }.get(rec.get('severity', ''), '')
            
            md += f"{i}. {severity_emoji} **{rec.get('type', '').upper()}**: {rec.get('message', '')}\n"
            md += f"   *Action*: {rec.get('action', '')}\n\n"
        
        md += "## Stage Execution Summary\n\n"
        
        execution = report.get('execution_summary', {})
        stage_statuses = execution.get('stage_statuses', {})
        stage_durations = execution.get('stage_durations', {})
        
        for stage, status in stage_statuses.items():
            duration = stage_durations.get(stage, 0)
            status_emoji = "✅" if status == "completed" else "❌" if status == "failed" else "⚠️"
            md += f"- **{stage}**: {status_emoji} ({duration:.2f}s)\n"
        
        return md