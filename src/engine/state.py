# from dataclasses import dataclass, field, asdict
# from typing import Dict, List, Any, Optional
# import pandas as pd
# from datetime import datetime

# @dataclass
# class PipelineState:
#     """
#     Represents the state of the data pipeline at any given moment.
#     Tracks changes, logs, and quality metrics.
#     """
#     id: str
#     timestamp: str
#     status: str = "PENDING"  # PENDING, RUNNING, SUCCESS, FAILED
#     steps_executed: List[str] = field(default_factory=list)
#     step_logs: List[Dict[str, Any]] = field(default_factory=list)
#     errors: List[str] = field(default_factory=list)
#     quality_score: float = 0.0
#     initial_stats: Dict[str, Any] = field(default_factory=dict)
#     final_stats: Dict[str, Any] = field(default_factory=dict)
    
#     _start_time: float = field(default_factory=datetime.now().timestamp)

#     def compress_logs(self, keep_last_n: int = 100):
#         """Keep only recent logs to prevent memory bloat"""
#         if len(self.step_logs) > keep_last_n:
#             self.step_logs = self.step_logs[-keep_last_n:]
    
#     def get_summary_log(self) -> Dict[str, Any]:
#         """Get summarized logs for long-running pipelines"""
#         return {
#             "total_steps": len(self.steps_executed),
#             "last_5_steps": self.steps_executed[-5:],
#             "error_count": len(self.errors),
#             "recent_logs": self.step_logs[-10:] if self.step_logs else []
#         }
#     def start(self, df: pd.DataFrame):
#         self.status = "RUNNING"
#         self.initial_stats = self._capture_stats(df)
#         self.timestamp = datetime.now().isoformat()

#     def add_step_log(self, step_name: str, details: Dict[str, Any]):
#         self.steps_executed.append(step_name)
#         self.step_logs.append({
#             "step": step_name,
#             "timestamp": datetime.now().isoformat(),
#             **details
#         })

#     def fail(self, error: str):
#         self.status = "FAILED"
#         self.errors.append(error)

#     def complete(self, df: pd.DataFrame, score: float = 100.0):
#         self.status = "SUCCESS"
#         self.quality_score = score
#         self.final_stats = self._capture_stats(df)

#     def get_report(self) -> Dict[str, Any]:
#         return asdict(self)

#     def _capture_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
#         return {
#             "rows": len(df),
#             "cols": len(df.columns),
#             "missing_total": int(df.isna().sum().sum()),
#             "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
#         }
# class Stage:
#     def __init__(self, name, instance, config):
#         self.name = name
#         self.instance = instance
#         self.config = config
"""
Pipeline State Management
Tracks the state of the data pipeline at any given moment.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional
import pandas as pd
from datetime import datetime
import json

@dataclass
class PipelineMetrics:
    """Metrics for tracking pipeline performance."""
    execution_time: float = 0.0
    memory_usage_mb: float = 0.0
    rows_processed: int = 0
    columns_processed: int = 0
    transformations_applied: int = 0

@dataclass
class PipelineState:
    """
    Represents the state of the data pipeline at any given moment.
    Tracks changes, logs, and quality metrics.
    """
    id: str
    timestamp: str
    status: str = "PENDING"  # PENDING, RUNNING, SUCCESS, FAILED
    steps_executed: List[str] = field(default_factory=list)
    step_logs: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    quality_score: float = 0.0
    initial_stats: Dict[str, Any] = field(default_factory=dict)
    final_stats: Dict[str, Any] = field(default_factory=dict)
    metrics: PipelineMetrics = field(default_factory=PipelineMetrics)
    
    _start_time: float = field(default_factory=lambda: datetime.now().timestamp())
    _current_df: Optional[pd.DataFrame] = field(default=None, repr=False)

    def start(self, df: pd.DataFrame):
        """Start the pipeline with initial DataFrame."""
        self.status = "RUNNING"
        self.timestamp = datetime.now().isoformat()
        self._current_df = df.copy()
        self.initial_stats = self._capture_stats(df)
        self.metrics.rows_processed = len(df)
        self.metrics.columns_processed = len(df.columns)

    def add_step_log(self, step_name: str, details: Dict[str, Any]):
        """Add log entry for a pipeline step."""
        self.steps_executed.append(step_name)
        self.step_logs.append({
            "step": step_name,
            "timestamp": datetime.now().isoformat(),
            "duration": datetime.now().timestamp() - self._start_time,
            **details
        })
        
        # Update metrics
        if 'rows_affected' in details:
            self.metrics.transformations_applied += details['rows_affected']

    def fail(self, error: str):
        """Mark pipeline as failed with error message."""
        self.status = "FAILED"
        self.errors.append({
            "timestamp": datetime.now().isoformat(),
            "message": error
        })

    def add_warning(self, warning: str):
        """Add warning message."""
        self.warnings.append({
            "timestamp": datetime.now().isoformat(),
            "message": warning
        })

    def complete(self, df: pd.DataFrame, score: float = 100.0):
        """Mark pipeline as completed successfully."""
        self.status = "SUCCESS"
        self.quality_score = score
        self.final_stats = self._capture_stats(df)
        self._current_df = df.copy()
        self.metrics.execution_time = datetime.now().timestamp() - self._start_time

    def get_report(self) -> Dict[str, Any]:
        """Get comprehensive pipeline report."""
        report = asdict(self)
        
        # Remove internal fields
        report.pop('_start_time', None)
        report.pop('_current_df', None)
        
        # Add summary statistics
        report['summary'] = {
            'total_steps': len(self.steps_executed),
            'total_errors': len(self.errors),
            'total_warnings': len(self.warnings),
            'quality_score': self.quality_score,
            'execution_time_seconds': round(self.metrics.execution_time, 2)
        }
        
        return report

    def _capture_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Capture statistics from DataFrame."""
        try:
            return {
                "rows": len(df),
                "columns": len(df.columns),
                "missing_total": int(df.isna().sum().sum()),
                "missing_percentage": round((df.isna().sum().sum() / (len(df) * len(df.columns))) * 100, 2),
                "duplicates": int(df.duplicated().sum()),
                "duplicate_percentage": round((df.duplicated().sum() / len(df)) * 100, 2) if len(df) > 0 else 0,
                "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
                "dtype_distribution": {
                    str(dtype): count 
                    for dtype, count in df.dtypes.value_counts().items()
                }
            }
        except Exception as e:
            return {
                "rows": len(df),
                "columns": len(df.columns),
                "error": f"Failed to capture stats: {str(e)}"
            }

    def save_to_file(self, filepath: str):
        """Save pipeline state to JSON file."""
        report = self.get_report()
        
        # Convert DataFrame stats to serializable format
        def serialize_dict(d):
            if isinstance(d, dict):
                return {k: serialize_dict(v) for k, v in d.items()}
            elif hasattr(d, 'isoformat'):
                return d.isoformat()
            elif isinstance(d, (int, float, str, bool, type(None))):
                return d
            else:
                return str(d)
        
        serialized_report = serialize_dict(report)
        
        with open(filepath, 'w') as f:
            json.dump(serialized_report, f, indent=2, default=str)
    
    @classmethod
    def load_from_file(cls, filepath: str) -> 'PipelineState':
        """Load pipeline state from JSON file."""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Reconstruct PipelineState from saved data
        # Note: This won't restore the DataFrame, only metadata
        state = cls(id=data['id'], timestamp=data['timestamp'])
        state.status = data['status']
        state.steps_executed = data['steps_executed']
        state.step_logs = data['step_logs']
        state.errors = data['errors']
        state.warnings = data['warnings']
        state.quality_score = data['quality_score']
        state.initial_stats = data['initial_stats']
        state.final_stats = data['final_stats']
        
        # Reconstruct metrics
        metrics_data = data.get('metrics', {})
        state.metrics = PipelineMetrics(
            execution_time=metrics_data.get('execution_time', 0.0),
            memory_usage_mb=metrics_data.get('memory_usage_mb', 0.0),
            rows_processed=metrics_data.get('rows_processed', 0),
            columns_processed=metrics_data.get('columns_processed', 0),
            transformations_applied=metrics_data.get('transformations_applied', 0)
        )
        
        return state