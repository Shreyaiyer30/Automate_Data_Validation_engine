# import json
# import time
# from datetime import datetime
# from typing import Dict, List, Any, Optional
# from pathlib import Path

# class AuditLogger:
#     """
#     Robust logger for pipeline stages, tracking duration, severity, and state.
#     UI-agnostic and production-ready.
#     """
#     def __init__(self, log_dir: str = "logs"):
#         self.log_dir = Path(log_dir)
#         self.log_dir.mkdir(parents=True, exist_ok=True)
#         self.entries: List[Dict[str, Any]] = []
#         self._start_times: Dict[str, float] = {}
#         self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
#         self.schema_version = "1.1.0"
#         self.max_entries = 10000

#     def log_stage_start(self, stage_name: str, input_shape: tuple):
#         """Record the start of a pipeline stage."""
#         self._start_times[stage_name] = time.perf_counter()
#         entry = {
#             "event": "STEP_START",
#             "severity": "INFO",
#             "stage": stage_name,
#             "input_rows": input_shape[0],
#             "input_cols": input_shape[1]
#         }
#         self._add_entry(entry)

#     def log_stage_complete(self, stage_name: str, output_shape: tuple, metadata: Dict[str, Any] = None):
#         """Record the completion of a pipeline stage with duration."""
#         duration_ms = 0.0
#         if stage_name in self._start_times:
#             duration_ms = (time.perf_counter() - self._start_times.pop(stage_name)) * 1000
            
#         entry = {
#             "event": "STEP_COMPLETE",
#             "severity": "INFO",
#             "stage": stage_name,
#             "output_rows": output_shape[0],
#             "output_cols": output_shape[1],
#             "duration_ms": round(duration_ms, 2),
#             "metadata": metadata or {}
#         }
#         self._add_entry(entry)

#     def log_mutation(self, stage_name: str, mutation_type: str, details: Dict[str, Any]):
#         """Record data changes (e.g., imputation, outlier removal)."""
#         entry = {
#             "event": "MUTATION",
#             "severity": "INFO",
#             "stage": stage_name,
#             "type": mutation_type,
#             "details": details
#         }
#         self._add_entry(entry)

#     def log_error(self, stage_name: str, error_msg: str, critical: bool = False):
#         """Record errors during pipeline execution."""
#         entry = {
#             "event": "ERROR",
#             "severity": "CRITICAL" if critical else "ERROR",
#             "stage": stage_name,
#             "message": error_msg
#         }
#         self._add_entry(entry)

#     def _add_entry(self, entry: Dict[str, Any]):
#         """Internal helper with memory management."""
#         if len(self.entries) >= self.max_entries:
#             self.entries.pop(0)
            
#         entry.update({
#             "timestamp": datetime.now().isoformat(),
#             "session_id": self.session_id,
#             "schema_version": self.schema_version
#         })
#         self.entries.append(entry)

#     def get_summary(self) -> Dict[str, Any]:
#         """Summarize current audit trail."""
#         stages = [e['stage'] for e in self.entries if e['event'] == 'STEP_COMPLETE']
#         total_duration = sum(e.get('duration_ms', 0) for e in self.entries)
#         return {
#             "session_id": self.session_id,
#             "schema_version": self.schema_version,
#             "stages_executed": len(stages),
#             "total_duration_ms": round(total_duration, 2),
#             "errors": len([e for e in self.entries if e['event'] == 'ERROR'])
#         }

#     def save_logs(self):
#         """Persist logs to JSON file."""
#         log_file = self.log_dir / f"audit_{self.session_id}.json"
#         with open(log_file, 'w') as f:
#             json.dump({
#                 "summary": self.get_summary(),
#                 "entries": self.entries
#             }, f, indent=2)
#         return log_file
"""
Audit Logger - Structured logging for every pipeline mutation.
Provides complete traceability of all data transformations.
"""
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class AuditLogger:
    """
    Centralized audit logging for the atomic pipeline.
    Tracks every mutation with timestamps and metadata.
    """
    
    def __init__(self, log_dir: str = "data/audit_logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.entries: List[Dict] = []
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.pipeline_id: Optional[str] = None
    
    def log_pipeline_start(self, pipeline_id: str, initial_shape: tuple) -> None:
        """Log the start of a pipeline."""
        self.pipeline_id = pipeline_id
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "pipeline_start",
            "pipeline_id": pipeline_id,
            "initial_rows": initial_shape[0],
            "initial_cols": initial_shape[1],
            "session_id": self.session_id
        }
        self.entries.append(entry)
        logger.info(f"Pipeline {pipeline_id} started with shape {initial_shape}")
    
    def log_stage_start(self, stage_name: str, input_shape: tuple) -> None:
        """Log the start of a pipeline stage."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "stage_start",
            "stage": stage_name,
            "input_rows": input_shape[0],
            "input_cols": input_shape[1]
        }
        self.entries.append(entry)
        logger.debug(f"Stage {stage_name} started with shape {input_shape}")
    
    def log_stage_complete(self, stage_name: str, output_shape: tuple, 
                          metadata: Dict[str, Any]) -> None:
        """Log the completion of a pipeline stage."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "stage_complete",
            "stage": stage_name,
            "output_rows": output_shape[0],
            "output_cols": output_shape[1],
            "metadata": metadata
        }
        self.entries.append(entry)
        logger.debug(f"Stage {stage_name} completed with shape {output_shape}")
    
    def log_mutation(self, stage: str, mutation_type: str, 
                    details: Dict[str, Any]) -> None:
        """Log a specific data mutation."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "mutation",
            "stage": stage,
            "mutation_type": mutation_type,
            "details": details
        }
        self.entries.append(entry)
        logger.info(f"Mutation in stage {stage}: {mutation_type} - {details}")
    
    def log_error(self, stage: str, error_type: str, message: str) -> None:
        """Log an error during pipeline execution."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "error",
            "stage": stage,
            "error_type": error_type,
            "message": message
        }
        self.entries.append(entry)
        logger.error(f"Error in stage {stage}: {error_type} - {message}")
    
    def log_validation_result(self, passed: bool, issues: List[str], 
                             warnings: List[str]) -> None:
        """Log validation results."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "validation",
            "passed": passed,
            "issues": issues,
            "warnings": warnings
        }
        self.entries.append(entry)
        logger.info(f"Validation {'passed' if passed else 'failed'} with {len(issues)} issues, {len(warnings)} warnings")
    
    def log_pipeline_complete(self, final_shape: tuple, status: str, 
                             quality_score: float) -> None:
        """Log pipeline completion."""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "event": "pipeline_complete",
            "final_rows": final_shape[0],
            "final_cols": final_shape[1],
            "status": status,
            "quality_score": quality_score,
            "total_entries": len(self.entries)
        }
        self.entries.append(entry)
        logger.info(f"Pipeline completed with status {status}, quality {quality_score}")
    
    def get_audit_trail(self) -> List[Dict]:
        """Get the complete audit trail."""
        return self.entries.copy()
    
    def save_to_file(self, filename: Optional[str] = None) -> Path:
        """Save audit log to JSON file."""
        if filename is None:
            if self.pipeline_id:
                filename = f"audit_{self.pipeline_id}_{self.session_id}.json"
            else:
                filename = f"audit_{self.session_id}.json"
        
        filepath = self.log_dir / filename
        
        audit_data = {
            "session_id": self.session_id,
            "pipeline_id": self.pipeline_id,
            "created_at": datetime.now().isoformat(),
            "total_entries": len(self.entries),
            "entries": self.entries
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(audit_data, f, indent=2, default=str)
        
        logger.info(f"Audit log saved to {filepath}")
        return filepath
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the audit log."""
        stages_executed = [e for e in self.entries if e['event'] == 'stage_complete']
        mutations = [e for e in self.entries if e['event'] == 'mutation']
        errors = [e for e in self.entries if e['event'] == 'error']
        
        # Calculate execution time
        start_time = None
        end_time = None
        
        for entry in self.entries:
            if entry['event'] == 'pipeline_start':
                start_time = datetime.fromisoformat(entry['timestamp'])
            elif entry['event'] == 'pipeline_complete':
                end_time = datetime.fromisoformat(entry['timestamp'])
        
        execution_time = None
        if start_time and end_time:
            execution_time = (end_time - start_time).total_seconds()
        
        return {
            "session_id": self.session_id,
            "pipeline_id": self.pipeline_id,
            "total_entries": len(self.entries),
            "stages_executed": len(stages_executed),
            "mutations_logged": len(mutations),
            "errors_logged": len(errors),
            "stage_names": [s['stage'] for s in stages_executed],
            "execution_time_seconds": execution_time,
            "status": self.entries[-1]['status'] if self.entries else 'unknown'
        }
    
    def clear(self) -> None:
        """Clear all log entries."""
        self.entries = []
        self.pipeline_id = None
        logger.info("Audit log cleared")
    
    def __repr__(self) -> str:
        return f"AuditLogger(session={self.session_id}, entries={len(self.entries)})"