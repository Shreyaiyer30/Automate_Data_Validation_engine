from typing import List, Dict, Any
import pandas as pd
from src.engine.audit.audit_logger import AuditLogger
from src.engine.stages import (
    DetectTypesStage,
    CleanDataStage,
    SchemaCheckStage,
    DuplicatesStage,
    MissingValuesStage,
    OutliersStage
)

class LifecycleManager:
    """
    Manages the order and execution of pipeline stages.
    """
    def __init__(self, logger: AuditLogger):
        self.logger = logger
        # Define the canonical pipeline order
        self.stages = [
            SchemaCheckStage(logger),
            DetectTypesStage(logger),
            CleanDataStage(logger),
            DuplicatesStage(logger),
            MissingValuesStage(logger),
            OutliersStage(logger)
        ]

    def run_pipeline(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Execute all registered stages in order, respecting FAIL semantics."""
        from src.engine.stages.base_stage import StageState
        
        for stage in self.stages:
            # Check if stage is enabled in config
            # stages config can be a List or a Dict
            stage_config = config.get("stages", {})
            
            # If config is empty (test mode) and we didn't explicitly include it, skip elective stages
            # For backward compatibility, if 'stages' is missing, run all.
            if isinstance(stage_config, list):
                if stage.name not in stage_config:
                    continue
            elif isinstance(stage_config, dict) and stage_config:
                if stage.name in stage_config and not stage_config[stage.name].get("enabled", True):
                    continue
                if stage.name not in stage_config:
                    continue
            
            try:
                # Stages now return (df, state)
                df, state = stage.run(df, config)
                if state == StageState.FAIL:
                    self.logger.log_error(stage.name, "PIPELINE_BLOCKED", f"Pipeline execution halted due to FAIL state in {stage.name}")
                    break
            except Exception as e:
                # Log error and stop execution for safety
                self.logger.log_error(stage.name, "STAGE_FAILURE", f"Stage failed unexpectedly: {str(e)}")
                break
        return df