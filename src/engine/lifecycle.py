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
            DetectTypesStage(logger),
            CleanDataStage(logger),
            SchemaCheckStage(logger),
            DuplicatesStage(logger),
            MissingValuesStage(logger),
            OutliersStage(logger)
        ]

    def run_pipeline(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Execute all registered stages in order."""
        for stage in self.stages:
            try:
                df = stage.run(df, config)
            except Exception as e:
                # Log error and continue to next stage if possible
                self.logger.log_error(stage.name, "STAGE_FAILURE", f"Stage failed: {str(e)}")
                continue
        return df