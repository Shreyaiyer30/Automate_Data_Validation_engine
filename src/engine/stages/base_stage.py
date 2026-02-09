import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple
from enum import Enum
from src.engine.audit.audit_logger import AuditLogger

class StageState(Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"

class BaseStage(ABC):
    """
    Abstract base class for all pipeline stages.
    Ensures unified contract and audit integration.
    """
    def __init__(self, logger: AuditLogger):
        self.logger = logger
        self.state = StageState.PASS

    @property
    @abstractmethod
    def name(self) -> str:
        """String identifier for the stage."""
        pass

    @abstractmethod
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, StageState]:
        """Main execution logic for the stage. Must return (DataFrame, state)."""
        pass

    def run(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, StageState]:
        """Wrapper to handle audit logging for the stage and enforce System Contracts."""
        initial_shape = df.shape
        initial_cols = list(df.columns)
        self.logger.log_stage_start(self.name, initial_shape)
        
        try:
            df, state = self.execute(df, config)
            
            # --- AUTHORITATIVE CONTRACT ENFORCEMENT ---
            # 1. Schema Integrity Rule: Columns must match exactly
            if list(df.columns) != initial_cols:
                self.logger.log_error(self.name, "SCHEMA_VIOLATION", 
                                     f"Columns changed from {initial_cols} to {list(df.columns)}. Aborting.", critical=True)
                return df, StageState.FAIL
                
            # 2. Row Preservation Rule: Unexpected row deletion
            if len(df) != initial_shape[0] and not config.get('destructive_row_deletion', False):
                self.logger.log_error(self.name, "UNAUTHORIZED_DATA_LOSS", 
                                     f"Row count changed from {initial_shape[0]} to {len(df)} without 'destructive_row_deletion' flag.", critical=True)
                return df, StageState.FAIL
            
            self.state = state
            self.logger.log_stage_complete(self.name, df.shape, metadata={"state": state.value})
            
            if state == StageState.FAIL:
                self.logger.log_error(self.name, "Critical failure in stage", critical=True)
            elif state == StageState.WARN:
                self.logger.log_mutation(self.name, "Warning issued during stage execution", {"status": "WARN"})
                
            return df, state
        except Exception as e:
            self.logger.log_error(self.name, f"Execution failed: {str(e)}", critical=True)
            self.state = StageState.FAIL
            return df, StageState.FAIL