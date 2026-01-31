import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any
from src.engine.audit.audit_logger import AuditLogger

class BaseStage(ABC):
    """
    Abstract base class for all pipeline stages.
    Ensures unified contract and audit integration.
    """
    def __init__(self, logger: AuditLogger):
        self.logger = logger

    @property
    @abstractmethod
    def name(self) -> str:
        """String identifier for the stage."""
        pass

    @abstractmethod
    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Main execution logic for the stage."""
        pass

    def run(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Wrapper to handle audit logging for the stage."""
        self.logger.log_stage_start(self.name, df.shape)
        try:
            df = self.execute(df, config)
            self.logger.log_stage_complete(self.name, df.shape)
        except Exception as e:
            self.logger.log_error(self.name, f"Execution failed: {str(e)}", critical=False)
            raise
        return df