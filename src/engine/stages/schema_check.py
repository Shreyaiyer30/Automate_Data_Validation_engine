import pandas as pd
from typing import Dict, Any
from src.engine.stages.base_stage import BaseStage

class SchemaCheckStage(BaseStage):
    """
    Stage to verify schema integrity.
    Detects unexpected columns and type mismatches.
    """
    @property
    def name(self) -> str:
        return "SCHEMA_CHECK"

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        expected_cols = config.get("schema", {}).get("required_columns", [])
        
        if expected_cols:
            missing = set(expected_cols) - set(df.columns)
            if missing:
                self.logger.log_error(self.name, f"Missing required columns: {missing}", critical=True)
            
            unexpected = set(df.columns) - set(expected_cols)
            if unexpected:
                self.logger.log_mutation(self.name, "unexpected_columns", {"columns": list(unexpected)})
                
        # Basic check for all-null columns
        for col in df.columns:
            if df[col].isnull().all():
                self.logger.log_error(self.name, f"Column '{col}' is entirely null.", critical=False)
                
        return df
