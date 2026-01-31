import pandas as pd
from typing import Dict, Any
from src.engine.stages.base_stage import BaseStage

class DuplicatesStage(BaseStage):
    """
    Stage to identify and remove duplicate records.
    Provides detailed before/after counts for the audit trail.
    """
    @property
    def name(self) -> str:
        return "DUPLICATES"

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        rows_before = len(df)
        
        # Check if specific key columns are provided for duplicate detection
        subset = config.get("cleaning", {}).get("duplicate_keys", None)
        
        df_cleaned = df.drop_duplicates(subset=subset)
        rows_dropped = rows_before - len(df_cleaned)
        
        if rows_dropped > 0:
            self.logger.log_mutation(self.name, "drop_duplicates", {
                "count": rows_dropped,
                "subset_used": subset or "full_row"
            })
            
        return df_cleaned
