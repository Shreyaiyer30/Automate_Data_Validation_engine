import pandas as pd
from typing import Dict, Any, Tuple
from src.engine.stages.base_stage import BaseStage, StageState

class DuplicatesStage(BaseStage):
    """
    Stage to identify and remove duplicate records.
    Provides detailed before/after counts for the audit trail.
    """
    @property
    def name(self) -> str:
        return "DUPLICATES"

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, StageState]:
        # Determine if we should actually remove or just flag
        remove_enabled = config.get("remove_duplicates", False) or config.get("destructive_row_deletion", False)
        
        # Check if specific key columns are provided for duplicate detection
        subset = config.get("cleaning", {}).get("duplicate_keys", None)
        
        dup_count = int(df.duplicated(subset=subset).sum())
        state = StageState.PASS
        
        if dup_count > 0:
            if remove_enabled:
                df = df.drop_duplicates(subset=subset)
                self.logger.log_mutation(self.name, "drop_duplicates", {
                    "count": dup_count,
                    "subset_used": subset or "full_row"
                })
                state = StageState.WARN
            else:
                self.logger.log_mutation(self.name, "duplicate_detection", {
                    "count": dup_count,
                    "note": "Duplicates preserved (deletion disabled)"
                })
                state = StageState.WARN
            
        return df, state
