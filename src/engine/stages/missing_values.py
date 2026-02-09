import pandas as pd
from typing import Dict, Any
from src.engine.stages.base_stage import BaseStage

class MissingValuesStage(BaseStage):
    """
    Stage to handle missing values with production-grade strategies.
    - Numeric -> median
    - Categorical -> mode
    - Datetime -> forward fill
    """
    @property
    def name(self) -> str:
        return "MISSING_VALUES"

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, StageState]:
        from src.engine.stages.base_stage import StageState
        df = df.copy()
        thresholds = config.get("thresholds", {})
        max_missing_row_pct = thresholds.get("max_missing_row_percentage", 50.0)
        
        # 1. Handle rows exceeding missingness threshold
        missing_row_pct = df.isnull().mean(axis=1) * 100
        rows_to_drop = missing_row_pct > max_missing_row_pct
        drop_count = int(rows_to_drop.sum())
        
        state = StageState.PASS
        if drop_count > 0:
            if config.get('destructive_row_deletion', False):
                df = df[~rows_to_drop]
                self.logger.log_mutation(self.name, "row_deletion", {
                    "count": drop_count,
                    "reason": f"Missingness > {max_missing_row_pct}%"
                })
                state = StageState.WARN
            else:
                self.logger.log_mutation(self.name, "row_preservation_bypass", {
                    "count": drop_count,
                    "reason": f"Threshold {max_missing_row_pct}% exceeded but deletion disabled."
                })
                state = StageState.WARN

        # 2. Impute with type-specific strategies
        for col in df.columns:
            if df[col].isnull().any():
                state = StageState.WARN
                count_before = int(df[col].isnull().sum())
                
                # Check column type
                if pd.api.types.is_numeric_dtype(df[col]):
                    strategy = "median"
                    fill_value = df[col].median()
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    strategy = "ffill"
                    df[col] = df[col].ffill()
                    self.logger.log_mutation(self.name, "impute", {"column": col, "strategy": strategy, "count": count_before})
                    continue
                else:
                    strategy = "mode"
                    fill_value = df[col].mode().iloc[0] if not df[col].mode().empty else "Unknown"
                
                df[col] = df[col].fillna(fill_value)
                self.logger.log_mutation(self.name, "impute", {
                    "column": col,
                    "strategy": strategy,
                    "count": count_before
                })
        
        return df, state
