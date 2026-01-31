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

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        thresholds = config.get("thresholds", {})
        max_missing_row_pct = thresholds.get("max_missing_row_percentage", 50.0)
        
        # 1. Drop rows exceeding threshold
        missing_row_pct = df.isnull().mean(axis=1) * 100
        rows_before = len(df)
        df = df[missing_row_pct <= max_missing_row_pct]
        rows_dropped = rows_before - len(df)
        
        if rows_dropped > 0:
            self.logger.log_mutation(self.name, "row_deletion", {
                "count": rows_dropped,
                "reason": f"Missingness > {max_missing_row_pct}%"
            })

        # 2. Impute with type-specific strategies
        for col in df.columns:
            if df[col].isnull().any():
                count_before = int(df[col].isnull().sum())
                
                # Check column type
                if pd.api.types.is_numeric_dtype(df[col]):
                    strategy = "median"
                    fill_value = df[col].median()
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    strategy = "ffill"
                    df[col] = df[col].fillna(method='ffill')
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
        
        return df
