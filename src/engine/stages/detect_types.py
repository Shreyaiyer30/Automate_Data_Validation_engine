import pandas as pd
from typing import Dict, Any
from src.engine.stages.base_stage import BaseStage

class DetectTypesStage(BaseStage):
    """
    Pipeline stage to automatically detect and cast column types.
    """
    @property
    def name(self) -> str:
        return "TYPE_DETECTION"

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """Detect types using pandas inference."""
        df = df.copy()
        for col in df.columns:
            # 1. Attempt numeric conversion
            try:
                numeric_series = pd.to_numeric(df[col], errors='ignore')
                if pd.api.types.is_numeric_dtype(numeric_series):
                    df[col] = numeric_series
                    self.logger.log_mutation(self.name, "cast_numeric", {"column": col})
                    continue
            except (ValueError, TypeError):
                pass
                
            # 2. Attempt datetime conversion
            try:
                # Try to convert only if it looks like a date string
                if df[col].dtype == 'object':
                    date_series = pd.to_datetime(df[col], errors='coerce')
                    if not date_series.isna().all():
                        df[col] = date_series
                        self.logger.log_mutation(self.name, "cast_datetime", {"column": col})
                        continue
            except (ValueError, TypeError):
                pass
                
        return df.infer_objects()