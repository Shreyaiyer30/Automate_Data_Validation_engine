import pandas as pd
import numpy as np
from typing import Dict, Any
from src.engine.stages.base_stage import BaseStage

class OutliersStage(BaseStage):
    """
    Stage to detect and handle outliers in numeric columns.
    """
    @property
    def name(self) -> str:
        return "OUTLIERS"

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, StageState]:
        from src.engine.stages.base_stage import StageState
        df = df.copy()
        method = config.get("thresholds", {}).get("outlier_method", "iqr")
        state = StageState.PASS
        
        for col in df.select_dtypes(include=[np.number]).columns:
            if method == "iqr":
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
            else: # z-score fallback
                mean = df[col].mean()
                std = df[col].std()
                if std == 0: continue
                lower_bound = mean - 3 * std
                upper_bound = mean + 3 * std
            
            outliers_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
            outlier_count = outliers_mask.sum()
            
            if outlier_count > 0:
                state = StageState.WARN
                # For this stage, we clip outliers to the bounds instead of dropping
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
                self.logger.log_mutation(self.name, "clip_outliers", {
                    "column": col,
                    "count": int(outlier_count),
                    "method": method
                })
                
        return df, state
