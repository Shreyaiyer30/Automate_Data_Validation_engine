import pandas as pd
from typing import Dict, Any
from src.engine.stages.base_stage import BaseStage

class CleanDataStage(BaseStage):
    """
    Stage for production-grade text normalization.
    - Trim spaces
    - Case normalization
    - Fixing inconsistent categories
    """
    @property
    def name(self) -> str:
        return "CLEAN_DATA"

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        df = df.copy()
        text_config = config.get("cleaning", {}).get("text", {})
        case_mode = text_config.get("case", "none") # none, upper, lower, title
        
        for col in df.select_dtypes(include=['object']).columns:
            # 1. Trim
            df[col] = df[col].astype(str).str.strip()
            
            # 2. Case Normalization
            if case_mode == "upper":
                df[col] = df[col].str.upper()
            elif case_mode == "lower":
                df[col] = df[col].str.lower()
            elif case_mode == "title":
                df[col] = df[col].str.title()
                
            # 3. Standardize Nulls
            df[col] = df[col].replace({'nan': pd.NA, 'None': pd.NA, '': pd.NA})
            
            self.logger.log_mutation(self.name, "text_normalization", {
                "column": col,
                "case": case_mode,
                "trimmed": True
            })
            
        return df
