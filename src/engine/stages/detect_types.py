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

    def execute(self, df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, StageState]:
        """Detect types using pandas inference."""
        from src.engine.stages.base_stage import StageState
        df = df.copy()
        state = StageState.PASS
        
        for col in df.columns:
            # 1. Handle potential numeric text
            if df[col].dtype == 'object':
                # Sanitize: Remove quotes, commas, symbols (preserving decimal points, negative signs, and scientific notation)
                # Now also handles currency symbols like $, £, €, ₹
                sanitized = df[col].astype(str).str.replace(r'[^\d\.\-eE+]', '', regex=True)
                
                # Filter out empty strings from sanitization
                sanitized = sanitized.replace('', pd.NA)
                
                # Attempt conversion
                try:
                    numeric_series = pd.to_numeric(sanitized, errors='coerce')
                    
                    # Confidence Check: Only convert if > 10% of values are actually numeric
                    if not numeric_series.isna().all() and (numeric_series.notna().mean() > 0.1):
                        df[col] = numeric_series
                        state = StageState.WARN
                        self.logger.log_mutation(self.name, "numeric_conversion", {
                            "column": col,
                            "note": "Textual numerics (including symbols/currency) sanitized and converted"
                        })
                        continue
                except:
                    pass
                
            # 2. Attempt datetime conversion
            try:
                # Try to convert only if it looks like a date string
                if df[col].dtype == 'object':
                    date_series = pd.to_datetime(df[col], errors='coerce')
                    if not date_series.isna().all():
                        df[col] = date_series
                        state = StageState.WARN
                        self.logger.log_mutation(self.name, "cast_datetime", {"column": col})
                        continue
            except (ValueError, TypeError):
                pass
                
        return df.infer_objects(), state