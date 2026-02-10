import pandas as pd
import json
import numpy as np
from pathlib import Path
from typing import Dict, Any

class DataExporter:
    """Handles various export formats for cleaned data and reports."""
    
    @staticmethod
    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        df_export = df.copy()
        # Handle object columns: replace string placeholders with NaN
        cols = df_export.select_dtypes(include=['object']).columns
        if len(cols) > 0:
            df_export[cols] = df_export[cols].replace(
                to_replace=r'^(?i)(nan|unknown|null|none|n/a|\s*)$', 
                value=np.nan, regex=True
            )
        return df_export

    @staticmethod
    def to_csv(df: pd.DataFrame, path: Path):
        DataExporter._prepare_df(df).to_csv(path, index=False)
        
    @staticmethod
    def to_excel(df: pd.DataFrame, path: Path):
        DataExporter._prepare_df(df).to_excel(path, index=False)
        
    @staticmethod
    def to_json(data: Dict[str, Any], path: Path):
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)