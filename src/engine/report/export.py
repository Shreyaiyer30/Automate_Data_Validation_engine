import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any

class DataExporter:
    """Handles various export formats for cleaned data and reports."""
    
    @staticmethod
    def to_csv(df: pd.DataFrame, path: Path):
        df.to_csv(path, index=False)
        
    @staticmethod
    def to_excel(df: pd.DataFrame, path: Path):
        df.to_excel(path, index=False)
        
    @staticmethod
    def to_json(data: Dict[str, Any], path: Path):
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)