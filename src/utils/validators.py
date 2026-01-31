import pandas as pd
from typing import List, Optional

def validate_dataframe_input(df: pd.DataFrame) -> bool:
    """Ensure dataframe is valid for processing."""
    if df is None or df.empty:
        return False
    return True

def find_duplicate_columns(df: pd.DataFrame) -> List[str]:
    """Return names of duplicate columns."""
    return df.columns[df.columns.duplicated()].tolist()