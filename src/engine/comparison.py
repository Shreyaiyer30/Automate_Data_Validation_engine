import pandas as pd
import numpy as np
from typing import Dict, List, Any

def compare_datasets(df_raw: pd.DataFrame, df_clean: pd.DataFrame) -> Dict[str, Any]:
    """
    Computes cell-level differences and distribution shifts between two datasets.
    """
    shared_cols = [c for c in df_raw.columns if c in df_clean.columns]
    min_rows = min(len(df_raw), len(df_clean))
    
    diff_rows = []
    total_changed = 0
    
    for col in shared_cols:
        # Fill nulls with a placeholder for comparison
        raw_s = df_raw[col].iloc[:min_rows].fillna("__NULL__").astype(str)
        clean_s = df_clean[col].iloc[:min_rows].fillna("__NULL__").astype(str)
        
        # Compare by position
        changed = int((raw_s.to_numpy() != clean_s.to_numpy()).sum())
        total_changed += changed
        diff_rows.append({
            "Column": col,
            "Changed Cells": changed,
            "Change %": f"{changed/min_rows*100:.1f}%" if min_rows > 0 else "0%"
        })
        
    total_cells = min_rows * len(shared_cols)
    pct = (total_changed / total_cells * 100) if total_cells > 0 else 0
    
    return {
        "column_diffs": diff_rows,
        "total_changed_cells": total_changed,
        "total_cells_compared": total_cells,
        "total_change_pct": round(pct, 2),
        "row_delta": len(df_clean) - len(df_raw),
        "col_delta": len(df_clean.columns) - len(df_raw.columns)
    }

def get_distribution_data(df: pd.DataFrame, column: str) -> pd.Series:
    """Returns a cleaned series for distribution plotting."""
    if column not in df.columns:
        return pd.Series(dtype=float)
    return df[column].dropna()
