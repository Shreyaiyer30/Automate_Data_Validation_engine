import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any


# ── Quick Stats (used by upload.py) ──────────────────────────────────────────

def get_quick_stats(df: pd.DataFrame) -> dict:
    """
    Return a lightweight summary dict suitable for the upload preview card.
    Runs fast — no per-column deep analysis.
    """
    if df is None or df.empty:
        return {
            "rows": 0, "cols": 0, "missing_cells": 0, "missing_pct": 0.0,
            "duplicate_rows": 0, "numeric_cols": 0, "text_cols": 0,
            "date_cols": 0, "bool_cols": 0, "memory_mb": 0.0,
            "completeness_pct": 0.0,
        }

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    text_cols    = df.select_dtypes(include=["object", "string"]).columns.tolist()
    date_cols    = df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    bool_cols    = df.select_dtypes(include=["bool"]).columns.tolist()

    total_cells   = df.size or 1
    missing_cells = int(df.isnull().sum().sum())
    missing_pct   = round(missing_cells / total_cells * 100, 2)
    completeness  = round(100 - missing_pct, 2)

    return {
        "rows":             len(df),
        "cols":             len(df.columns),
        "missing_cells":    missing_cells,
        "missing_pct":      missing_pct,
        "completeness_pct": completeness,
        "duplicate_rows":   int(df.duplicated().sum()),
        "numeric_cols":     len(numeric_cols),
        "text_cols":        len(text_cols),
        "date_cols":        len(date_cols),
        "bool_cols":        len(bool_cols),
        "memory_mb":        round(df.memory_usage(deep=True).sum() / 1024 ** 2, 2),
    }


# ── Column-Level Profile ──────────────────────────────────────────────────────

def profile_column(series: pd.Series) -> dict:
    """
    Return a detailed profile dict for a single column/Series.
    Safe — handles all dtypes without raising.
    """
    result: dict[str, Any] = {
        "name":       series.name,
        "dtype":      str(series.dtype),
        "count":      len(series),
        "null_count": int(series.isnull().sum()),
        "null_pct":   round(series.isnull().mean() * 100, 2),
        "unique":     int(series.nunique(dropna=True)),
        "unique_pct": round(series.nunique(dropna=True) / max(len(series), 1) * 100, 2),
    }

    non_null = series.dropna()

    # Numeric
    if pd.api.types.is_numeric_dtype(series):
        result.update({
            "inferred_type": "numeric",
            "mean":   round(float(non_null.mean()), 4) if len(non_null) else None,
            "median": round(float(non_null.median()), 4) if len(non_null) else None,
            "std":    round(float(non_null.std()), 4) if len(non_null) > 1 else None,
            "min":    float(non_null.min()) if len(non_null) else None,
            "max":    float(non_null.max()) if len(non_null) else None,
            "q25":    float(non_null.quantile(0.25)) if len(non_null) else None,
            "q75":    float(non_null.quantile(0.75)) if len(non_null) else None,
            "skew":   round(float(non_null.skew()), 4) if len(non_null) > 2 else None,
            "zeros":  int((non_null == 0).sum()),
            "negatives": int((non_null < 0).sum()),
        })

    # Datetime
    elif pd.api.types.is_datetime64_any_dtype(series):
        result.update({
            "inferred_type": "datetime",
            "min_date": str(non_null.min()) if len(non_null) else None,
            "max_date": str(non_null.max()) if len(non_null) else None,
            "range_days": (non_null.max() - non_null.min()).days if len(non_null) >= 2 else None,
        })

    # Boolean
    elif pd.api.types.is_bool_dtype(series):
        vc = non_null.value_counts()
        result.update({
            "inferred_type": "boolean",
            "true_count":  int(vc.get(True, 0)),
            "false_count": int(vc.get(False, 0)),
        })

    # Text / categorical
    else:
        vc = non_null.value_counts()
        result.update({
            "inferred_type":  "text" if result["unique"] > 20 else "categorical",
            "top_values":     vc.head(5).to_dict(),
            "avg_length":     round(non_null.astype(str).str.len().mean(), 1) if len(non_null) else None,
            "max_length":     int(non_null.astype(str).str.len().max()) if len(non_null) else None,
            "min_length":     int(non_null.astype(str).str.len().min()) if len(non_null) else None,
            "has_whitespace": bool(non_null.astype(str).str.contains(r"^\s|\s$", regex=True).any()),
            "has_special_chars": bool(non_null.astype(str).str.contains(r"[^a-zA-Z0-9\s]", regex=True).any()),
        })

    return result


# ── Full Dataset Profile ──────────────────────────────────────────────────────

def profile_dataframe(df: pd.DataFrame) -> dict:
    """
    Return a complete profile of the entire DataFrame.
    Includes dataset-level stats and per-column profiles.
    """
    if df is None or df.empty:
        return {"error": "Empty or null DataFrame provided."}

    quick   = get_quick_stats(df)
    columns = {}

    for col in df.columns:
        try:
            columns[col] = profile_column(df[col])
        except Exception as e:
            columns[col] = {"error": str(e)}

    # Correlation matrix for numeric columns (top 10 max)
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    correlation  = None
    if len(numeric_cols) >= 2:
        try:
            corr_cols   = numeric_cols[:10]
            correlation = df[corr_cols].corr().round(3).to_dict()
        except Exception:
            correlation = None

    # Constant columns
    constant_cols = [
        col for col in df.columns
        if df[col].dropna().nunique() <= 1
    ]

    # High-null columns (>= 50%)
    high_null_cols = [
        col for col in df.columns
        if df[col].isnull().mean() >= 0.5
    ]

    # High-cardinality text columns (unique > 95% of rows)
    high_card_cols = [
        col for col in df.select_dtypes(include=["object", "string"]).columns
        if df[col].nunique() / max(len(df), 1) > 0.95
    ]

    return {
        "generated_at":     datetime.now().isoformat(),
        "quick_stats":      quick,
        "columns":          columns,
        "correlation":      correlation,
        "constant_cols":    constant_cols,
        "high_null_cols":   high_null_cols,
        "high_card_cols":   high_card_cols,
        "warnings":         _generate_warnings(df, constant_cols, high_null_cols, quick),
    }


# ── Warning Generator ─────────────────────────────────────────────────────────

def _generate_warnings(df: pd.DataFrame, constant_cols: list,
                        high_null_cols: list, quick: dict) -> list[dict]:
    """Generate a list of actionable data quality warnings."""
    warnings = []

    if quick["duplicate_rows"] > 0:
        warnings.append({
            "level":   "warning",
            "code":    "DUPLICATE_ROWS",
            "message": f"{quick['duplicate_rows']:,} duplicate rows detected.",
            "action":  "Consider deduplication before analysis.",
        })

    if quick["missing_pct"] > 20:
        warnings.append({
            "level":   "warning",
            "code":    "HIGH_MISSING",
            "message": f"{quick['missing_pct']}% of cells are missing.",
            "action":  "Review missing value imputation strategy.",
        })

    if constant_cols:
        warnings.append({
            "level":   "info",
            "code":    "CONSTANT_COLUMNS",
            "message": f"{len(constant_cols)} constant column(s): {', '.join(constant_cols[:5])}.",
            "action":  "These columns carry no information and can be dropped.",
        })

    if high_null_cols:
        warnings.append({
            "level":   "warning",
            "code":    "HIGH_NULL_COLUMNS",
            "message": f"{len(high_null_cols)} column(s) have >= 50% missing values.",
            "action":  "Consider dropping or imputing: " + ", ".join(high_null_cols[:5]),
        })

    if quick["memory_mb"] > 500:
        warnings.append({
            "level":   "info",
            "code":    "LARGE_DATASET",
            "message": f"Dataset is {quick['memory_mb']} MB in memory.",
            "action":  "Consider chunked processing for large datasets.",
        })

    return warnings


# ── Comparison Profile (Raw vs Clean) ────────────────────────────────────────

def compare_profiles(raw_df: pd.DataFrame, clean_df: pd.DataFrame) -> dict:
    """
    Compare quick stats between raw and cleaned DataFrames.
    Returns a diff summary useful for the export/report page.
    """
    raw   = get_quick_stats(raw_df)
    clean = get_quick_stats(clean_df)

    return {
        "rows_removed":      raw["rows"] - clean["rows"],
        "cols_removed":      raw["cols"] - clean["cols"],
        "missing_reduction": round(raw["missing_cells"] - clean["missing_cells"], 0),
        "missing_pct_before": raw["missing_pct"],
        "missing_pct_after":  clean["missing_pct"],
        "completeness_gain":  round(clean["completeness_pct"] - raw["completeness_pct"], 2),
        "duplicates_before":  raw["duplicate_rows"],
        "duplicates_after":   clean["duplicate_rows"],
        "memory_before_mb":   raw["memory_mb"],
        "memory_after_mb":    clean["memory_mb"],
    }

# ── Alias for overview.py compatibility ──────────────────────────────────────

def get_column_profiles(df: pd.DataFrame) -> list[dict]:
    """
    Return a list of per-column profile dicts.
    Alias used by overview.py — wraps profile_column for each column.
    """
    profiles = []
    for col in df.columns:
        try:
            p = profile_column(df[col])
            profiles.append(p)
        except Exception as e:
            profiles.append({
                "name":          col,
                "dtype":         str(df[col].dtype),
                "error":         str(e),
                "inferred_type": "unknown",
            })
    return profiles