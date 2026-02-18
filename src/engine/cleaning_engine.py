import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, List, Dict, Any, Union

def clean_dataset(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """
    Main entry point for the enterprise-grade cleaning engine.
    Applies 10 stages of cleaning and returns a Tuple of (cleaned_df, audit_trail).
    """
    audit_trail = []
    current_df = df.copy()
    
    # Initial state
    before_shape = current_df.shape
    
    # 1. Column-Level Cleaning (Standardize Headers)
    current_df, log_entry = _stage_column_cleanup(current_df, config.get("column_cleanup", {}))
    audit_trail.append(log_entry)
    
    # 2. Numeric Cleaning (Standardize first)
    current_df, log_entry = _stage_numeric(current_df, config.get("numeric", {}))
    audit_trail.append(log_entry)
    
    # 3. Text & Categorical Cleaning
    current_df, log_entry = _stage_text_categorical(current_df, config.get("text_categorical", {}))
    audit_trail.append(log_entry)
    
    # 4. Boolean Standardization
    current_df, log_entry = _stage_boolean(current_df, config.get("boolean", {}))
    audit_trail.append(log_entry)
    
    # 5. Date Parsing & Cleaning
    current_df, log_entry = _stage_dates(current_df, config.get("dates", {}))
    audit_trail.append(log_entry)

    # 6. Duplicate Handling (Now safe with standardized types)
    current_df, log_entry = _stage_duplicates(current_df, config.get("duplicates", {}))
    audit_trail.append(log_entry)
    
    # 7. Range-Based Cleaning
    current_df, log_entry = _stage_ranges(current_df, config.get("ranges", {}))
    audit_trail.append(log_entry)
    
    # 8. Missing Value Handling
    current_df, log_entry = _stage_missing_values(current_df, config.get("missing_values", {}))
    audit_trail.append(log_entry)
    
    # 9. Scaling and Normalization
    current_df, log_entry = _stage_scaling(current_df, config.get("scaling", {}))
    audit_trail.append(log_entry)
    
    # 10. Audit & Final Summary
    retention_rate = (len(current_df) / len(df)) * 100 if len(df) > 0 else 0
    final_log = {
        "stage": "final_summary",
        "timestamp": datetime.now().isoformat(),
        "total_actions": sum(1 for e in audit_trail if e.get("cells_changed", 0) > 0 or e.get("rows_affected", 0) > 0),
        "retention_rate": f"{retention_rate:.1f}%",
        "before_shape": before_shape,
        "after_shape": current_df.shape
    }
    audit_trail.append(final_log)
    
    return current_df, audit_trail

# --- Stage 1: Column-Level Cleaning ---
def _stage_column_cleanup(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    desc = "Standarding headers and removing constant columns."
    before_df = df.copy()
    
    # Header standardization
    strategy = config.get("header_case", "Title Case") # Title Case, Snake Case
    
    new_cols = []
    mapping = {}
    for col in df.columns:
        c = str(col).strip()
        # Remove special chars but keep space/underscore for now
        import re
        c = re.sub(r'[^a-zA-Z0-9\s_]', '', c)
        c = c.replace('_', ' ')
        
        if strategy == "Snake Case":
            c = "_".join(c.lower().split())
        else: # Default Title Case
            c = " ".join([word.capitalize() for word in c.split()])
        
        mapping[col] = c
        new_cols.append(c)
    
    df.columns = new_cols
    
    # Remove constant columns
    dropped_const = []
    if config.get("remove_constant", True):
        for col in df.columns:
            if df[col].nunique() <= 1:
                dropped_const.append(col)
        df = df.drop(columns=dropped_const)
        
    return df, {
        "stage": "column_level_cleaning",
        "description": desc,
        "renamed_mapping": mapping,
        "removed_constant_columns": dropped_const,
        "rows_affected": 0,
        "cells_changed": 0,
        "timestamp": datetime.now().isoformat()
    }

# --- Stage 2: Duplicate Handling ---
def _stage_duplicates(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    before_rows = len(df)
    cols_removed = []
    
    # Column Duplication
    if config.get("remove_duplicate_cols", True):
        deduped_cols = df.loc[:, ~df.columns.duplicated()]
        # Value-based col duplication (strict)
        to_drop = []
        for i in range(len(deduped_cols.columns)):
            for j in range(i + 1, len(deduped_cols.columns)):
                col1 = deduped_cols.columns[i]
                col2 = deduped_cols.columns[j]
                if deduped_cols[col1].equals(deduped_cols[col2]):
                    to_drop.append(col2)
        df = deduped_cols.drop(columns=list(set(to_drop)))
        cols_removed = list(set(to_drop))

    # Row Duplication (Exact)
    row_strat = config.get("row_strategy", "first") # first, last, drop_all
    keep = row_strat if row_strat in ["first", "last"] else False
    
    # Key-based or total
    key_cols = config.get("key_columns", [])
    if key_cols:
        # Latest record by timestamp if provided
        ts_col = config.get("timestamp_column")
        if ts_col and ts_col in df.columns:
            df = df.sort_values(ts_col).drop_duplicates(subset=key_cols, keep="last")
        else:
            df = df.drop_duplicates(subset=key_cols, keep=keep)
    else:
        df = df.drop_duplicates(keep=keep)
        
    rows_removed = before_rows - len(df)
    
    return df, {
        "stage": "duplicate_handling",
        "rows_affected": rows_removed,
        "columns_removed": cols_removed,
        "cells_changed": 0,
        "timestamp": datetime.now().isoformat()
    }

# --- Stage 3: Numeric Cleaning ---
def _stage_numeric(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cols = config.get("columns", df.select_dtypes(include=[np.number, 'object']).columns.tolist())
    cells_changed = 0
    invalid_count = 0
    
    for col in cols:
        if col not in df.columns: continue
        
        # If object, clean currency and commas
        if df[col].dtype == 'object':
            original = df[col].copy()
            # Remove symbols: ₹, $, €, £
            df[col] = df[col].astype(str).str.replace(r'[₹$€£,]', '', regex=True)
            # Try convert
            df[col] = pd.to_numeric(df[col], errors="coerce")
            
            invalid_count += df[col].isna().sum() - original.isna().sum()
            cells_changed += (df[col] != original).sum()
            
    return df, {
        "stage": "numeric_cleaning",
        "columns_converted": cols,
        "invalid_conversions_count": int(invalid_count),
        "cells_changed": int(cells_changed),
        "timestamp": datetime.now().isoformat()
    }

# --- Stage 4: Text & Categorical Cleaning ---
def _stage_text_categorical(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cols = config.get("columns", df.select_dtypes(include=['object', 'string']).columns.tolist())
    cells_changed = 0
    
    case_rule = config.get("case_normalization", "none") # Title Case, upper, lower, sentence case
    
    for col in cols:
        if col not in df.columns: continue
        original = df[col].copy()
        
        # 1. Standardize nulls
        df[col] = df[col].replace(['N/A', 'NA', 'null', 'None', 'NULL', 'None', ''], np.nan)
        
        # 2. Whitespace cleaning
        df[col] = df[col].astype(str).str.strip().replace(r'\s+', ' ', regex=True)
        
        # 3. Special characters
        if config.get("remove_special_chars", False):
            df[col] = df[col].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
            
        # 4. Case
        if case_rule == "Title Case": df[col] = df[col].str.title()
        elif case_rule == "upper": df[col] = df[col].str.upper()
        elif case_rule == "lower": df[col] = df[col].str.lower()
        elif case_rule == "sentence case": df[col] = df[col].str.capitalize()
        
        # 5. Categorical mapping (e.g. M/Male -> Male)
        mapping = config.get("categorical_mapping", {}).get(col, {})
        if mapping:
            df[col] = df[col].replace(mapping)
            
        cells_changed += (df[col] != original).sum()

    return df, {
        "stage": "text_categorical_cleaning",
        "columns_affected": cols,
        "cells_changed": int(cells_changed),
        "timestamp": datetime.now().isoformat()
    }

# --- Stage 5: Boolean Standardization ---
def _stage_boolean(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cols = config.get("columns", [])
    conversions = 0
    invalid = 0
    
    bool_map = {
        '1': True, '0': False, 1: True, 0: False,
        'true': True, 'false': False, 'TRUE': True, 'FALSE': False,
        'yes': True, 'no': False, 'YES': True, 'NO': False,
        'y': True, 'n': False, 'Y': True, 'N': False
    }
    
    for col in cols:
        if col not in df.columns: continue
        original = df[col].copy()
        df[col] = df[col].map(lambda x: bool_map.get(str(x).lower().strip(), np.nan) if not pd.isna(x) else np.nan)
        
        conversions += (df[col].notna() & (df[col] != original)).sum()
        invalid += df[col].isna().sum() - original.isna().sum()
        
    return df, {
        "stage": "boolean_standardization",
        "conversions_count": int(conversions),
        "invalid_boolean_count": int(invalid),
        "timestamp": datetime.now().isoformat()
    }

# --- Stage 6: Date Parsing & Cleaning ---
def _stage_dates(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cols_config = config.get("columns", {}) # { "col_name": "format" }
    invalid_count = 0
    fixed_tz = 0
    
    for col, fmt in cols_config.items():
        if col not in df.columns: continue
        original_na = df[col].isna().sum()
        
        # Enforce explicit format
        df[col] = pd.to_datetime(df[col], format=fmt, errors='coerce')
        
        # Timezone consistency (standardize to UTC-naive or specific)
        if df[col].dt.tz is not None:
            df[col] = df[col].dt.tz_localize(None)
            fixed_tz += len(df)
            
        invalid_count += df[col].isna().sum() - original_na

    return df, {
        "stage": "date_parsing_cleaning",
        "invalid_date_count": int(invalid_count),
        "timezone_fixes_applied": int(fixed_tz),
        "timestamp": datetime.now().isoformat()
    }

# --- Stage 7: Range-Based Cleaning ---
def _stage_ranges(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    # Numeric ranges
    num_constraints = config.get("numeric_columns", {}) # { col: {min: X, max: Y, action: "clip/nan/median/drop"} }
    invalid_count = 0
    rows_dropped = 0
    
    for col, c in num_constraints.items():
        if col not in df.columns: continue
        
        low, high = c.get("min", -np.inf), c.get("max", np.inf)
        # Ensure column is numeric before comparison to avoid TypeError
        col_data = pd.to_numeric(df[col], errors='coerce')
        mask = (col_data < low) | (col_data > high)
        invalid_count += mask.sum()
        
        action = c.get("action", "nan")
        if action == "clip":
            df[col] = col_data.clip(lower=low, upper=high)
        elif action == "median":
            df.loc[mask, col] = col_data.median()
        elif action == "drop":
            rows_dropped += mask.sum()
            df = df[~mask]
        else: # nan
            df.loc[mask, col] = np.nan

    # Date ranges (e.g. remove future dates)
    if config.get("no_future_dates", True):
        now = pd.Timestamp.now()
        for col in df.select_dtypes(include=['datetime64', 'datetime']).columns:
            mask = df[col] > now
            invalid_count += mask.sum()
            df.loc[mask, col] = pd.NaT

    return df, {
        "stage": "range_based_cleaning",
        "invalid_values_count": int(invalid_count),
        "rows_affected": int(rows_dropped),
        "timestamp": datetime.now().isoformat()
    }

# --- Stage 8: Missing Value Handling ---
def _stage_missing_values(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    numeric_strat = config.get("numeric_strategy", "mean") # mean, median, mode, zero, custom, nan, drop
    categorical_strat = config.get("categorical_strategy", "mode") # mode, N/A, nan, drop
    date_strat = config.get("date_strategy", "ffill") # ffill, bfill, nan, drop
    
    before_missing = df.isnull().sum().sum()
    before_rows = len(df)
    
    # 1. Numeric
    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        if numeric_strat == "mean": df[col] = df[col].fillna(df[col].mean())
        elif numeric_strat == "median": df[col] = df[col].fillna(df[col].median())
        elif numeric_strat == "mode": 
            m = df[col].mode()
            if not m.empty: df[col] = df[col].fillna(m[0])
        elif numeric_strat == "zero": df[col] = df[col].fillna(0)
        elif numeric_strat == "custom": df[col] = df[col].fillna(config.get("custom_numeric_fill", 0))
        elif numeric_strat == "drop": df = df.dropna(subset=[col])
        # else nan, do nothing
        
    # 2. Categorical
    cat_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in cat_cols:
        if categorical_strat == "mode":
            m = df[col].mode()
            if not m.empty: df[col] = df[col].fillna(m[0])
        elif categorical_strat == "N/A": df[col] = df[col].fillna("N/A")
        elif categorical_strat == "drop": df = df.dropna(subset=[col])
        
    # 3. Dates
    date_cols = df.select_dtypes(include=['datetime64']).columns
    for col in date_cols:
        if date_strat == "ffill": df[col] = df[col].ffill()
        elif date_strat == "bfill": df[col] = df[col].bfill()
        elif date_strat == "drop": df = df.dropna(subset=[col])

    after_missing = df.isnull().sum().sum()
    rows_removed = before_rows - len(df)
    
    return df, {
        "stage": "missing_value_handling",
        "missing_before": int(before_missing),
        "missing_after": int(after_missing),
        "rows_affected": int(rows_removed),
        "timestamp": datetime.now().isoformat()
    }

# --- Stage 9: Scaling and Normalization ---
def _stage_scaling(df: pd.DataFrame, config: dict) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    method = config.get("method", "none") # minmax, zscore, none
    cols = config.get("columns", [])
    
    if method == "none" or not cols:
        return df, {"stage": "scaling", "description": "No scaling applied", "timestamp": datetime.now().isoformat()}
        
    for col in cols:
        if col not in df.columns: continue
        if method == "minmax":
            df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
        elif method == "zscore":
            df[col] = (df[col] - df[col].mean()) / df[col].std()
            
    return df, {
        "stage": "scaling",
        "scaling_method": method,
        "columns_scaled": cols,
        "timestamp": datetime.now().isoformat()
    }
