import pandas as pd
import numpy as np

def validate_dataframe(df, rules):
    """
    Validate DataFrame against column rules.
    Returns a dictionary with validation results.
    """
    report = {
        "missing_values": {},
        "duplicate_rows": int(df.duplicated().sum()),
        "schema_errors": [],
        "type_mismatches": {}
    }

    # 1. Schema Validation (Check if required columns exist)
    for col, constraints in rules.items():
        if constraints.get('required', False) and col not in df.columns:
            report["schema_errors"].append(f"Missing required column: {col}")

    # 2. Missing Values Check
    report["missing_values"] = df.isnull().sum().to_dict()

    # 3. Type Checking
    for col, constraints in rules.items():
        if col in df.columns:
            expected_type = constraints.get('dtype')
            if expected_type:
                # Basic type check - this can be complex with pandas dtypes
                # For simplicity, we check if the actual dtype can be coerced to the expected one
                try:
                    if expected_type == "int64":
                        pd.to_numeric(df[col], errors='raise').astype(int)
                    elif expected_type == "float64":
                        pd.to_numeric(df[col], errors='raise').astype(float)
                    elif expected_type.startswith("datetime"):
                        pd.to_datetime(df[col], errors='raise')
                except Exception:
                    report["type_mismatches"][col] = f"Expected {expected_type}, but data contains non-conforming values."

    return report

def get_out_of_range_values(df, rules):
    """
    Check for values outside defined min/max ranges.
    """
    out_of_range = {}
    for col, constraints in rules.items():
        if col in df.columns:
            min_val = constraints.get('min')
            max_val = constraints.get('max')
            
            if min_val is not None or max_val is not None:
                # Convert to numeric for range check, errors='coerce' to skip NaNs or bad types already flagged
                series = pd.to_numeric(df[col], errors='coerce')
                
                if min_val is not None:
                    count = (series < min_val).sum()
                    if count > 0:
                        out_of_range[f"{col}_min"] = int(count)
                
                if max_val is not None:
                    count = (series > max_val).sum()
                    if count > 0:
                        out_of_range[f"{col}_max"] = int(count)
                        
    return out_of_range
