import pandas as pd
import numpy as np
import io
from src.engine.cleaning_engine import clean_dataset
from src.engine.validation_engine import DataValidationEngine
from src.engine.exporter import generate_formatted_excel

def run_verification_test():
    print("--- Starting Enterprise Engine Verification Test ---")
    
    # Create "dirty" synthetic dataset
    data = {
        "  Full_name  ": ["john DOE", "Jane Smith", "jane smith", "Duplicate Col", np.nan],
        "Age": [25, 30, 30, "40", -5],
        "Salary": ["12000", "15000", "15000", "invalid", "100.50"],
        "Is_Active": ["yes", "no", "no", "1", "invalid"],
        "Join_Date": ["2023-01-01", "2023-01-02", "2023-01-02", "invalid", "2030-12-31"], # Future date
        "DupCol": [1, 2, 2, 3, 4], # Matches index partially? No, let's make it a real duplicate
        "ConstCol": ["Fixed", "Fixed", "Fixed", "Fixed", "Fixed"]
    }
    df = pd.DataFrame(data)
    df["DupCol"] = df["  Full_name  "] # Force column duplication
    
    print(f"Initial Shape: {df.shape}")
    
    # 1. Validation Rules
    rules = {
        "Age": [{"type": "Whole number", "error_message": "Age must be integer"}],
        "Salary": [{"type": "Decimal number", "error_message": "Salary must be numeric"}],
        "Is_Active": [{"type": "Pattern", "pattern_type": "Custom Regex", "regex": "^(yes|no|1|0)$"}]
    }
    
    # 2. Cleaning Config
    config = {
        "column_cleanup": {"header_case": "Snake Case", "remove_constant": True},
        "duplicates": {"remove_duplicate_cols": True, "row_strategy": "first"},
        "numeric": {"columns": ["age", "salary"]},
        "text_categorical": {"columns": ["full_name", "dupcol"], "case_normalization": "Title Case"},
        "boolean": {"columns": ["is_active"]},
        "dates": {"columns": {"join_date": "%Y-%m-%d"}},
        "ranges": {
            "numeric_columns": {"age": {"min": 0, "max": 120, "action": "clip"}},
            "no_future_dates": True
        },
        "missing_values": {
            "numeric_strategy": "mean", 
            "categorical_strategy": "mode",
            "date_strategy": "nan" # Don't fill so we can verify NaT
        },
        "scaling": {"method": "none", "columns": []}
    }
    
    print("\n[Step 1] Running Validation...")
    val_res = DataValidationEngine.validate_dataset(df, rules)
    print(f"Validation: {val_res['passed']} passed, {val_res['failed']} failed.")
    
    print("\n[Step 2] Running Cleaning Engine...")
    cleaned_df, audit = clean_dataset(df, config)
    print(f"Cleaned Shape: {cleaned_df.shape}")
    print("\n--- Cleaned Data Preview ---")
    print(cleaned_df)
    print("\n--- Audit Trail Summary ---")
    for entry in audit:
        print(f"Stage: {entry.get('stage')} | Affected: {entry.get('rows_affected',0)} rows, {entry.get('cells_changed',0)} cells")
    
    # Verify some key expectations
    # 1. Header standardization: "  Full_name  " -> "full_name" (Snake Case)
    assert "full_name" in cleaned_df.columns, "Header standardization failed"
    # 2. Constant column removed
    assert "const_col" not in cleaned_df.columns, "Constant column cleanup failed"
    
    # 3. Future date handled
    # Find the row that had "2030-12-31" originally. It was the last row (index 4).
    # If deduplication happened, it might be a different index.
    date_val = cleaned_df["join_date"].iloc[-1]
    print(f"DEBUG: Last row join_date = {date_val} (type: {type(date_val)})")
    assert pd.isna(date_val), f"Future date cleanup failed. Found: {date_val}"
    
    # 4. Currency symbols removed
    assert isinstance(cleaned_df["salary"].iloc[0], (int, float, np.number)), f"Currency cleanup failed. Type: {type(cleaned_df['salary'].iloc[0])}"
    
    # 5. Row duplicates removed
    assert len(cleaned_df) < len(df), f"Row deduplication failed. Rows: {len(cleaned_df)}"
    
    print("\n[Step 3] Verifying Excel Export...")
    excel_bytes = generate_formatted_excel(df, cleaned_df, val_res, pd.DataFrame(audit))
    print(f"Generated Excel Size: {len(excel_bytes)} bytes")
    
    print("\n--- All Verification Tests Passed! ---")

if __name__ == "__main__":
    run_verification_test()
