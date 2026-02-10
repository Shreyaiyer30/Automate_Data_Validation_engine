import pandas as pd
import numpy as np
import io

def test_export_logic():
    print("--- Testing Export Logic ---")
    
    # 1. Create Data with NaNs and Mixed Types
    df = pd.DataFrame({
        "text_col": ["A", "B", np.nan, "D"],
        "num_col": [1.0, 2.5, np.nan, 4.0],
        "date_col": [pd.Timestamp("2023-01-01"), pd.NaT, pd.Timestamp("2023-01-03"), pd.NaT]
    })
    
    # Simulate "Unknown" strings that should be NaN
    df["dirty_col"] = ["X", "Unknown", "Y", "nan"]
    
    print("\nOriginal DataFrame:")
    print(df)
    
    # 2. Simulate CSV Export (Current behavior likely buggy if we just do to_csv)
    print("\n--- CSV Export Check ---")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()
    print("CSV Content (Raw):")
    print(csv_content)
    
    # Check if NaNs are empty strings (Correct) or "nan" (Incorrect)
    if ",," in csv_content or ",\n" in csv_content:
        print("CSV: NaNs appear to be empty strings.")
    else:
        print("CSV: NaNs might be missing or explicitly 'nan'.")

    # 3. Simulate Excel Export
    print("\n--- Excel Export Check ---")
    # We can't easily print Excel binary, but we can verify the logic we PLAN to use.
    # The user wants "Unknown", "nan" strings to be np.nan FIRST.
    
    # Proposed cleanup function
    def prepare_for_export(df_in):
        df_out = df_in.copy()
        
        # Replace common placeholders with NaN
        replace_vals = ["Unknown", "nan", "NaN", "null", "None", "N/A", ""]
        df_out.replace(replace_vals, np.nan, inplace=True)
        
        return df_out
        
    df_clean = prepare_for_export(df)
    print("\nCleaned DataFrame for Export:")
    print(df_clean)
    
    # Verify Date column is still datetime
    print(f"\nDate Col Type: {df_clean['date_col'].dtype}")
    
    print("\nTest Complete.")

if __name__ == "__main__":
    test_export_logic()
