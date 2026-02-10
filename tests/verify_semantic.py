import pandas as pd
import sys
from pathlib import Path
import json

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_processor import DataProcessor

def verify_semantic_system():
    print("Starting Semantic Validation System verification...")
    
    # 1. Load test data
    test_file = "data/semantic_test.csv"
    df = pd.read_csv(test_file)
    print(f"Loaded test data with columns: {df.columns.tolist()}")
    
    # 2. Analyze Data Quality (Rule Generation)
    # Use a clean config to avoid interference from default pipeline stages during verification
    clean_config = {
        "stages": [] # Empty list skips all elective atomic stages
    }
    processor = DataProcessor(settings=clean_config)
    analysis = processor.analyze_data_quality(df)
    
    print("\n--- Semantic Detection Analysis ---")
    for col, stats in analysis["column_analysis"].items():
        print(f"Column: {col} | Inferred Semantic Type: {stats.get('semantic_type')}")
    
    suggested_rules = analysis["suggested_rules"]
    print("\n--- Suggested Semantic Rules ---")
    for col, rules in suggested_rules.items():
        if "handle_semantic" in rules:
            print(f"Column: {col} -> {rules['handle_semantic']} ({rules.get('dob_source_column', '')})")

    # 3. Process Data
    print("\n--- Processing Data ---")
    
    # Manually run the steps to see where it breaks, passing initial_stats to avoid NoneType error
    initial_stats = analysis.get("column_analysis", {})
    df_atomic, report_atomic = processor.engine.run(df, initial_stats=initial_stats)
    print("\n--- Data After Atomic Stage ---")
    print(df_atomic.head())
    
    df_cleaned, report = processor.process_data(df, suggested_rules)
    
    print("\n--- Final Cleaned Data Sample ---")
    print(df_cleaned.head())
    
    print("\n--- Full Audit Log ---")
    for log in report.get('audit_log', []):
        msg = log.get('mutation_type') or log.get('event')
        details = log.get('details') or log.get('metadata') or ''
        print(f"[{log.get('stage')}] {msg}: {details}")

    # 4. Check requirements
    # - Age calculated from DOB
    # - Emails validated but not corrected
    # - Phone validated for 10 digits
    
    # Identify normalized column names
    norm_mapping = report['header_normalization']['mapping']
    age_col = norm_mapping.get("User Age")
    email_col = norm_mapping.get("Primary Email")
    phone_col = norm_mapping.get("Contact Number")
    
    print("\n--- Requirement Check ---")
    if age_col in df_cleaned.columns:
        print(f"Age Column '{age_col}' values: {df_cleaned[age_col].tolist()}")
    
    if email_col in df_cleaned.columns:
        print(f"Email Column '{email_col}' (original preserved): {df_cleaned[email_col].tolist()}")
        
    if phone_col in df_cleaned.columns:
        print(f"Phone Column '{phone_col}' (original preserved): {df_cleaned[phone_col].tolist()}")

    print("\nVerification complete.")

if __name__ == "__main__":
    verify_semantic_system()
