import pandas as pd
import sys
from pathlib import Path
import json

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_processor import DataProcessor

def verify_semantic_deep():
    print("Starting Advanced Semantic Validation Deep Verification...")
    
    # 1. Load test data
    test_file = "data/semantic_deep_test.csv"
    df = pd.read_csv(test_file)
    print(f"Loaded test data with shape: {df.shape}")
    
    # 2. Analyze Data Quality
    # Use clean config to isolate semantic logic
    clean_config = {"stages": []}
    processor = DataProcessor(settings=clean_config)
    analysis = processor.analyze_data_quality(df)
    
    print("\n--- Global Quality Alerts ---")
    global_rules = analysis["suggested_rules"].get("_global", {})
    for alert in global_rules.get("quality_alerts", []):
        try:
            print(alert)
        except UnicodeEncodeError:
            print(alert.encode('ascii', 'replace').decode())
    
    print("\n--- Structured Metadata (Analysis) ---")
    for col in ["DOB", "Age", "Phone"]:
        meta = analysis["column_analysis"][col].get("semantic_metadata", {})
        print(f"Column: {col} -> {json.dumps(meta, indent=2)}")

    # 3. Process Data
    print("\n--- Processing Data ---")
    suggested_rules = analysis["suggested_rules"]
    df_cleaned, report = processor.process_data(df, suggested_rules)
    
    print("\n--- Cleaned Data Sample ---")
    print(df_cleaned.head())
    
    print("\n--- Audit Log Highlights ---")
    semantic_logs = [e for e in report.get('audit_log', []) if e.get('stage') in ['SEMANTIC_ENGINE', 'SEMANTIC_VALIDATOR']]
    for log in semantic_logs:
        print(f"[{log.get('stage')}] {log.get('mutation_type')}: {log.get('details')}")

    # 4. Requirement Validation
    print("\n--- Requirement Validation ---")
    # Check if Dob (normalized header) serial 35845 was converted to 14-02-1998
    dob_val = df_cleaned.iloc[0]["Dob"] 
    print(f"DOB '35845' converted to: {dob_val} (Expected: 14-02-1998)")
    
    # Check if Age was derived from DOB (for the first row)
    age_val = df_cleaned.iloc[0]["Age"]
    print(f"Age derived from 35845 (1998): {age_val}")
    
    # Check if missing phone was detected
    phone_meta = analysis["column_analysis"]["Phone"].get("semantic_metadata", {})
    print(f"Missing phones detected in metadata: {phone_meta.get('missing')}")

    print("\nDeep Verification complete.")

if __name__ == "__main__":
    verify_semantic_deep()
