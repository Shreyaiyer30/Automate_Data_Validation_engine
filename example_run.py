import pandas as pd
import json
import sys
import argparse
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.semantic_pipeline.pipeline import DynamicCleaningPipeline

def main():
    parser = argparse.ArgumentParser(description="Run Semantic Data Cleaning Pipeline on a dataset.")
    parser.add_argument("input_file", nargs='?', help="Path to input CSV or Excel file", default=None)
    parser.add_argument("--output", "-o", help="Path to save cleaned file (default: cleaned_<input>.csv)", default=None)
    
    args = parser.parse_args()
    
    # Load Data
    if args.input_file:
        file_path = Path(args.input_file)
        if not file_path.exists():
            print(f"Error: File '{file_path}' not found.")
            return
            
        print(f"Loading data from {file_path}...")
        if file_path.suffix.lower() in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path)
        else:
            try:
                df = pd.read_csv(file_path)
            except:
                # Try with encoding
                df = pd.read_csv(file_path, encoding='latin1')
    else:
        print("No input file provided. Using built-in demo data.")
        # Create demo data if no file
        data = [
            {"Name": "  rahul sharma ", "Phone": "9876543210", "Email": "RAHUL@GMAIL.COM", "DOB": "1990/05/20", "Age": 34, "City": "delhi"},
            {"Name": "priya", "Phone": "+91-9988776655", "Email": "priya.test", "DOB": "1995-12-12", "Age": 150, "City": "MUMBAI"},
            {"Name": "Amit Kumar", "Phone": "12345", "Email": "amit@work.org", "DOB": "invalid", "Age": 28, "City": "  pune "},
            {"Name": "Sneha R", "Phone": "0000000000", "Email": "sneha@domain.co", "DOB": "2000-01-01", "Age": 24, "City": "Bangalore"},
            {"Name": "Mismatched", "Phone": "9000000000", "Email": "mismatch@test.com", "DOB": "1980-01-01", "Age": 20, "City": "Chennai"},
            {"Name": "John Doe", "Phone": "8888888888", "Email": "jane.smith@other.com", "DOB": "1990-01-01", "Age": 36, "City": "NYC"},
        ]
        df = pd.DataFrame(data)
        file_path = Path("demo_data.csv") # Virtual path for naming output

    print(f"Data Shape: {df.shape}")
    
    # Run Pipeline
    pipeline = DynamicCleaningPipeline()
    print("Running pipeline...")
    df_clean, report = pipeline.process(df)
    
    # Save Outputs
    base_name = file_path.stem
    output_csv = args.output or f"cleaned_{base_name}.csv"
    output_json = f"report_{base_name}.json"
    
    print(f"Saving cleaned data to {output_csv}...")
    df_clean.to_csv(output_csv, index=False)
    
    print(f"Saving validation report to {output_json}...")
    # Helper to serialize Enum/Dataclass
    def json_default(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)
        
    with open(output_json, 'w') as f:
        json.dump(report, f, indent=2, default=json_default)
        
    # Summary
    print("\n--- Summary ---")
    print(f"Initial Score: {report['initial_quality']['score']}")
    print(f"Final Score:   {report['final_quality']['score']}")
    print(f"Detected Types: {list(report['detected_types'].keys())}")
    print(f"Issues Remaining: {report['final_quality']['issue_count']}")

if __name__ == "__main__":
    main()
