import sys
import os
import argparse
import json
from pathlib import Path

# Add src to path if needed
sys.path.append(str(Path(__file__).parent.parent))

from src.data_processor import DataProcessor

def main():
    parser = argparse.ArgumentParser(description="Automated Data Validation & Cleaning Engine CLI")
    parser.add_argument("--file", required=True, help="Path to the raw CSV dataset")
    parser.add_argument("--config", help="Path to YAML validation rules")
    parser.add_argument("--output", help="Path to save cleaned CSV")
    parser.add_argument("--report", help="Path to save quality report (JSON)")
    
    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: File {args.file} not found.")
        sys.exit(1)

    processor = DataProcessor(config_path=args.config)
    
    print(f"[*] Processing {args.file}...")
    try:
        df_cleaned, report = processor.process_data(args.file)
        
        # Save results
        output_csv = args.output or f"cleaned_{os.path.basename(args.file)}"
        df_cleaned.to_csv(output_csv, index=False)
        print(f"[+] Cleaned data saved to {output_csv}")

        if args.report:
            with open(args.report, 'w') as f:
                json.dump(report, f, indent=4)
            print(f"[+] Quality report saved to {args.report}")
        else:
            print("\n--- Quality Report Summary ---")
            print(json.dumps(report['summary'], indent=4))
            
    except Exception as e:
        print(f"[-] Error during processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
