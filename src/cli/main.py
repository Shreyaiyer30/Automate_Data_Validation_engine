import argparse
import pandas as pd
from src.engine.atomic_engine import AtomicEngine
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Data Quality Engine CLI")
    parser.add_argument("--input", "-i", type=str, required=True, help="Input CSV path")
    parser.add_argument("--output", "-o", type=str, default="data/cleaned/output.csv", help="Output path")
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: File {args.input} not found.")
        return
        
    print(f"🚀 Processing {args.input}...")
    df = pd.read_csv(input_path)
    
    engine = AtomicEngine()
    df_cleaned, report = engine.run(df)
    
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_cleaned.to_csv(output_path, index=False)
    
    print(f"✅ Success! Quality Score: {report['quality_score']}%")
    print(f"📄 Report saved in audit logs.")

if __name__ == "__main__":
    main()
