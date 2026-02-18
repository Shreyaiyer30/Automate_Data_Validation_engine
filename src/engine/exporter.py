import pandas as pd
import io
from typing import Dict, Any
import json
from datetime import datetime

def generate_formatted_excel(df_raw: pd.DataFrame, df_clean: pd.DataFrame, 
                             validation_report: Dict[str, Any], corrections_df: pd.DataFrame) -> bytes:
    """
    Generates a 4-sheet Excel Data Package:
    1. Raw_Data
    2. Clean_Data
    3. Validation_Report
    4. Corrections_Applied
    """
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # 1. Raw Data
        df_raw.to_excel(writer, sheet_name="Raw_Data", index=False)
        
        # 2. Clean Data
        df_clean.to_excel(writer, sheet_name="Clean_Data", index=False)
        
        # 3. Validation Report
        val_df = validation_report.get("report_df", pd.DataFrame())
        if not val_df.empty:
            val_df.to_excel(writer, sheet_name="Validation_Report", index=False)
        else:
            pd.DataFrame([{"Message": "No validation report available"}]).to_excel(writer, sheet_name="Validation_Report", index=False)
            
        # 4. Corrections Applied
        corrections_df.to_excel(writer, sheet_name="Corrections_Applied", index=False)
        
    output.seek(0)
    return output.getvalue()

def assemble_json_report(run_result: dict, filename: str, raw_df, clean_df) -> str:
    """Assemble a JSON summary report from the pipeline run result."""
    report = {
        "filename":       filename,
        "generated_at":   datetime.now().isoformat(),
        "state":          run_result.get("state", "UNKNOWN"),
        "rows": {
            "raw":   len(raw_df)   if raw_df   is not None else 0,
            "clean": len(clean_df) if clean_df is not None else 0,
        },
        "columns": {
            "raw":   len(raw_df.columns)   if raw_df   is not None else 0,
            "clean": len(clean_df.columns) if clean_df is not None else 0,
        },
        "summary":     run_result.get("summary", {}),
        "audit_trail": run_result.get("audit_trail", []),
    }
    return json.dumps(report, indent=2, default=str)