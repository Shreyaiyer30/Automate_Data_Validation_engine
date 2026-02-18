import pandas as pd
import numpy as np
import re
from typing import Dict, Any, List

class DataValidationEngine:
    @staticmethod
    def validate_dataset(df: pd.DataFrame, rules: Dict[str, List[Dict[str, Any]]], strict_mode: bool = False) -> Dict[str, Any]:
        """
        Enterprise-grade validation engine.
        Supports column-level and cross-field logic.
        """
        val_df = df.copy()
        val_df["_val_status"] = "PASS"
        val_df["_val_reason"] = ""
        
        column_summary = {}
        row_failures = []
        
        # 1. Column-Level Validation
        for col, col_rules in rules.items():
            if col not in df.columns: continue
            column_summary[col] = {"failed_count": 0, "issues": []}
            
            for rule in col_rules:
                rule_type = rule.get("type")
                mask = pd.Series(False, index=df.index)
                reason = rule.get("error_message", f"Invalid {col}")
                
                if rule_type == "Whole number":
                    curr_mask = pd.to_numeric(df[col], errors='coerce').isna() | (pd.to_numeric(df[col], errors='coerce') % 1 != 0)
                    mask |= curr_mask
                
                elif rule_type == "Decimal number":
                    mask |= pd.to_numeric(df[col], errors='coerce').isna()
                
                elif rule_type == "Required":
                    mask |= df[col].isna() | (df[col].astype(str).str.strip() == "")
                
                elif rule_type == "Unique":
                    mask |= df.duplicated(subset=[col], keep=False)
                
                elif rule_type == "Pattern":
                    ptype = rule.get("pattern_type")
                    if ptype == "Email":
                        regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                    elif ptype == "Phone":
                        regex = r'^\+?1?\d{9,15}$'
                    elif ptype == "URL":
                        regex = r'^https?://.+'
                    else: # Custom Regex
                        regex = rule.get("regex", r'.*')
                    
                    mask |= ~df[col].astype(str).str.match(regex, na=False)
                
                elif rule_type == "In Column":
                    other_col = rule.get("other_column")
                    if other_col in df.columns:
                        mask |= ~df[col].isin(df[other_col])
                
                elif rule_type == "Formula":
                    formula = rule.get("formula")
                    try:
                        # eval returns True for passed, so we invert
                        mask |= ~df.eval(formula)
                    except:
                        pass
                
                # Apply failures to val_df
                if mask.any():
                    val_df.loc[mask, "_val_status"] = "FAIL"
                    # Append reasons
                    def append_reason(existing, new):
                        if not existing: return new
                        return f"{existing} | {new}"
                    
                    val_df.loc[mask, "_val_reason"] = val_df.loc[mask, "_val_reason"].apply(lambda x: append_reason(x, reason))
                    
                    column_summary[col]["failed_count"] += mask.sum()
                    column_summary[col]["issues"].append(reason)

        # 2. Cross-Field Logical Validation (Can be part of Formula rules or explicit)
        # Note: General formula support in step 1 covers cross-field like `Age > Experience`
        
        failed_count = (val_df["_val_status"] == "FAIL").sum()
        passed_count = len(df) - failed_count
        
        # 3. Handle Strict Mode (Drop failing rows if configured)
        if strict_mode:
            val_df = val_df[val_df["_val_status"] == "PASS"]
            
        return {
            "total_rows": len(df),
            "passed": passed_count,
            "failed": int(failed_count),
            "column_summary": column_summary,
            "report_df": val_df
        }
