def build_corrections_report(audit_trail: list, before_df, after_df) -> str:
    """
    Generates a professional Markdown correction report from the engine's audit trail.
    Only includes stages that performed meaningful mutations.
    """
    sections = []
    sections.append("# 🛡️ Data Integrity & Correction Report")
    sections.append(f"**Generated at:** {before_df.index.name if hasattr(before_df.index, 'name') else 'N/A'}")
    
    # 1. Summary Overview
    rows_in = len(before_df)
    rows_out = len(after_df)
    retention = (rows_out / rows_in) * 100 if rows_in > 0 else 0
    
    summary_md = f"""
## Executive Summary
- **Input Record Count:** {rows_in:,}
- **Output Record Count:** {rows_out:,}
- **Data Retention Rate:** {retention:.1f}%
- **Total Cleaning Stages Applied:** {len([s for s in audit_trail if s.get('stage') != 'final_summary'])}
- **Processing Status:** ✅ HIGH INTEGRITY
    """
    sections.append(summary_md)
    
    # 2. Detailed Stages
    sections.append("## Detailed Correction Log")
    
    counter = 1
    for entry in audit_trail:
        stage = entry.get("stage")
        if stage == "final_summary": continue
        
        # Only report if something changed
        rows_affected = entry.get("rows_affected", 0)
        cells_changed = entry.get("cells_changed", 0)
        renamed = entry.get("renamed_mapping", {})
        dropped_cols = entry.get("columns_removed", entry.get("removed_constant_columns", []))
        
        if rows_affected > 0 or cells_changed > 0 or renamed or dropped_cols:
            title = stage.replace('_', ' ').capitalize()
            sections.append(f"### {counter}. {title}")
            
            if rows_affected:
                sections.append(f"- **Rows Removed/Modified:** {rows_affected:,}")
            if cells_changed:
                sections.append(f"- **Data Points Corrected:** {cells_changed:,}")
            
            # Specific details per stage
            if stage == "column_level_cleaning" and renamed:
                sections.append("- **Header Standardization Applied:**")
                # Show first 5 renames as example if many
                for old, new in list(renamed.items())[:5]:
                    if old != new:
                        sections.append(f"  - `{old}` → `{new}`")
                if len(renamed) > 5:
                    sections.append(f"  - ... and {len(renamed)-5} other columns standardized.")
                    
            if dropped_cols:
                sections.append(f"- **Columns Dropped:** {', '.join([f'`{c}`' for c in dropped_cols])}")
                
            if stage == "numeric_cleaning":
                sections.append(f"- **Invalid Numeric Coercions:** {entry.get('invalid_conversions_count', 0)}")
                
            if stage == "missing_value_handling":
                sections.append(f"- **Null Values Resolved:** {entry.get('missing_before', 0) - entry.get('missing_after', 0):,}")
            
            counter += 1
            
    if counter == 1:
        sections.append("*No data mutations were required. Dataset already meets integrity standards.*")
        
    return "\n\n".join(sections)
