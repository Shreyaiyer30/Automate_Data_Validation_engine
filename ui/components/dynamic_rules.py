import streamlit as st
import pandas as pd
from typing import Dict

def render_dynamic_rules_editor(df: pd.DataFrame, processor):
    """UI for viewing and editing dynamic cleaning rules"""
    
    st.markdown("### 🔧 Dynamic Cleaning Rules")
    st.info("AI has analyzed your dataset and suggested the following cleaning rules. You can override them below.")
    
    # Generate rules automatically
    # analysis = processor.analyze_data_quality(df)
    # auto_rules = analysis['suggested_rules']
    # quality_score = analysis.get('quality_score', 100)
    # st.metric("Data Quality Score", f"{quality_score:.1f}/100")
    
    analysis = processor.analyze_data_quality(df)

    auto_rules = analysis.get('suggested_rules', {})

    if not auto_rules:
        st.warning("⚠️ No suggested rules available.")
        return {}

    quality_score = analysis.get('quality_score', 100)
    st.metric("Data Quality Score", f"{quality_score:.1f}/100")

    # Global Rules
    with st.expander("🌐 Global Cleaning Rules", expanded=True):
        remove_dups = st.checkbox(
            "Remove duplicate rows", 
            value=auto_rules.get('_global', {}).get('remove_duplicates', False)
        )
        if '_global' not in auto_rules: auto_rules['_global'] = {}
        auto_rules['_global']['remove_duplicates'] = remove_dups

    for col, rules in auto_rules.items():
        if col.startswith("_"): continue
        with st.expander(f"📊 {col} ({df[col].dtype})", expanded=False):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Show column statistics
                st.markdown("**Statistics:**")
                patterns = analysis['column_analysis'][col]
                st.write(f"Missing: {patterns['missing_count']} ({patterns['missing_percentage']:.1f}%)")
                if pd.api.types.is_numeric_dtype(df[col]):
                    st.write(f"Min: {patterns.get('min', 'N/A')}, Max: {patterns.get('max', 'N/A')}")
                    if patterns.get('has_outliers'):
                        st.warning(f"⚠️ {patterns['outlier_count']} outliers detected")
                else:
                    st.write(f"Unique values: {patterns['unique_count']}")
                    if patterns.get('has_special_chars'):
                        st.warning(f"⚠️ Special characters detected")
            
            with col2:
                # Allow rule overrides
                st.markdown("**Override Rules:**")
                
                if df[col].dtype == 'object':
                    handle_missing = st.selectbox(
                        f"Handle missing",
                        ["fill_with_mode", "fill_with_unknown", "forward_fill", "backward_fill", "do_nothing"],
                        index=0,
                        key=f"missing_{col}"
                    )
                    auto_rules[col]['handle_missing'] = handle_missing
                    auto_rules[col]['handle_missing'] = handle_missing
                else:
                    handle_missing = st.selectbox(
                        f"Handle missing",
                        ["impute_with_median", "impute_with_mean", "forward_fill", "backward_fill", "do_nothing"],
                        index=0,
                        key=f"missing_{col}"
                    )
                    auto_rules[col]['handle_missing'] = handle_missing

                # Date Format Input
                if pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == 'object':
                     date_fmt = st.text_input(f"Date Format (Optional)", value=rules.get('date_format', ''), key=f"date_fmt_{col}", placeholder="%Y-%m-%d")
                     if date_fmt:
                         auto_rules[col]['date_format'] = date_fmt
            
            # Show suggested rules
            st.markdown("**Suggested Transformations:**")
            rule_cols = st.columns(3)
            
            i = 0
            for rule_key, rule_value in rules.items():
                if rule_key == 'handle_missing': 
                    continue
                
                with rule_cols[i % 3]:
                    if isinstance(rule_value, bool):
                        enabled = st.toggle(
                            rule_key.replace('_', ' ').title(), 
                            value=rule_value, 
                            key=f"{col}_{rule_key}"
                        )
                        auto_rules[col][rule_key] = enabled
                        i += 1
                    elif isinstance(rule_value, (int, float)):
                        # Just display numeric thresholds for now
                        st.text(f"{rule_key.replace('_', ' ').title()}: {rule_value:.1f}")
                        i += 1
    
    return auto_rules