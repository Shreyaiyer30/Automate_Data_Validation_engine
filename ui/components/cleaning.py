import streamlit as st
import pandas as pd
from ui.components.dynamic_rules import render_dynamic_rules_editor

def render_cleaning_lab(df: pd.DataFrame):
    """Renders controls for the cleaning laboratory."""
    st.markdown("## 🧪 CLEANING LAB")
    
    # 1. Dynamic Rules Section
    st.session_state.active_rules = render_dynamic_rules_editor(df, st.session_state.processor)
    
    # 2. Manual Config (Legacy/Custom)
    st.markdown("---")
    st.markdown("### 🛠️ Global Parameters")
    c1, c2 = st.columns(2)
    with c1:
        missing_thresh = st.slider("Max Missing Value % (Row Drop)", 0, 100, 50)
    with c2:
        outlier_method = st.selectbox("Outlier Detection Method", ["iqr", "z-score"])
        
    config = {
        "thresholds": {
            "max_missing_row_percentage": missing_thresh,
            "outlier_method": outlier_method
        }
    }
    
    if st.button("🚀 EXECUTE PIPELINE", use_container_width=True):
        with st.spinner("Processing..."):
            # Use the new DataProcessor which handles dynamic rules
            df_cleaned, report = st.session_state.processor.process_data(
                df, 
                rules=st.session_state.active_rules
            )
            st.session_state.df_cleaned = df_cleaned
            st.session_state.report = report
            st.success("Cleaning Pipeline Completed!")
            st.rerun()
