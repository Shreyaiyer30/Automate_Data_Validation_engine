import streamlit as st
import pandas as pd
import numpy as np
from ui.components.dynamic_rules import render_dynamic_rules_editor

def render_cleaning_lab(df: pd.DataFrame):
    """Renders controls for the cleaning laboratory."""
    st.markdown("## 🧪 CLEANING LAB")
    
    # 1. Global Parameters (Top Level for better UX)
    st.markdown("### 🛠️ Global Parameters")
    c1, c2 = st.columns(2)
    with c1:
        missing_thresh = st.slider("Max Missing Value % (Row Drop)", 0, 100, 50, help="Rows with more than this % of missing values will be dropped at the start.")
    with c2:
        outlier_method = st.selectbox("Global Outlier Method", ["iqr", "z-score"], help="Method used for clipping outliers in numeric columns.")
        
    # 2. Dynamic Rules Section
    # Pass global settings to the processor/generator
    st.session_state.processor.rule_generator.settings['outlier_method'] = outlier_method
    
    st.session_state.active_rules = render_dynamic_rules_editor(df, st.session_state.processor)
    st.session_state.active_rules["_global"]["outlier_method"] = outlier_method
    st.session_state.active_rules["_global"]["max_missing_row_percentage"] = missing_thresh
    
    # 3. Execution
    st.markdown("---")
    c_btn, c_reset = st.columns([4, 1])
    
    if c_btn.button("🚀 EXECUTE PIPELINE", use_container_width=True):
        with st.spinner("Processing..."):
            df_cleaned, report = st.session_state.processor.process_data(
                df, 
                rules=st.session_state.active_rules
            )
            st.session_state.df_cleaned = df_cleaned
            st.session_state.report = report
            st.success("Cleaning Pipeline Completed!")
            
    # 4. Comparative Preview (Enhanced Visibility)
    if st.session_state.df_cleaned is not None:
        st.markdown("### 🔍 Side-by-Side Change Preview")
        
        # --- ARCHITECTURAL CONTRACT: STYLING CONTEXT (PHASE 4 ALIGNMENT) ---
        # 1. Alignment & Sampling
        df_raw_sample = df.head(10).copy()
        df_clean_sample = st.session_state.df_cleaned.head(10).copy()
        
        # 2. Schema Bridging (Align Raw to Normalized for comparison)
        header_mapping = st.session_state.report.get('header_normalization', {}).get('mapping', {})
        df_raw_aligned = df_raw_sample.rename(columns=header_mapping)
        
        # Ensure Alignment (Aligned names vs Normalized names)
        if df_raw_aligned.shape == df_clean_sample.shape and list(df_raw_aligned.columns) == list(df_clean_sample.columns):
            try:
                # 3. Vectorized Masking (Logic Layer)
                diff_mask = df_clean_sample.astype(str) != df_raw_aligned.astype(str)
                
                # 4. Stateless Styling (Presentation Layer)
                def apply_style(data):
                    color = 'background-color: rgba(0, 255, 0, 0.15); border: 1px solid rgba(0, 255, 0, 0.3);'
                    return pd.DataFrame(np.where(diff_mask, color, ''), 
                                      index=data.index, columns=data.columns)

                styled_df = df_clean_sample.style.apply(apply_style, axis=None)
                
                st.info("Cells highlighted in green were modified by the pipeline.")
                tab1, tab2 = st.tabs(["✨ Cleaned Preview", "📄 Raw Baseline"])
                
                with tab1:
                    # Cleaned state already has Title Case headers
                    st.dataframe(styled_df, use_container_width=True)
                with tab2:
                    # Raw state shows original headers (Preservation Contract)
                    st.dataframe(df_raw_sample, use_container_width=True)
            except Exception as e:
                st.warning(f"⚠️ Preview style failed: {str(e)}. Displaying raw data.")
                st.dataframe(df_clean_sample, use_container_width=True)
        else:
            st.warning("⚠️ Preview unavailable due to alignment mismatch.")
            st.dataframe(df_clean_sample.head(10), use_container_width=True)
        
        st.rerun() if 'rerun_flag' in st.session_state else None
