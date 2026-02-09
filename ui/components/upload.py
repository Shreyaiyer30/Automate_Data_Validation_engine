import streamlit as st
import pandas as pd
from src.utils.file_loader import EnhancedDataLoader

def render_upload_section():
    uploaded_file = st.file_uploader(
        "Upload your dataset",
        type=["csv", "xlsx", "xls", "parquet", "json"]
    )

    if uploaded_file:
        try:
            # Create loader and load file
            loader = EnhancedDataLoader(verbose=True)
            df, metadata = loader.load(uploaded_file)

            st.success(f"Successfully loaded '{uploaded_file.name}'")
            st.session_state.df_raw = df
            st.session_state.metadata = metadata
            
            # Clear downstream state to prevent desync errors
            st.session_state.df_cleaned = None
            st.session_state.report = None
            st.session_state.active_rules = {}
            
            return True

        except Exception as e:
            st.error(f"Failed to load file: {e}")
            return False

    return False
