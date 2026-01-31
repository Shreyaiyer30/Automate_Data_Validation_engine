import streamlit as st
import pandas as pd
from src.utils.file_loader import robust_read_csv
from src.utils.file_loader import smart_load

def render_upload_section():
    uploaded_file = st.file_uploader(
        "Upload your dataset",
        type=["csv", "xlsx", "xls", "parquet", "json"]
    )

    if uploaded_file:
        try:
            df, metadata = smart_load(uploaded_file)

            st.success("File uploaded successfully")
            st.session_state["raw_df"] = df
            st.session_state["load_metadata"] = metadata
            return True

        except Exception as e:
            st.error(f"Failed to load file: {e}")
            return False

    return False
