import streamlit as st
import pandas as pd
from typing import Optional

# Session Keys Mapping
RAW_DF = "raw_df"
CLEAN_DF = "clean_df"
ANALYZED_DF = "analyzed_df"
RUN_RESULT = "run_result"
UPLOAD_FILENAME = "upload_filename"
PIPELINE_STAGE = "pipeline_stage"
CURRENT_PAGE = "current_page"
DEBUG_MODE = "debug_mode"
DEBUG_EXCEPTIONS = "debug_exceptions"
VALIDATION_RULES = "validation_rules"
VALIDATION_REPORT = "validation_report"

def init_session():
    """Initializes the standardized session state."""
    defaults = {
        CURRENT_PAGE: "Home",
        PIPELINE_STAGE: 0,
        RAW_DF: None,
        CLEAN_DF: None,
        ANALYZED_DF: None,
        RUN_RESULT: None,
        UPLOAD_FILENAME: "",
        DEBUG_MODE: False,
        DEBUG_EXCEPTIONS: [],
        VALIDATION_RULES: {},
        VALIDATION_REPORT: None,
        "config": {},
        "header_mapping": {},
        "cleaning_actions": [],
        "score_after": 0.0,
        "_validation_issues": [],
        "_validation_score": 100.0
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

def get_df(key: str) -> Optional[pd.DataFrame]:
    return st.session_state.get(key)

def set_df(key: str, df: Optional[pd.DataFrame]):
    st.session_state[key] = df
