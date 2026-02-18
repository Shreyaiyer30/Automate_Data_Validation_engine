import streamlit as st
import pandas as pd
from src.core.constants import PIPELINE_STEPS
from src.core.session_state import RAW_DF, CLEAN_DF, ANALYZED_DF, RUN_RESULT, UPLOAD_FILENAME, PIPELINE_STAGE, CURRENT_PAGE
from ui.components.pipeline_tracker import render_pipeline_tracker
from src.engine.ingestion import load_dataset
from src.engine.profiler import get_quick_stats


def render():
    """Renders the Upload page."""
    st.markdown(
        """
        <div class="studio-shell">
            <div class="studio-head">
                <div>
                    <p class="studio-title">Upload Dataset</p>
                    <p class="studio-sub">Bring in CSV or Excel data to start the quality pipeline.</p>
                </div>
                <span class="studio-chip">Ingestion</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_pipeline_tracker(active_idx=0, pipeline_steps=PIPELINE_STEPS)

    uploaded = st.file_uploader("Drop your file here", type=["csv", "xlsx", "xls"], help="Supports CSV and Excel. Max 200MB.")

    if uploaded:
        try:
            with st.spinner("Loading dataset..."):
                df = load_dataset(uploaded)

            st.session_state[RAW_DF] = df
            st.session_state[CLEAN_DF] = None
            st.session_state[ANALYZED_DF] = None
            st.session_state[RUN_RESULT] = None
            st.session_state[UPLOAD_FILENAME] = uploaded.name
            st.session_state[PIPELINE_STAGE] = 1

            st.success(f"Loaded {uploaded.name} - {len(df):,} rows x {len(df.columns)} columns")

            st.markdown('<div class="studio-card">', unsafe_allow_html=True)
            st.markdown('<div class="studio-card-title">Preview</div>', unsafe_allow_html=True)
            st.dataframe(df.head(20), use_container_width=True, height=280)

            stats = get_quick_stats(df)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Rows", f"{stats['rows']:,}")
            c2.metric("Columns", stats["cols"])
            c3.metric("Missing Cells", f"{stats['missing_cells']:,}")
            c4.metric("Duplicate Rows", f"{stats['duplicates']:,}")
            st.markdown("</div>", unsafe_allow_html=True)

            if st.button("Continue to Overview", use_container_width=False):
                st.session_state[CURRENT_PAGE] = "Overview"
                st.rerun()

        except Exception as e:
            st.error(f"Failed to load file: {e}")
    else:
        st.markdown(
            """
            <div class="studio-card" style="text-align:center; padding:2.2rem;">
                <div style="font-size:2.2rem;">Data File</div>
                <div style="margin-top:0.4rem; color:#cbd5e1;">Upload CSV/XLSX above to begin.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
