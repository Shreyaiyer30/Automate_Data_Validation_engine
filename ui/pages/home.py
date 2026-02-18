import streamlit as st
from src.core.constants import PIPELINE_STEPS
from src.core.session_state import CURRENT_PAGE
from ui.components.pipeline_tracker import render_pipeline_tracker


def render():
    """Renders the Home page."""
    st.markdown(
        """
        <div class="studio-shell">
            <div class="studio-head">
                <div>
                    <p class="studio-title">DataClean Studio</p>
                    <p class="studio-sub">Production-style data quality workflow: upload, profile, validate, clean, visualize, export.</p>
                </div>
                <span class="studio-chip">6-Step Pipeline</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="studio-card">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    col1.markdown("**Smart Profiling**\n\nColumn stats, missingness, duplicates, and distributions.")
    col2.markdown("**Rule Validation**\n\nApply business rules and detect violations before cleaning.")
    col3.markdown("**Deterministic Cleaning**\n\nRun controlled transformations with full audit trail.")
    st.markdown("</div>", unsafe_allow_html=True)

    c1, c2 = st.columns([1, 3])
    if c1.button("Get Started", use_container_width=True, type="primary"):
        st.session_state[CURRENT_PAGE] = "Upload"
        st.rerun()
    c2.caption("Supports CSV/XLSX. Data stays in your local app session.")

    render_pipeline_tracker(active_idx=-1, pipeline_steps=PIPELINE_STEPS)
