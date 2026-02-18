import streamlit as st
import pandas as pd
import numpy as np
from src.core.constants import PIPELINE_STEPS
from src.core.session_state import RAW_DF, CURRENT_PAGE
from ui.components.pipeline_tracker import render_pipeline_tracker
from ui.components.tables import render_excel_table
from src.engine.profiler import get_quick_stats, get_column_profiles


def render():
    """Renders the Overview page."""
    df = st.session_state.get(RAW_DF)
    if df is None:
        st.warning("Upload a dataset first.")
        return

    st.markdown(
        """
        <div class="studio-shell">
            <div class="studio-head">
                <div>
                    <p class="studio-title">Data Overview Studio</p>
                    <p class="studio-sub">Inspect structure, distributions, and dataset quality before validation.</p>
                </div>
                <span class="studio-chip">Profiler</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_pipeline_tracker(active_idx=1, pipeline_steps=PIPELINE_STEPS)

    stats = get_quick_stats(df)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Rows", f"{stats['rows']:,}")
    c2.metric("Columns", stats["cols"])
    c3.metric("Missing Cells", f"{stats['missing_cells']:,}", f"{stats['missing_pct']:.1f}%")
    c4.metric("Duplicate Rows", f"{stats['duplicates']:,}")
    c5.metric("Numeric Cols", stats["numeric_cols"])

    st.markdown('<div class="studio-card">', unsafe_allow_html=True)
    tab1, tab2, tab3 = st.tabs(["Column Profiles", "Distributions", "Raw Preview"])

    with tab1:
        st.markdown('<div class="studio-card-title">Column-wise Statistics</div>', unsafe_allow_html=True)
        profiles = get_column_profiles(df)
        st.dataframe(pd.DataFrame(profiles), use_container_width=True, hide_index=True)

    with tab2:
        num_df = df.select_dtypes(include=[np.number])
        if num_df.empty:
            st.info("No numeric columns found.")
        else:
            selected_col = st.selectbox("Select numeric column", num_df.columns.tolist())
            series = num_df[selected_col].dropna()
            col_a, col_b = st.columns([2, 1])
            with col_a:
                try:
                    import altair as alt

                    hist_df = pd.DataFrame({"value": series})
                    chart = alt.Chart(hist_df).mark_bar(color="#4f8eff", opacity=0.82).encode(
                        x=alt.X("value:Q", bin=alt.Bin(maxbins=40), title=selected_col),
                        y=alt.Y("count()", title="Count"),
                    ).properties(height=260)
                    st.altair_chart(chart, use_container_width=True)
                except Exception:
                    st.bar_chart(series.value_counts().sort_index())
            with col_b:
                st.markdown('<div class="studio-card-title">Stats</div>', unsafe_allow_html=True)
                st.write(f"**Mean:** {series.mean():.3f}")
                st.write(f"**Median:** {series.median():.3f}")
                st.write(f"**Std:** {series.std():.3f}")
                st.write(f"**Min:** {series.min()}")
                st.write(f"**Max:** {series.max()}")

    with tab3:
        render_excel_table(df)

    st.markdown("</div>", unsafe_allow_html=True)

    nav1, _ = st.columns([1, 4])
    if nav1.button("Continue to Validate", use_container_width=True):
        st.session_state[CURRENT_PAGE] = "Validate"
        st.rerun()
