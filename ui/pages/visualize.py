import streamlit as st
import pandas as pd
import numpy as np
from src.core.constants import PIPELINE_STEPS
from src.core.session_state import RAW_DF, CLEAN_DF, PIPELINE_STAGE, CURRENT_PAGE
from ui.components.pipeline_tracker import render_pipeline_tracker
from src.engine.comparison import compare_datasets, get_distribution_data

def render():
    """Renders the Visualize page."""
    raw_df   = st.session_state.get(RAW_DF)
    clean_df = st.session_state.get(CLEAN_DF)

    if raw_df is None:
        st.warning("Upload a dataset first.")
        return
    if clean_df is None:
        st.warning("Run the cleaning pipeline first.")
        return

    st.markdown(
        """
        <div class="studio-shell">
            <div class="studio-head">
                <div>
                    <p class="studio-title">Visualization Studio</p>
                    <p class="studio-sub">Comparative analysis and distribution shifts between raw and clean data.</p>
                </div>
                <span class="studio-chip">Insight</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )
    render_pipeline_tracker(active_idx=4, pipeline_steps=PIPELINE_STEPS)

    st.markdown('<div class="studio-card">', unsafe_allow_html=True)
    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 Raw Data",
        "✅ Clean Data",
        "🔄 Cell Diff",
        "📊 Distributions"
    ])

    with tab1:
        st.markdown(f'<div class="studio-card-title">Original — {len(raw_df):,} rows × {len(raw_df.columns)} cols</div>', unsafe_allow_html=True)
        st.info("ℹ️ Raw data is read-only. Never modified by the pipeline.")
        st.dataframe(raw_df.head(200), use_container_width=True, height=380)

    with tab2:
        st.markdown(f'<div class="studio-card-title">Cleaned — {len(clean_df):,} rows × {len(clean_df.columns)} cols</div>', unsafe_allow_html=True)
        st.dataframe(clean_df.head(200), use_container_width=True, height=380)

    with tab3:
        st.markdown('<div class="studio-card-title">Cell-level Changes</div>', unsafe_allow_html=True)
        
        # Use engine-side comparison
        comp_res = compare_datasets(raw_df, clean_df)
        diff_rows = comp_res["column_diffs"]
        total_changed = comp_res["total_changed_cells"]
        total_cells = comp_res["total_cells_compared"]
        pct = comp_res["total_change_pct"]

        if not diff_rows:
            st.info("No shared columns to compare.")
        else:
            diff_df = pd.DataFrame(diff_rows).sort_values("Changed Cells", ascending=False)
            st.dataframe(diff_df, use_container_width=True, hide_index=True)
            st.caption(f"Total cells changed: **{total_changed:,}** of {total_cells:,} ({pct}%)")

    with tab4:
        st.markdown('<div class="studio-card-title">Distribution Shifts</div>', unsafe_allow_html=True)
        num_raw   = raw_df.select_dtypes(include=[np.number])
        num_clean = clean_df.select_dtypes(include=[np.number])
        shared_num = [c for c in num_raw.columns if c in num_clean.columns]

        if not shared_num:
            st.info("No shared numeric columns to compare.")
        else:
            selected = st.selectbox("Select numeric column", shared_num)
            try:
                import altair as alt

                s_raw = get_distribution_data(raw_df, selected)
                s_clean = get_distribution_data(clean_df, selected)
                
                raw_hist = pd.DataFrame({"value": s_raw, "dataset": "Raw"})
                cln_hist = pd.DataFrame({"value": s_clean, "dataset": "Clean"})
                combined = pd.concat([raw_hist, cln_hist])

                chart = alt.Chart(combined).mark_bar(opacity=0.7, cornerRadiusTopLeft=2,
                                                      cornerRadiusTopRight=2).encode(
                    x=alt.X("value:Q", bin=alt.Bin(maxbins=40), title=selected),
                    y=alt.Y("count()", stack=None, title="Count"),
                    color=alt.Color("dataset:N", scale=alt.Scale(
                        domain=["Raw","Clean"], range=["#ef4444","#22c55e"]
                    )),
                    tooltip=["dataset:N", "count()"]
                ).properties(height=260)
                st.altair_chart(chart, use_container_width=True)

                # Stats side by side
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("**Raw Stats**")
                    st.write(f"Mean: {s_raw.mean():.3f}")
                    st.write(f"Std:  {s_raw.std():.3f}")
                with c2:
                    st.markdown("**Clean Stats**")
                    st.write(f"Mean: {s_clean.mean():.3f}")
                    st.write(f"Std:  {s_clean.std():.3f}")
            except Exception as e:
                st.error(f"Chart error: {e}")
    st.markdown("</div>", unsafe_allow_html=True)

    st.divider()
    nav1, _ = st.columns([1, 4])
    if nav1.button("➡️ Continue to Export", type="primary", use_container_width=True):
        st.session_state[PIPELINE_STAGE] = max(st.session_state.get(PIPELINE_STAGE, 0), 5)
        st.session_state[CURRENT_PAGE] = "Export"
        st.rerun()
