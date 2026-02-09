import streamlit as st
import pandas as pd
import plotly.express as px
from src.utils import get_column_stats

def render_overview_section(df: pd.DataFrame):
    """Renders insightful overview of raw data with required analytics."""
    st.markdown("## 📊 DATA OVERVIEW (RAW)")
    
    # 1. High-level stats & Quality Score
    analysis = st.session_state.processor.analyze_data_quality(df)
    quality_score = analysis.get('quality_score', 0)
    
    # Structural Resolution (Duplication Contract Compliance)
    resolution = analysis.get('resolution_summary', {})
    if resolution:
        st.warning("🔄 **Duplicate columns were detected and merged automatically.**")
        with st.expander("📝 View Resolution Summary", expanded=False):
            st.markdown("The system dynamically resolved these groups while preserving row integrity:")
            for canonical, info in resolution.items():
                st.markdown(f"- **{canonical}** (Selected) ← Merged from: `{', '.join(info['merged_from'])}`")
                st.caption(f"Strategy: `{info['strategy']}` | Selection Score: `{info.get('canonical_score', 'N/A')}` | Value Conflicts: `{info.get('conflict_count', 0)}`")

    # KPI Grid
    st.markdown("""
    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 30px;">
        <div class="glass-card" style="border-top: 4px solid #00ffff; text-align: center; padding: 15px;">
            <p style="color: #666; margin: 0; font-size: 0.8rem; text-transform: uppercase;">Quality Score</p>
            <h2 style="margin: 10px 0; color: #fff;">{quality_score:.1f}%</h2>
        </div>
        <div class="glass-card" style="border-top: 4px solid #ff00ff; text-align: center; padding: 15px;">
            <p style="color: #666; margin: 0; font-size: 0.8rem; text-transform: uppercase;">Total Rows</p>
            <h2 style="margin: 10px 0; color: #fff;">{row_count:,}</h2>
        </div>
        <div class="glass-card" style="border-top: 4px solid #ff9900; text-align: center; padding: 15px;">
            <p style="color: #666; margin: 0; font-size: 0.8rem; text-transform: uppercase;">Columns</p>
            <h2 style="margin: 10px 0; color: #fff;">{col_count}</h2>
        </div>
        <div class="glass-card" style="border-top: 4px solid #ff4b4b; text-align: center; padding: 15px;">
            <p style="color: #666; margin: 0; font-size: 0.8rem; text-transform: uppercase;">Missing Cells</p>
            <h2 style="margin: 10px 0; color: #fff;">{missing_cells:,}</h2>
        </div>
        <div class="glass-card" style="border-top: 4px solid #00ff00; text-align: center; padding: 15px;">
            <p style="color: #666; margin: 0; font-size: 0.8rem; text-transform: uppercase;">Duplicate Rows</p>
            <h2 style="margin: 10px 0; color: #fff;">{dup_rows:,}</h2>
        </div>
    </div>
    """.format(
        quality_score=quality_score, 
        row_count=len(df), 
        col_count=len(df.columns), 
        missing_cells=df.isna().sum().sum(), 
        dup_rows=df.duplicated().sum()
    ), unsafe_allow_html=True)
    
    st.markdown("---")
    
    # 1.5 Raw Data Preview (Phase 4: Original Header Preservation Rule)
    st.markdown("### 📑 Raw Data Sample")
    st.dataframe(df.head(100), use_container_width=True, height=300)
    
    st.markdown("---")
    
    # 2. Missing Value Heatmap
    st.markdown("### 🗺️ Missing Value Heatmap")
    if df.isna().sum().sum() > 0:
        # Optimization for very large datasets
        plot_df = df
        if len(df) > 1000:
            plot_df = df.sample(1000)
            st.info("Showing heatmap for a sample of 1,000 rows.")
            
        fig = px.imshow(plot_df.isna().astype(int), 
                       labels=dict(x="Columns", y="Rows", color="Missing"),
                       color_continuous_scale="Viridis",
                       aspect="auto")
        fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.success("No missing values detected in the entire dataset! 🎉")
    
    # 3. Column-wise profiling cards (Required Analytics)
    st.markdown("### 📋 Column Profiles")
    cols = st.columns(3)
    for i, col_name in enumerate(df.columns):
        with cols[i % 3]:
            stats = get_column_stats(df[col_name])
            
            # Build stats display
            stats_html = f"""
            <div class='glass-card'>
                <h4 style='color: #ff00ff; margin-bottom: 10px;'>{col_name}</h4>
                <p style='font-size: 0.9rem;'><b>Type:</b> {stats['dtype']}</p>
                <p style='font-size: 0.9rem;'><b>Missing:</b> {stats['null_count']} ({stats['null_pct']}%)</p>
                <p style='font-size: 0.9rem;'><b>Unique:</b> {stats['nunique']}</p>
            """
            
            if 'min' in stats:
                stats_html += f"""
                <hr style='opacity: 0.2; margin: 10px 0;'>
                <p style='font-size: 0.8rem;'><b>Min:</b> {stats['min']:.2f} | <b>Max:</b> {stats['max']:.2f}</p>
                <p style='font-size: 0.8rem;'><b>Mean:</b> {stats['mean']:.2f}</p>
                <p style='font-size: 0.8rem; color: #ff4444;'><b>Outliers:</b> {stats.get('outlier_count', 0)}</p>
                """
            
            stats_html += "</div>"
            st.markdown(stats_html, unsafe_allow_html=True)
