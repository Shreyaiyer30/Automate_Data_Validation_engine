import streamlit as st
import pandas as pd
import plotly.express as px
from src.utils.helpers import get_column_stats

def render_overview_section(df: pd.DataFrame):
    """Renders insightful overview of raw data."""
    st.markdown("## 📊 DATA OVERVIEW (RAW)")
    
    # High-level stats
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Rows", f"{len(df):,}")
    c2.metric("Total Columns", f"{len(df.columns)}")
    c3.metric("Missing Cells", f"{df.isna().sum().sum():,}")
    c4.metric("Duplicates", f"{df.duplicated().sum():,}")
    
    # Missing Value Heatmap
    st.markdown("### 🗺️ Missing Value Heatmap")
    if df.isna().sum().sum() > 0:
        fig = px.imshow(df.isna().astype(int), 
                       labels=dict(x="Columns", y="Rows", color="Missing"),
                       color_continuous_scale="Viridis")
        fig.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.success("No missing values detected in the entire dataset! 🎉")
    
    # Column-wise profiling cards
    st.markdown("### 📋 Column Profiles")
    cols = st.columns(3)
    for i, col_name in enumerate(df.columns):
        with cols[i % 3]:
            stats = get_column_stats(df[col_name])
            st.markdown(f"""
            <div class='glass-card'>
                <h4 style='color: #ff00ff;'>{col_name}</h4>
                <p><b>Type:</b> {df[col_name].dtype}</p>
                <p><b>Missing:</b> {stats['null_count']} ({stats['null_pct']}%)</p>
                <p><b>Unique:</b> {stats['nunique']}</p>
            </div>
            """, unsafe_allow_html=True)
