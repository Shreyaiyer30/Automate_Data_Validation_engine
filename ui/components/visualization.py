import streamlit as st
import pandas as pd
import plotly.express as px

def render_visualization_section(df_raw: pd.DataFrame, df_cleaned: pd.DataFrame):
    """Renders charts showing the impact of cleaning."""
    st.markdown("## 📈 VISUALIZATION")
    
    numeric_cols = df_cleaned.select_dtypes(include=['number']).columns.tolist()
    if not numeric_cols:
        st.warning("No numeric columns available for visualization.")
        return
        
    target_col = st.selectbox("Select Column to Visualize Impact", numeric_cols)
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("**RAW DISTRIBUTION**")
        fig1 = px.histogram(df_raw, x=target_col, color_discrete_sequence=['#ff4444'])
        fig1.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white')
        st.plotly_chart(fig1, use_container_width=True)
        
    with c2:
        st.markdown("**CLEANED DISTRIBUTION**")
        fig2 = px.histogram(df_cleaned, x=target_col, color_discrete_sequence=['#00ff00'])
        fig2.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white')
        st.plotly_chart(fig2, use_container_width=True)
