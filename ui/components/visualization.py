import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def render_visualization_section(df_raw: pd.DataFrame, df_cleaned: pd.DataFrame, report: dict = None):
    """Renders charts showing the impact of cleaning."""
    st.markdown("## 📈 VISUALIZATION")
    
    # 0. Resolve Schema Mapping (Raw vs Cleaned)
    header_mapping = {}
    if report and 'header_normalization' in report:
        header_mapping = report['header_normalization'].get('mapping', {})
    
    # Invert mapping (Cleaned Name -> Raw Name)
    inv_mapping = {v: k for k, v in header_mapping.items()}
    
    numeric_cols = df_cleaned.select_dtypes(include=['number']).columns.tolist()
    if not numeric_cols:
        st.warning("No numeric columns available for visualization.")
        return
        
    target_col = st.selectbox("Select Column to Visualize Impact", numeric_cols)
    raw_col = inv_mapping.get(target_col, target_col)
    
    # Safety Check: Ensure raw_col exists in df_raw
    if raw_col not in df_raw.columns:
        st.error(f"Schema mismatch: Column '{raw_col}' not found in raw data.")
        return

    # 1. Distribution Comparison (Overlaid Histogram)
    st.markdown("### 📊 Distribution Comparison")
    
    view_type = st.radio("View Type", ["Overlaid", "Side-by-Side"], horizontal=True)
    
    if view_type == "Overlaid":
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=df_raw[raw_col], name='Raw', marker_color='#ff4444', opacity=0.5))
        fig.add_trace(go.Histogram(x=df_cleaned[target_col], name='Cleaned', marker_color='#00ff00', opacity=0.5))
        
        fig.update_layout(
            barmode='overlay',
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**RAW**")
            fig1 = px.histogram(df_raw, x=raw_col, color_discrete_sequence=['#ff4444'], opacity=0.7)
            fig1.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white')
            st.plotly_chart(fig1, use_container_width=True)
        with c2:
            st.markdown("**CLEANED**")
            fig2 = px.histogram(df_cleaned, x=target_col, color_discrete_sequence=['#00ff00'], opacity=0.7)
            fig2.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white')
            st.plotly_chart(fig2, use_container_width=True)
        
    # 2. Outlier Comparison (Box Plot)
    st.markdown("### 📦 Outlier Comparison (Box Plots)")
    
    # Prepare combined data for box plot (aligning headers to cleaned name for consistent axis)
    raw_sub = pd.DataFrame({target_col: df_raw[raw_col], 'State': 'Raw'})
    clean_sub = pd.DataFrame({target_col: df_cleaned[target_col], 'State': 'Cleaned'})
    combined = pd.concat([raw_sub, clean_sub])
    
    fig_box = px.box(combined, x='State', y=target_col, color='State', 
                     color_discrete_map={'Raw': '#ff4444', 'Cleaned': '#00ff00'})
    fig_box.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white')
    st.plotly_chart(fig_box, use_container_width=True)

    # 3. Missing Values Heatmap (Cleaned)
    st.markdown("### 🗺️ Missing Value Heatmap (Cleaned)")
    if df_cleaned.isna().sum().sum() > 0:
        fig_heat = px.imshow(df_cleaned.isna().astype(int), 
                            labels=dict(x="Columns", y="Rows", color="Missing"),
                            color_continuous_scale="Viridis")
        fig_heat.update_layout(plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white')
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.success("No missing values in cleaned data! 🎉")
