import streamlit as st
import plotly.express as px
import pandas as pd

def render_visualization(df):
    """
    Renders simple, beginner-friendly visualizations.
    """
    st.subheader("📉 Data Visualization")
    st.markdown("Explore your cleaned data through interactive charts.")
    
    if df.empty:
        st.warning("No data available to visualize.")
        return

    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

    viz_tab1, viz_tab2 = st.tabs(["📊 Distributions", "🔗 Relationships"])

    with viz_tab1:
        if numeric_cols:
            col_to_plot = st.selectbox("Select Column to View Distribution", numeric_cols)
            fig = px.histogram(df, x=col_to_plot, nbins=30, title=f"Distribution of {col_to_plot}",
                               color_discrete_sequence=['#4CAF50'], marginal="box")
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("No numeric columns found for distributions.")

    with viz_tab2:
        if len(numeric_cols) >= 2:
            c1, c2 = st.columns(2)
            with c1:
                x_axis = st.selectbox("X Axis", numeric_cols, index=0)
            with c2:
                y_axis = st.selectbox("Y Axis", numeric_cols, index=1 if len(numeric_cols) > 1 else 0)
            
            color_col = None
            if categorical_cols:
                color_col = st.selectbox("Color by (Optional)", ["None"] + categorical_cols)
                if color_col == "None": color_col = None
            
            fig = px.scatter(df, x=x_axis, y=y_axis, color=color_col, 
                             title=f"{y_axis} vs {x_axis}",
                             color_discrete_sequence=px.colors.qualitative.Safe)
            st.plotly_chart(fig, width='stretch')
        else:
            st.info("Need at least two numeric columns for relationship analysis.")
