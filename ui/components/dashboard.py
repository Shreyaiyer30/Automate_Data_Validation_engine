
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def render_dashboard():
    """Render main dashboard with data overview"""
    
    df = st.session_state.df_original
    
    st.header("📊 Data Overview")
    
    # Metrics row
    col1, col2, col3, col4, col5 = st.columns(5)
    
    col1.metric("Total Rows", f"{len(df):,}")
    col2.metric("Total Columns", len(df.columns))
    col3.metric("Missing Values", f"{df.isnull().sum().sum():,}")
    col4.metric("Duplicates", f"{df.duplicated().sum():,}")
    col5.metric("Memory (MB)", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f}")
    
    # Data preview
    st.subheader("Data Preview")
    st.dataframe(df.head(100), use_container_width=True)
    
    # Column analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Missing Values by Column")
        missing_data = df.isnull().sum()
        missing_data = missing_data[missing_data > 0].sort_values(ascending=False)
        
        if len(missing_data) > 0:
            fig = px.bar(
                x=missing_data.index,
                y=missing_data.values,
                labels={'x': 'Column', 'y': 'Missing Count'},
                title='Missing Values Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("✅ No missing values!")
    
    with col2:
        st.subheader("Data Types Distribution")
        dtype_counts = df.dtypes.value_counts()
        
        fig = px.pie(
            values=dtype_counts.values,
            names=dtype_counts.index.astype(str),
            title='Data Types Distribution'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Detailed column info
    st.subheader("Column Details")
    
    column_info = []
    for col in df.columns:
        column_info.append({
            'Column': col,
            'Type': str(df[col].dtype),
            'Non-Null': df[col].count(),
            'Null': df[col].isnull().sum(),
            'Null %': f"{(df[col].isnull().sum() / len(df)) * 100:.2f}%",
            'Unique': df[col].nunique(),
            'Duplicates': df[col].duplicated().sum()
        })
    
    column_df = pd.DataFrame(column_info)
    st.dataframe(column_df, use_container_width=True)

