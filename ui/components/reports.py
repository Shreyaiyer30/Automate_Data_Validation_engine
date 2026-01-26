
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from io import BytesIO
import json

def render_reports():
    """Render reports and export options"""
    
    st.header("📈 Quality Reports")
    
    if st.session_state.df_cleaned is not None:
        original = st.session_state.df_original
        cleaned = st.session_state.df_cleaned
        
        # Comparison metrics
        st.subheader("Before vs After Comparison")
        
        col1, col2, col3, col4 = st.columns(4)
        
        rows_removed = len(original) - len(cleaned)
        missing_fixed = original.isnull().sum().sum() - cleaned.isnull().sum().sum()
        duplicates_removed = original.duplicated().sum() - cleaned.duplicated().sum()
        
        orig_accuracy = 1 - (original.isnull().sum().sum() / (len(original) * len(original.columns)))
        clean_accuracy = 1 - (cleaned.isnull().sum().sum() / (len(cleaned) * len(cleaned.columns)))
        
        col1.metric("Rows Removed", f"{rows_removed:,}", delta=f"-{(rows_removed/len(original)*100):.1f}%")
        col2.metric("Missing Fixed", f"{missing_fixed:,}")
        col3.metric("Duplicates Removed", f"{duplicates_removed:,}")
        col4.metric("Data Accuracy", f"{clean_accuracy:.1%}", delta=f"+{(clean_accuracy-orig_accuracy):.1%}")
        
        # Visual comparison
        st.subheader("Visual Comparison")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Missing values comparison
            fig = go.Figure(data=[
                go.Bar(name='Original', x=['Missing Values'], y=[original.isnull().sum().sum()]),
                go.Bar(name='Cleaned', x=['Missing Values'], y=[cleaned.isnull().sum().sum()])
            ])
            fig.update_layout(title='Missing Values Comparison', barmode='group')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Duplicates comparison
            fig = go.Figure(data=[
                go.Bar(name='Original', x=['Duplicates'], y=[original.duplicated().sum()]),
                go.Bar(name='Cleaned', x=['Duplicates'], y=[cleaned.duplicated().sum()])
            ])
            fig.update_layout(title='Duplicates Comparison', barmode='group')
            st.plotly_chart(fig, use_container_width=True)
        
        # Export section
        st.subheader("💾 Export Data")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # CSV export
            csv = cleaned.to_csv(index=False)
            st.download_button(
                "📥 Download Cleaned CSV",
                csv,
                "cleaned_data.csv",
                "text/csv"
            )
        
        with col2:
            # Excel export
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                cleaned.to_excel(writer, index=False)
            
            st.download_button(
                "📥 Download Cleaned Excel",
                buffer.getvalue(),
                "cleaned_data.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col3:
            # Quality report export
            if st.session_state.quality_report:
                report_json = json.dumps(st.session_state.quality_report, indent=2, default=str)
                st.download_button(
                    "📥 Download Quality Report",
                    report_json,
                    "quality_report.json",
                    "application/json"
                )
    
    else:
        st.info("Clean the data first to view reports and export options")

