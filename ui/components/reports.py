import streamlit as st
import json

def render_reports(report):
    st.markdown("### 📋 Detailed Inspection")
    
    with st.expander("Validation Report (Schema & Types)", expanded=False):
        st.json(report['validation_details'])
        
    with st.expander("Correction Summary (What was fixed)", expanded=True):
        st.write("Below is a breakdown of the automated corrections applied to the dataset.")
        st.json(report['correction_details'])
        
    with st.expander("Outlier Detection (Z-score Analysis)", expanded=False):
        if report['outlier_details']:
            st.write("The following columns contained statistical outliers that were capped:")
            st.json(report['outlier_details'])
        else:
            st.success("No significant outliers detected based on the current threshold.")
            
    with st.expander("Full JSON Quality Report", expanded=False):
        st.download_button(
            "Download Full Report",
            data=json.dumps(report, indent=4),
            file_name="quality_report.json",
            mime="application/json"
        )
        st.json(report)
    
    with st.expander("Full JSON Quality Report", expanded=False):
        st.download_button(
            "Download Full Report",
            data=json.dumps(report, indent=4),
            file_name="quality_report.json",
            mime="application/json"
        )
        st.json(report)
    