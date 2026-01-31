import streamlit as st
import pandas as pd
import json

def render_export_section(df: pd.DataFrame, report: dict):
    """Renders export options for data and reports."""
    st.markdown("## 📥 EXPORT & FINAL REPORT")
    
    st.markdown(f"""
    <div class='glass-card' style='text-align: center; border-top: 4px solid #00ffff;'>
        <h1 style='color: #ffffff;'>{report['quality_score']}%</h1>
        <p style='color: #888;'>Final Data Quality Score</p>
    </div>
    """, unsafe_allow_html=True)
    
    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.download_button("📥 DOWNLOAD CSV", 
                          df.to_csv(index=False).encode('utf-8'),
                          "cleaned_data.csv", "text/csv", use_container_width=True)
    with c2:
        st.download_button("📥 DOWNLOAD JSON REPORT",
                          json.dumps(report, indent=2).encode('utf-8'),
                          "quality_report.json", "application/json", use_container_width=True)
    with c3:
        # Placeholder for Excel
        st.button("📄 DOWNLOAD EXCEL (Coming Soon)", use_container_width=True, disabled=True)
