import streamlit as st
import pandas as pd
import plotly.express as px

def render_dashboard(report, df_raw, df_cleaned):
    """Render dashboard with date information and high-level trends"""
    
    st.header("📊 Data Overview & Intelligence")
    
    # 1. Date Intelligence Section
    st.markdown("""
    <div class='glass-card' style='margin-bottom: 2rem;'>
        <h3>📅 Date Intelligence</h3>
        <p>The system automatically detects and standardizes date formats for analysis.</p>
    </div>
    """, unsafe_allow_html=True)
    
    date_columns = []
    # Use the report for accurate detection
    date_corrections = report.get('correction_details', {}).get('date_corrections', {})
    
    if date_corrections:
        for col, details in date_corrections.items():
            date_columns.append(col)
            with st.expander(f"📌 Column: {col} Details", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("**Original Samples**")
                    st.dataframe(df_raw[col].head(5).reset_index(drop=True), use_container_width=True)
                with c2:
                    st.markdown("**Standardized Samples**")
                    st.dataframe(df_cleaned[col].head(5).reset_index(drop=True), use_container_width=True)
                
                st.info(f"✅ Corrected as **{details['detected']}** format. Total {details['count']} records standardized.")
    else:
        st.info("No date-specific corrections were required for this dataset.")

    # 2. Quality Metrics Profiling
    st.markdown("---")
    st.subheader("📈 Statistical Profiling")
    
    if 'profiling' in report:
        stats = report['profiling'].get('statistics', {})
        if stats:
            stats_df = pd.DataFrame(stats).T
            st.dataframe(stats_df.style.highlight_max(axis=0, color='rgba(102, 126, 234, 0.2)'), use_container_width=True)
        else:
            st.warning("No numeric columns available for statistical profiling.")

    # 3. Missing Value Analysis
    missing_data = report.get('validation_details', {}).get('missing_values', {})
    if any(v > 0 for v in missing_data.values()):
        st.markdown("### 🔍 Missing Data Heatmap")
        missing_series = pd.Series(missing_data)
        fig = px.bar(
            missing_series, 
            title="Missing Values by Column (Before Cleaning)",
            labels={'index': 'Column', 'value': 'Count'},
            color_discrete_sequence=['#764ba2']
        )
        st.plotly_chart(fig, use_container_width=True)
