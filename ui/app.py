import streamlit as st
import pandas as pd
import sys
import io
import json
from pathlib import Path

# Add src to path
sys.path.append('.')

from src.data_processor import DataProcessor
from ui.components.sidebar import render_sidebar
from ui.components.dashboard import render_dashboard
from ui.components.type_inspector import render_type_inspector
from ui.components.reports import render_reports
from ui.components.visualization import render_visualization

# Page Config
st.set_page_config(
    page_title="Data Engine v2 | Professional Data Cleaning",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_advanced_css():
    """Load both external stylesheet and advanced inline CSS"""
    # 1. Load style.css from assets
    css_path = Path(__file__).parent / "assets" / "style.css"
    if css_path.exists():
        with open(css_path) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
            
    # 2. Inline advanced animations
    st.markdown("""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    
    /* Global Styles */
    * {
        font-family: 'Inter', sans-serif;
    }
    
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        background-attachment: fixed;
    }
    
    .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
        background: rgba(255, 255, 255, 0.95);
        border-radius: 20px;
        box-shadow: 0 20px 60px rgba(0,0,0,0.3);
        margin-top: 2rem;
        backdrop-filter: blur(10px);
    }
    
    /* Animated Header */
    @keyframes fadeInDown {
        from { opacity: 0; transform: translateY(-30px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    @keyframes pulse {
        0%, 100% { transform: scale(1); }
        50% { transform: scale(1.05); }
    }
    
    @keyframes gradient {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    .hero-header {
        animation: fadeInDown 1s ease-out;
        background: linear-gradient(270deg, #667eea, #764ba2, #f093fb, #4facfe);
        background-size: 800% 800%;
        animation: gradient 8s ease infinite;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    /* Glassmorphism Cards */
    .glass-card {
        background: rgba(255, 255, 255, 0.7);
        backdrop-filter: blur(10px);
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.3);
        padding: 2rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }
    
    .glass-card:hover {
        transform: translateY(-10px);
        box-shadow: 0 12px 40px rgba(102, 126, 234, 0.4);
    }
    
    /* Metric Cards with Gradient */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
        padding: 1.5rem;
        border-radius: 15px;
        border: 2px solid transparent;
        background-clip: padding-box;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }
    
    [data-testid="stMetric"]:hover {
        transform: scale(1.05);
        border-color: #667eea;
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.3);
    }
    
    [data-testid="stMetricValue"] {
        font-size: 2.5rem !important;
        font-weight: 800 !important;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    [data-testid="stMetricLabel"] {
        font-weight: 600 !important;
        color: #555 !important;
        font-size: 0.9rem !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Enhanced Dataframe */
    .stDataFrame {
        border-radius: 15px !important;
        overflow: hidden !important;
        box-shadow: 0 10px 30px rgba(0,0,0,0.15) !important;
        border: none !important;
    }
    
    /* Animated Buttons */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.75rem 2rem !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4) !important;
        transition: all 0.3s ease !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        color: white !important;
    }
    
    /* Feature Cards */
    .feature-card {
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
        border-radius: 20px;
        padding: 2.5rem;
        text-align: center;
        transition: all 0.4s ease;
        border: 2px solid transparent;
        cursor: pointer;
        position: relative;
        overflow: hidden;
    }
    
    .feature-card:hover {
        transform: translateY(-15px) scale(1.02);
        border-color: #667eea;
        box-shadow: 0 15px 40px rgba(102, 126, 234, 0.3);
    }
    
    .feature-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
        animation: pulse 2s infinite;
    }
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: white;
    }
    
    /* Floating Action Button Style */
    .floating-stats {
        position: sticky;
        top: 20px;
        z-index: 999;
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(10px);
        border-radius: 20px;
        padding: 1rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        margin-bottom: 2rem;
    }
    
    .stSpinner > div {
        border-color: #667eea transparent transparent transparent !important;
    }
    </style>
    """, unsafe_allow_html=True)

def create_hero_section():
    """Create animated hero section"""
    st.markdown("""
    <div style='text-align: center; padding: 3rem 0 2rem 0;'>
        <h1 class='hero-header' style='font-size: 4rem; margin-bottom: 0.5rem; font-weight: 900;'>
            🛡️ Data Engine v2
        </h1>
        <p style='font-size: 1.3rem; color: #666; font-weight: 300; letter-spacing: 2px;'>
            PROFESSIONAL DATA VALIDATION & CLEANING PLATFORM
        </p>
        <div style='width: 100px; height: 4px; background: linear-gradient(90deg, #667eea, #764ba2); margin: 1rem auto; border-radius: 2px;'></div>
    </div>
    """, unsafe_allow_html=True)

def create_floating_stats(df_cleaned, report):
    """Create floating stats bar"""
    st.markdown('<div class="floating-stats">', unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("📊 Rows", f"{len(df_cleaned):,}")
    with col2:
        st.metric("📋 Columns", f"{len(df_cleaned.columns)}")
    with col3:
        corrections = report.get('summary', {}).get('total_corrections_made', 0)
        st.metric("🔧 Fixes", f"{corrections:,}", delta=f"-{corrections}", delta_color="inverse")
    with col4:
        missing = report.get('summary', {}).get('total_missing_before', 0)
        st.metric("⚠️ Missing", f"{missing:,}", delta=f"-{missing}", delta_color="inverse")
    with col5:
        outliers = report.get('summary', {}).get('total_outliers_detected', 0)
        st.metric("🎯 Outliers", f"{outliers:,}")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_welcome_page():
    """Create stunning welcome page"""
    st.markdown("<br>", unsafe_allow_html=True)
    
    st.markdown("""
    <div style='text-align: center; padding: 4rem 2rem; background: linear-gradient(135deg, rgba(102, 126, 234, 0.05) 0%, rgba(118, 75, 162, 0.05) 100%); border-radius: 25px; margin-bottom: 3rem;'>
        <h1 style='font-size: 3.5rem; font-weight: 900; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent;'>
            Transform Your Data
        </h1>
        <p style='font-size: 1.4rem; color: #666; margin-top: 1rem; font-weight: 300;'>
            From messy datasets to analysis-ready insights in seconds
        </p>
        <div style='margin-top: 2rem;'>
            <span style='background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 1rem 3rem; border-radius: 50px; font-weight: 600; font-size: 1.1rem; box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4); display: inline-block;'>
                👈 Start by uploading your data
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<h2 style='text-align: center; margin: 3rem 0 2rem 0; font-weight: 700;'>Why Choose Data Engine?</h2>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    
    features = [
        {"icon": "🤖", "title": "AI-Powered Cleaning", "desc": "Smart algorithms automatically detect and fix data quality issues", "gradient": "linear-gradient(135deg, #667eea 0%, #764ba2 100%)"},
        {"icon": "⚡", "title": "Lightning Fast", "desc": "Process thousands of rows in milliseconds with optimized pipelines", "gradient": "linear-gradient(135deg, #f093fb 0%, #f5576c 100%)"},
        {"icon": "📊", "title": "Deep Analytics", "desc": "Comprehensive reports with actionable insights and visualizations", "gradient": "linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)"}
    ]
    
    for col, feature in zip([col1, col2, col3], features):
        with col:
            st.markdown(f"""
            <div class='feature-card'>
                <div class='feature-icon'>{feature['icon']}</div>
                <h3 style='margin: 1rem 0; font-weight: 700; color: #333;'>{feature['title']}</h3>
                <p style='color: #666; line-height: 1.6;'>{feature['desc']}</p>
                <div style='width: 60px; height: 4px; background: {feature['gradient']}; margin: 1rem auto; border-radius: 2px;'></div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("<h2 style='text-align: center; margin: 3rem 0 2rem 0; font-weight: 700;'>Get Started in 5 Steps</h2>", unsafe_allow_html=True)
    
    steps = [
        ("1️⃣", "Upload", "Drag & drop your CSV or Excel file", "#667eea"),
        ("2️⃣", "Analyze", "Review automated quality checks", "#764ba2"),
        ("3️⃣", "Clean", "AI fixes issues automatically", "#f093fb"),
        ("4️⃣", "Visualize", "Explore interactive charts", "#4facfe"),
        ("5️⃣", "Export", "Download cleaned data instantly", "#00f2fe")
    ]
    
    for num, title, desc, color in steps:
        st.markdown(f"""
        <div style='display: flex; align-items: center; padding: 1.5rem; margin: 1rem 0; background: linear-gradient(90deg, rgba(102, 126, 234, 0.05) 0%, rgba(118, 75, 162, 0.05) 100%); border-radius: 15px; border-left: 5px solid {color};'>
            <div style='font-size: 3rem; margin-right: 2rem; opacity: 0.7;'>{num}</div>
            <div>
                <h4 style='margin: 0; font-weight: 700; color: #333;'>{title}</h4>
                <p style='margin: 0.5rem 0 0 0; color: #666;'>{desc}</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

def main():
    load_advanced_css()
    create_hero_section()
    
    # Sidebar & Navigation
    selection, runtime_settings = render_sidebar()

    # Processor Initialization
    if 'processor' not in st.session_state:
        st.session_state.processor = DataProcessor()
    st.session_state.processor.settings.update(runtime_settings)

    # File Upload Section (Moved to Sidebar consistently)
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📤 Upload Dataset")
    uploaded_file = st.sidebar.file_uploader(
        "Choose file", type=["csv", "xlsx", "xls"], key="main_uploader"
    )
    
    if uploaded_file:
        try:
            if 'last_uploaded' not in st.session_state or st.session_state.last_uploaded != uploaded_file.name:
                with st.spinner("🚀 Processing your data..."):
                    uploaded_file.seek(0)
                    df_raw = st.session_state.processor.robust_read_file(uploaded_file)
                    uploaded_file.seek(0)
                    df_cleaned, report = st.session_state.processor.process_data(uploaded_file)
                    
                    st.session_state.df_raw = df_raw
                    st.session_state.df_cleaned = df_cleaned
                    st.session_state.report = report
                    st.session_state.last_uploaded = uploaded_file.name
                st.success("✅ Data processed successfully!")
                st.balloons()
            
            df_raw, df_cleaned, report = st.session_state.df_raw, st.session_state.df_cleaned, st.session_state.report
            create_floating_stats(df_cleaned, report)

            # ROUTES
            if selection == "📁 Upload Dataset":
                st.markdown("## 📁 Dataset Management")
                c1, c2, c3 = st.columns(3)
                for col, (label, val, grad) in zip([c1, c2, c3], [("Filename", uploaded_file.name, "#667eea"), ("Size", f"{uploaded_file.size / 1024:.2f} KB", "#764ba2"), ("Format", uploaded_file.type.split('/')[-1].upper(), "#f093fb")]):
                    col.markdown(f"<div class='glass-card' style='padding:1.5rem;text-align:center;'><h4 style='color:{grad};'>{label}</h4><p style='font-weight:600;'>{val}</p></div>", unsafe_allow_html=True)
                
                t1, t2, t3 = st.tabs(["📄 Raw Data", "✨ Cleaned Data", "📊 Comparison"])
                with t1: st.dataframe(df_raw.head(100), use_container_width=True, height=500)
                with t2: st.dataframe(df_cleaned.head(100), use_container_width=True, height=500)
                with t3:
                    sc1, sc2 = st.columns(2)
                    sc1.markdown("**Before**"); sc1.dataframe(df_raw.head(50), use_container_width=True, height=400)
                    sc2.markdown("**After**"); sc2.dataframe(df_cleaned.head(50), use_container_width=True, height=400)

            elif selection == "📊 Data Overview":
                render_dashboard(report, df_raw, df_cleaned)

            elif selection == "🔍 Type Inspector":
                render_type_inspector(df_cleaned, st.session_state.processor)

            elif selection == "🧹 Cleaning Lab":
                st.markdown("## 🧪 Cleaning Laboratory")
                with st.expander("🔍 Filters & Search", expanded=True):
                    fc1, fc2, fc3 = st.columns(3)
                    cat_cols = df_cleaned.select_dtypes(include='object').columns.tolist()
                    f_col = fc1.selectbox("📂 Filter Category", ["None"] + cat_cols)
                    if f_col != "None":
                        vals = fc1.multiselect(f"Select {f_col}", df_cleaned[f_col].unique())
                        if vals: df_cleaned = df_cleaned[df_cleaned[f_col].isin(vals)]
                    
                    num_cols = df_cleaned.select_dtypes(include='number').columns.tolist()
                    s_col = fc2.selectbox("📊 Sort by", ["None"] + num_cols)
                    if s_col != "None":
                        df_cleaned = df_cleaned.sort_values(by=s_col, ascending=fc2.checkbox("Ascending", True))
                    
                    search = fc3.text_input("🔎 Search")
                    if search:
                        df_cleaned = df_cleaned[df_cleaned.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

                st.dataframe(df_cleaned, use_container_width=True, height=500)
                render_reports(report)

            elif selection == "📉 Visualization":
                render_visualization(df_cleaned)

            elif selection == "📥 Export Results":
                st.markdown("## 📥 Export Results")
                ex1, ex2, ex3 = st.columns(3)
                with ex1:
                    st.markdown("<div class='glass-card' style='text-align:center;'><h3>CSV</h3></div>", unsafe_allow_html=True)
                    st.download_button("Download CSV", df_cleaned.to_csv(index=False).encode('utf-8'), f"cleaned_{uploaded_file.name}", "text/csv", use_container_width=True)
                with ex2:
                    st.markdown("<div class='glass-card' style='text-align:center;'><h3>Excel</h3></div>", unsafe_allow_html=True)
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer: df_cleaned.to_excel(writer, index=False)
                    st.download_button("Download Excel", output.getvalue(), f"cleaned_{uploaded_file.name}.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
                with ex3:
                    st.markdown("<div class='glass-card' style='text-align:center;'><h3>Report</h3></div>", unsafe_allow_html=True)
                    st.download_button("Download Report", json.dumps(report, indent=2).encode('utf-8'), f"report_{uploaded_file.name}.json", "application/json", use_container_width=True)

        except Exception as e:
            st.error("❌ Error Processing File")
            with st.expander("Details"): st.exception(e)
    else:
        create_welcome_page()

if __name__ == "__main__":
    main()