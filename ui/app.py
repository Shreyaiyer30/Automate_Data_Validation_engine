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
from ui.components.dynamic_rules import render_dynamic_rules_editor

# Page Config
st.set_page_config(
    page_title="Data Engine v2 | Professional Data Cleaning",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_dark_theme_css():
    """Load dark theme CSS with white text on black background"""
    # 1. Load style.css from assets
    css_path = Path(__file__).parent / "assets" / "style.css"
    if css_path.exists():
        with open(css_path) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
            
    # 2. Inline dark theme CSS
    st.markdown("""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    
    /* DARK THEME - Black background, White text */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        color: #ffffff !important;
        background-color: #0a0a0a !important;
    }
    
    /* Main app background */
    .main {
        background-color: #0a0a0a !important;
        color: #ffffff !important;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0a0a0a 0%, #1a1a1a 100%) !important;
        min-height: 100vh;
    }
    
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        background: transparent !important;
        border-radius: 0;
        box-shadow: none;
        margin-top: 0;
        backdrop-filter: none;
    }
    
    /* Dark theme text colors */
    h1, h2, h3, h4, h5, h6, p, div, span, label {
        color: #ffffff !important;
    }
    
    /* Animated Header - Neon gradient */
    @keyframes fadeInDown {
        from { opacity: 0; transform: translateY(-30px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    @keyframes pulse {
        0%, 100% { transform: scale(1); }
        50% { transform: scale(1.05); }
    }
    
    @keyframes neonGlow {
        0%, 100% { text-shadow: 0 0 10px #00ffff, 0 0 20px #00ffff, 0 0 30px #00ffff; }
        50% { text-shadow: 0 0 20px #ff00ff, 0 0 30px #ff00ff, 0 0 40px #ff00ff; }
    }
    
    @keyframes gradient {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    .hero-header {
        animation: fadeInDown 1s ease-out;
        background: linear-gradient(270deg, #00ffff, #ff00ff, #ffff00, #00ff00);
        background-size: 800% 800%;
        animation: gradient 8s ease infinite;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-weight: 800 !important;
        text-shadow: 0 2px 10px rgba(0, 255, 255, 0.3);
    }
    
    /* Dark Glassmorphism Cards */
    .glass-card {
        background: rgba(30, 30, 30, 0.8) !important;
        backdrop-filter: blur(10px);
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        padding: 2rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        transition: all 0.3s ease;
        margin-bottom: 1.5rem;
        color: #ffffff !important;
    }
    
    .glass-card:hover {
        transform: translateY(-5px);
        border-color: rgba(0, 255, 255, 0.3) !important;
        box-shadow: 0 12px 48px rgba(0, 255, 255, 0.2);
    }
    
    /* Dark Metric Cards */
    [data-testid="stMetric"] {
        background: rgba(40, 40, 40, 0.9) !important;
        backdrop-filter: blur(8px);
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
        color: #ffffff !important;
    }
    
    [data-testid="stMetric"]:hover {
        transform: scale(1.02);
        border-color: #00ffff !important;
        box-shadow: 0 8px 30px rgba(0, 255, 255, 0.2);
    }
    
    [data-testid="stMetricValue"] {
        font-size: 2rem !important;
        font-weight: 700 !important;
        color: #ffffff !important;
        text-shadow: 0 2px 4px rgba(0, 0, 0, 0.5);
    }
    
    [data-testid="stMetricLabel"] {
        font-weight: 500 !important;
        color: #cccccc !important;
        font-size: 0.85rem !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    [data-testid="stMetricDelta"] {
        # color: #00ff00 !important;
        # font-weight: 600 !important;
        color: #555 !important;
        font-size: 0.9rem !important;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    /* Dark Dataframe */
    .stDataFrame {
        border-radius: 12px !important;
        overflow: hidden !important;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4) !important;
        background: #1a1a1a !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
    }
    
    /* Dataframe table styling */
    .stDataFrame table {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
    }
    
    .stDataFrame th {
        background-color: #2a2a2a !important;
        color: #ffffff !important;
        border-bottom: 2px solid #00ffff !important;
    }
    
    .stDataFrame td {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
        border-bottom: 1px solid rgba(255, 255, 255, 0.1) !important;
    }
    
    .stDataFrame tr:hover td {
        background-color: rgba(0, 255, 255, 0.1) !important;
    }
    
    /* Dark Buttons */
    .stButton > button {
        border-radius: 8px !important;
        padding: 0.5rem 1.5rem !important;
        font-weight: 500 !important;
        transition: all 0.2s ease !important;
        background: linear-gradient(135deg, #00ffff, #0088ff) !important;
        color: #000000 !important;
        border: none !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(0, 255, 255, 0.4) !important;
    }
    
    .stDownloadButton > button {
        background: linear-gradient(135deg, #ff00ff, #8800ff) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.6rem 1.5rem !important;
        font-weight: 500 !important;
        box-shadow: 0 4px 20px rgba(255, 0, 255, 0.3) !important;
    }
    
    .stDownloadButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 30px rgba(255, 0, 255, 0.5) !important;
    }
    
    /* Dark Sidebar */
    [data-testid="stSidebar"] {
        background: rgba(20, 20, 20, 0.95) !important;
        backdrop-filter: blur(10px);
        border-right: 1px solid rgba(255, 255, 255, 0.1) !important;
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] * {
        color: #ffffff !important;
    }
    
    /* Sidebar widgets */
    [data-testid="stSidebar"] .stSelectbox > div,
    [data-testid="stSidebar"] .stMultiSelect > div,
    [data-testid="stSidebar"] .stTextInput > div,
    [data-testid="stSidebar"] .stSlider > div > div {
        background-color: #2a2a2a !important;
        border-color: rgba(255, 255, 255, 0.2) !important;
        color: #ffffff !important;
    }
    
    [data-testid="stSidebar"] .stSelectbox > div:hover,
    [data-testid="stSidebar"] .stMultiSelect > div:hover {
        border-color: #00ffff !important;
        box-shadow: 0 0 0 3px rgba(0, 255, 255, 0.2) !important;
    }
    
    /* Radio buttons and checkboxes in sidebar */
    [data-testid="stSidebar"] .stRadio > label,
    [data-testid="stSidebar"] .stCheckbox > label {
        color: #ffffff !important;
    }
    
    /* Floating stats bar */
    .floating-stats {
        position: sticky;
        top: 0;
        z-index: 999;
        background: rgba(30, 30, 30, 0.9) !important;
        backdrop-filter: blur(10px);
        border-bottom: 1px solid rgba(255, 255, 255, 0.1) !important;
        padding: 1rem 2rem;
        margin-bottom: 2rem;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        background-color: transparent !important;
        border-bottom: 2px solid rgba(255, 255, 255, 0.1) !important;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent !important;
        color: #cccccc !important;
        border: none !important;
        transition: all 0.3s ease !important;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        color: #00ffff !important;
        background-color: rgba(0, 255, 255, 0.1) !important;
    }
    
    .stTabs [aria-selected="true"] {
        color: #00ffff !important;
        border-bottom: 2px solid #00ffff !important;
        font-weight: 600 !important;
    }
    
    /* Expanders */
    .streamlit-expanderHeader {
        background: rgba(40, 40, 40, 0.8) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        color: #ffffff !important;
        border-radius: 8px !important;
    }
    
    .streamlit-expanderHeader:hover {
        border-color: #00ffff !important;
        background: rgba(40, 40, 40, 1) !important;
    }
    
    .streamlit-expanderContent {
        background: rgba(30, 30, 30, 0.8) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        border-top: none !important;
        border-radius: 0 0 8px 8px !important;
    }
    
    /* Progress bar */
    [data-testid="stProgress"] > div > div {
        background: linear-gradient(90deg, #00ffff, #ff00ff) !important;
        border-radius: 8px !important;
    }
    
    /* Text area and inputs */
    .stTextArea textarea,
    .stTextInput input {
        background-color: #2a2a2a !important;
        color: #ffffff !important;
        border-color: rgba(255, 255, 255, 0.2) !important;
    }
    
    .stTextArea textarea:focus,
    .stTextInput input:focus {
        border-color: #00ffff !important;
        box-shadow: 0 0 0 3px rgba(0, 255, 255, 0.2) !important;
    }
    
    /* File uploader */
    .stFileUploader {
        border: 2px dashed rgba(0, 255, 255, 0.3) !important;
        background: rgba(40, 40, 40, 0.5) !important;
        border-radius: 12px !important;
    }
    
    .stFileUploader:hover {
        border-color: #00ffff !important;
        background: rgba(40, 40, 40, 0.8) !important;
    }
    
    /* Success/Error/Warning messages */
    .stAlert {
        border-radius: 8px !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
    }
    
    .element-container div[data-testid="stSuccess"] {
        background: rgba(0, 255, 0, 0.1) !important;
        border-left: 4px solid #00ff00 !important;
    }
    
    .element-container div[data-testid="stError"] {
        background: rgba(255, 0, 0, 0.1) !important;
        border-left: 4px solid #ff0000 !important;
    }
    
    .element-container div[data-testid="stWarning"] {
        background: rgba(255, 255, 0, 0.1) !important;
        border-left: 4px solid #ffff00 !important;
    }
    
    .element-container div[data-testid="stInfo"] {
        background: rgba(0, 255, 255, 0.1) !important;
        border-left: 4px solid #00ffff !important;
    }
    
    /* Divider */
    hr {
        border-color: rgba(255, 255, 255, 0.1) !important;
    }
    
    /* Code blocks */
    code {
        background: rgba(0, 0, 0, 0.5) !important;
        color: #00ffff !important;
        border: 1px solid rgba(0, 255, 255, 0.3) !important;
        border-radius: 4px !important;
        padding: 2px 6px !important;
        font-family: 'JetBrains Mono', monospace !important;
    }
    
    /* Scrollbar styling */
    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }
    
    ::-webkit-scrollbar-track {
        background: #1a1a1a;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #444;
        border-radius: 5px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #666;
    }
    
    /* Responsive adjustments */
    @media (max-width: 768px) {
        .hero-header {
            font-size: 2.5rem !important;
        }
        
        .glass-card {
            padding: 1rem !important;
        }
        
        [data-testid="stMetricValue"] {
            font-size: 1.5rem !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)

def create_hero_section():
    """Create dark theme hero section"""
    st.markdown("""
    <div style='text-align: center; padding: 2rem 0;'>
        <h1 class='hero-header' style='font-size: 3.5rem; margin-bottom: 0.5rem; font-weight: 800;'>
            🛡️ DATA ENGINE v2
        </h1>
        <p style='font-size: 1.1rem; color: #cccccc; font-weight: 400; letter-spacing: 1px; text-shadow: 0 2px 4px rgba(0,0,0,0.5);'>
            PROFESSIONAL DATA VALIDATION & CLEANING PLATFORM
        </p>
        <div style='margin-top: 1rem;'>
            <span style='font-size: 0.9rem; color: #888; font-weight: 300;'>
                Transform your data with AI-powered precision
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

def create_floating_stats(df_cleaned, report):
    """Create dark theme floating stats bar"""
    st.markdown('<div class="floating-stats">', unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("📊 Rows", f"{len(df_cleaned):,}")
    with col2:
        st.metric("📋 Columns", f"{len(df_cleaned.columns)}")
    with col3:
        corrections = report.get('summary', {}).get('total_corrections_made', 0)
        st.metric("🔧 Fixes", f"{corrections:,}")
    with col4:
        missing = report.get('summary', {}).get('total_missing_before', 0)
        st.metric("⚠️ Missing", f"{missing:,}")
    with col5:
        outliers = report.get('summary', {}).get('total_outliers_detected', 0)
        st.metric("🎯 Outliers", f"{outliers:,}")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_welcome_page():
    """Create dark theme welcome page"""
    st.markdown("""
    <div style='text-align: center; padding: 4rem 2rem; background: rgba(30, 30, 30, 0.8); backdrop-filter: blur(10px); border-radius: 16px; border: 1px solid rgba(255, 255, 255, 0.1); box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4); margin-bottom: 3rem;'>
        <h1 style='font-size: 3rem; font-weight: 800; color: #ffffff; margin-bottom: 1rem;'>
            TRANSFORM YOUR DATA
        </h1>
        <p style='font-size: 1.2rem; color: #cccccc; margin-top: 1rem; font-weight: 400; line-height: 1.6;'>
            From messy datasets to analysis-ready insights in seconds<br>
            <span style='color: #00ffff; font-weight: 500;'>Powered by AI-driven automation</span>
        </p>
        <div style='margin-top: 3rem;'>
            <div style='background: linear-gradient(135deg, rgba(0, 255, 255, 0.2), rgba(255, 0, 255, 0.2)); padding: 1.5rem; border-radius: 12px; border: 1px solid rgba(0, 255, 255, 0.3); display: inline-block; max-width: 500px;'>
                <span style='color: #ffffff; font-weight: 500; font-size: 1.1rem;'>
                    👈 START BY UPLOADING YOUR DATA IN THE SIDEBAR
                </span>
                <div style='margin-top: 0.5rem;'>
                    <span style='color: #888; font-size: 0.9rem;'>
                        Supports CSV, Excel, and more
                    </span>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<h2 style='text-align: center; margin-bottom: 2rem; font-weight: 700; color: #ffffff;'>WHY CHOOSE DATA ENGINE?</h2>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    features = [
        {
            "icon": "🤖", 
            "title": "AI-POWERED CLEANING", 
            "desc": "Smart algorithms automatically detect and fix data quality issues",
            "color": "#00ffff"
        },
        {
            "icon": "⚡", 
            "title": "LIGHTNING FAST", 
            "desc": "Process thousands of rows in milliseconds with optimized pipelines",
            "color": "#ff00ff"
        },
        {
            "icon": "📊", 
            "title": "DEEP ANALYTICS", 
            "desc": "Comprehensive reports with actionable insights and visualizations",
            "color": "#ffff00"
        }
    ]
    
    for col, feature in zip([col1, col2, col3], features):
        with col:
            st.markdown(f"""
            <div class='glass-card' style='text-align: center; height: 100%; border-top: 4px solid {feature["color"]};'>
                <div style='font-size: 3rem; margin-bottom: 1rem;'>{feature['icon']}</div>
                <h3 style='margin-bottom: 1rem; font-weight: 700; color: {feature["color"]};'>{feature['title']}</h3>
                <p style='color: #cccccc; line-height: 1.5; font-size: 0.95rem;'>{feature['desc']}</p>
            </div>
            """, unsafe_allow_html=True)
    
    # Additional stats in dark theme
    st.markdown("""
    <div style='margin-top: 3rem; padding: 2rem; background: rgba(40, 40, 40, 0.8); border-radius: 12px; border: 1px solid rgba(255, 255, 255, 0.1);'>
        <div style='display: flex; justify-content: space-around; text-align: center;'>
            <div>
                <div style='font-size: 2.5rem; color: #00ffff; font-weight: 700;'>98%</div>
                <div style='color: #888; font-size: 0.9rem;'>Accuracy Rate</div>
            </div>
            <div>
                <div style='font-size: 2.5rem; color: #ff00ff; font-weight: 700;'>80%</div>
                <div style='color: #888; font-size: 0.9rem;'>Time Saved</div>
            </div>
            <div>
                <div style='font-size: 2.5rem; color: #ffff00; font-weight: 700;'>100K+</div>
                <div style='color: #888; font-size: 0.9rem;'>Records Processed</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

def main():
    load_dark_theme_css()
    create_hero_section()
    
    # Sidebar & Navigation
    selection, runtime_settings = render_sidebar()

    # Processor Initialization
    if 'processor' not in st.session_state:
        st.session_state.processor = DataProcessor()
    st.session_state.processor.settings.update(runtime_settings)

    # File Upload Section
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📤 UPLOAD DATASET")
    uploaded_file = st.sidebar.file_uploader(
        "Choose file", type=["csv", "xlsx", "xls"], key="main_uploader"
    )
    
    if uploaded_file:
        try:
            if 'last_uploaded' not in st.session_state or st.session_state.last_uploaded != uploaded_file.name:
                with st.spinner("🚀 ANALYZING YOUR DATA..."):
                    uploaded_file.seek(0)
                    df_raw = st.session_state.processor.robust_read_file(uploaded_file)
                    uploaded_file.seek(0)
                    df_cleaned, report = st.session_state.processor.process_data(uploaded_file)
                    
                    st.session_state.df_raw = df_raw
                    st.session_state.df_cleaned = df_cleaned
                    st.session_state.report = report
                    st.session_state.last_uploaded = uploaded_file.name
                    st.session_state.cleaning_applied = False
                st.success("✅ DATA ANALYZED SUCCESSFULLY!")
            
            df_raw, df_cleaned, report = st.session_state.df_raw, st.session_state.df_cleaned, st.session_state.report
            
            # Dynamic Rules Editor
            if not st.session_state.get('cleaning_applied', False):
                st.markdown("<div class='glass-card'>", unsafe_allow_html=True)
                st.markdown("<h3 style='color: #00ffff;'>🔧 DYNAMIC CLEANING RULES</h3>", unsafe_allow_html=True)
                final_rules = render_dynamic_rules_editor(df_raw, st.session_state.processor)
                if st.button("🚀 APPLY DYNAMIC CLEANING", use_container_width=True, type="primary"):
                    with st.spinner("CLEANING DATA..."):
                        df_cleaned = st.session_state.processor.clean_data(df_raw)
                        st.session_state.df_cleaned = df_cleaned
                        st.session_state.cleaning_applied = True
                        st.success("✨ DATA CLEANED SUCCESSFULLY!")
                        st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)

            create_floating_stats(df_cleaned, report)

            # ROUTES
            if selection == "📁 UPLOAD DATASET":
                st.markdown("## 📁 DATASET MANAGEMENT")
                c1, c2, c3 = st.columns(3)
                with c1: 
                    st.markdown(f"""
                    <div class='glass-card' style='padding:1rem;text-align:center;border-top:3px solid #00ffff;'>
                        <h4 style='color:#00ffff;'>FILENAME</h4>
                        <p style='color:#ffffff;font-family:monospace;'>{uploaded_file.name}</p>
                    </div>
                    """, unsafe_allow_html=True)
                with c2: 
                    st.markdown(f"""
                    <div class='glass-card' style='padding:1rem;text-align:center;border-top:3px solid #ff00ff;'>
                        <h4 style='color:#ff00ff;'>SIZE</h4>
                        <p style='color:#ffffff;font-family:monospace;'>{uploaded_file.size / 1024:.2f} KB</p>
                    </div>
                    """, unsafe_allow_html=True)
                with c3: 
                    st.markdown(f"""
                    <div class='glass-card' style='padding:1rem;text-align:center;border-top:3px solid #ffff00;'>
                        <h4 style='color:#ffff00;'>FORMAT</h4>
                        <p style='color:#ffffff;font-family:monospace;'>{uploaded_file.type.split('/')[-1].upper()}</p>
                    </div>
                    """, unsafe_allow_html=True)
                
                t1, t2, t3 = st.tabs(["📄 RAW DATA", "✨ CLEANED DATA", "📊 COMPARISON"])
                with t1: 
                    st.dataframe(df_raw.head(100), use_container_width=True, height=500)
                with t2: 
                    st.dataframe(df_cleaned.head(100), use_container_width=True, height=500)
                with t3:
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        st.markdown("**BEFORE CLEANING**")
                        st.dataframe(df_raw.head(50), use_container_width=True, height=400)
                    with sc2:
                        st.markdown("**AFTER CLEANING**")
                        st.dataframe(df_cleaned.head(50), use_container_width=True, height=400)

            elif selection == "📊 DATA OVERVIEW":
                render_dashboard(report, df_raw, df_cleaned)

            elif selection == "🔍 TYPE INSPECTOR":
                render_type_inspector(df_cleaned, st.session_state.processor)

            elif selection == "🧹 CLEANING LAB":
                st.markdown("## 🧪 CLEANING LABORATORY")
                with st.expander("🔍 FILTERS & SEARCH", expanded=True):
                    fc1, fc2, fc3 = st.columns(3)
                    cat_cols = df_cleaned.select_dtypes(include='object').columns.tolist()
                    f_col = fc1.selectbox("📂 FILTER CATEGORY", ["None"] + cat_cols)
                    if f_col != "None":
                        vals = fc1.multiselect(f"SELECT {f_col}", df_cleaned[f_col].unique())
                        if vals: df_cleaned = df_cleaned[df_cleaned[f_col].isin(vals)]
                    
                    num_cols = df_cleaned.select_dtypes(include='number').columns.tolist()
                    s_col = fc2.selectbox("📊 SORT BY", ["None"] + num_cols)
                    if s_col != "None":
                        df_cleaned = df_cleaned.sort_values(by=s_col, ascending=fc2.checkbox("ASCENDING", True))
                    
                    search = fc3.text_input("🔎 SEARCH")
                    if search:
                        df_cleaned = df_cleaned[df_cleaned.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

                st.dataframe(df_cleaned, use_container_width=True, height=500)
                render_reports(report)

            elif selection == "📉 VISUALIZATION":
                render_visualization(df_cleaned)

            elif selection == "📥 EXPORT RESULTS":
                st.markdown("## 📥 EXPORT RESULTS")
                ex1, ex2, ex3 = st.columns(3)
                with ex1:
                    st.markdown("""
                    <div class='glass-card' style='text-align:center;border-top:3px solid #00ffff;'>
                        <h3 style='color:#00ffff;'>CSV</h3>
                        <p style='color:#888;font-size:0.9rem;'>Comma-separated values</p>
                    </div>
                    """, unsafe_allow_html=True)
                    st.download_button("📥 DOWNLOAD CSV", 
                                     df_cleaned.to_csv(index=False).encode('utf-8'), 
                                     f"cleaned_{uploaded_file.name}", 
                                     "text/csv", 
                                     use_container_width=True)
                with ex2:
                    st.markdown("""
                    <div class='glass-card' style='text-align:center;border-top:3px solid #ff00ff;'>
                        <h3 style='color:#ff00ff;'>EXCEL</h3>
                        <p style='color:#888;font-size:0.9rem;'>Microsoft Excel format</p>
                    </div>
                    """, unsafe_allow_html=True)
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer: 
                        df_cleaned.to_excel(writer, index=False)
                    st.download_button("📥 DOWNLOAD EXCEL", 
                                     output.getvalue(), 
                                     f"cleaned_{uploaded_file.name}.xlsx", 
                                     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                                     use_container_width=True)
                with ex3:
                    st.markdown("""
                    <div class='glass-card' style='text-align:center;border-top:3px solid #ffff00;'>
                        <h3 style='color:#ffff00;'>REPORT</h3>
                        <p style='color:#888;font-size:0.9rem;'>Detailed JSON report</p>
                    </div>
                    """, unsafe_allow_html=True)
                    st.download_button("📥 DOWNLOAD REPORT", 
                                     json.dumps(report, indent=2).encode('utf-8'), 
                                     f"report_{uploaded_file.name}.json", 
                                     "application/json", 
                                     use_container_width=True)
                
                st.balloons()
                st.success("✅ YOUR DATA IS READY FOR DOWNLOAD!")

        except Exception as e:
            st.error("❌ ERROR PROCESSING FILE")
            with st.expander("DETAILS"):
                st.exception(e)
    else:
        create_welcome_page()

if __name__ == "__main__":
    main()