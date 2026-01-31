import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from ui.components.upload import render_upload_section
from ui.components.overview import render_overview_section
from ui.components.cleaning import render_cleaning_lab
from ui.components.visualization import render_visualization_section
from ui.components.export import render_export_section

# --- Page Config ---
st.set_page_config(
    page_title="DataClean Pro | Production Data Engine",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom Theme (CSS) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        background-color: #0a0a0a;
        color: #ffffff;
    }
    
    .stApp { background-color: #0a0a0a; }
    
    .glass-card {
        background: rgba(30, 30, 30, 0.7);
        backdrop-filter: blur(10px);
        padding: 24px;
        border-radius: 16px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
        margin-bottom: 24px;
        transition: transform 0.2s ease;
    }
    
    .glass-card:hover { transform: translateY(-3px); }
    
    h1, h2, h3 { letter-spacing: -0.5px; }
    
    .hero-gradient {
        background: linear-gradient(90deg, #00ffff, #ff00ff);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        font-size: 3.5rem;
    }
</style>
""", unsafe_allow_html=True)

# --- State Initialization ---
from src.data_processor import DataProcessor
if 'processor' not in st.session_state: st.session_state.processor = DataProcessor()
if 'df_raw' not in st.session_state: st.session_state.df_raw = None
if 'df_cleaned' not in st.session_state: st.session_state.df_cleaned = None
if 'report' not in st.session_state: st.session_state.report = None
if 'page' not in st.session_state: st.session_state.page = "Upload"
if 'active_rules' not in st.session_state: st.session_state.active_rules = {}

# --- Sidebar Navigation ---
with st.sidebar:
    st.markdown("<h1 class='hero-gradient' style='font-size: 2rem;'>🛡️ CLEANPRO</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    nav_options = {
        "📁 Upload Data": "Upload",
        "📊 Data Overview (RAW)": "Overview",
        "🧪 Cleaning Lab": "Cleaning",
        "📉 Visualization": "Visualization",
        "📥 Export Results": "Export"
    }
    
    # Disable certain pages until data is uploaded
    disabled = st.session_state.df_raw is None
    
    for label, target in nav_options.items():
        if st.button(label, use_container_width=True, type="secondary" if st.session_state.page != target else "primary", disabled=(disabled and target != "Upload")):
            st.session_state.page = target
            st.rerun()

# --- Page Routing ---
page = st.session_state.page

if page == "Upload":
    st.markdown("<h1 class='hero-gradient'>Start Your Journey</h1>", unsafe_allow_html=True)
    if render_upload_section():
        st.session_state.page = "Overview"
        st.rerun()

elif page == "Overview":
    render_overview_section(st.session_state.df_raw)

elif page == "Cleaning":
    render_cleaning_lab(st.session_state.df_raw)

elif page == "Visualization":
    if st.session_state.df_cleaned is not None:
        render_visualization_section(st.session_state.df_raw, st.session_state.df_cleaned)
    else:
        st.warning("Please run the Cleaning Pipeline first.")

elif page == "Export":
    if st.session_state.report is not None:
        render_export_section(st.session_state.df_cleaned, st.session_state.report)
    else:
        st.warning("No quality report available. Process your data first.")
