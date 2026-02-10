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
    
    /* Pipeline Progress Styles */
    .pipeline-container {
        display: flex;
        justify-content: space-between;
        margin-bottom: 40px;
        padding: 10px;
        background: rgba(20, 20, 20, 0.5);
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.05);
    }
    
    /* Enterprise Table Styling */
    [data-testid="stDataFrame"] {
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        overflow: hidden;
    }
    
    [data-testid="stTable"] {
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 8px;
        background: transparent;
    }
    
    [data-testid="stTable"] thead th {
        background-color: rgba(60, 60, 60, 0.5) !important;
        color: #00ffff !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.5px;
    }
    
    [data-testid="stTable"] tr:nth-child(even) {
        background-color: rgba(255, 255, 255, 0.02);
    }
    
    .st-emotion-cache-16idsys p { font-size: 0.9rem; }
    .step {
        text-align: center;
        flex: 1;
        position: relative;
        font-size: 0.8rem;
        color: #666;
    }
    .step.active { color: #00ffff; font-weight: 600; }
    .step.complete { color: #00ff00; }
    .step.fail { color: #ff4b4b; }
    .step-circle {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        background: #333;
        margin: 0 auto 5px;
    }
    .active .step-circle { background: #00ffff; box-shadow: 0 0 10px #00ffff; }
    .complete .step-circle { background: #00ff00; }
    .fail .step-circle { background: #ff4b4b; }
</style>
""", unsafe_allow_html=True)

# --- State Initialization ---
from src.data_processor import DataProcessor
# --- ARCHITECTURAL CONTRACT: STATE INTEGRITY ---
# Re-instantiate processor on code change or version mismatch (Streamlit live-reload safety)
current_version = getattr(DataProcessor, 'VERSION', '1.0.0')
if ('processor' not in st.session_state or 
    getattr(st.session_state.processor, 'VERSION', '1.0.0') != current_version):
    st.session_state.processor = DataProcessor()

# Enforce clean DataFrame in session state (Recovery from stale multi-sheet loads)
if 'df_raw' in st.session_state and isinstance(st.session_state.df_raw, dict):
    # Auto-resolve stale dict to primary DataFrame
    if st.session_state.df_raw:
        # Sort keys to be deterministic (optional)
        first_key = sorted(st.session_state.df_raw.keys())[0]
        st.session_state.df_raw = st.session_state.df_raw[first_key]
    else:
        st.session_state.df_raw = pd.DataFrame()

if 'df_raw' not in st.session_state: st.session_state.df_raw = None
if 'df_cleaned' not in st.session_state: st.session_state.df_cleaned = None
if 'report' not in st.session_state: st.session_state.report = None
if 'metadata' not in st.session_state: st.session_state.metadata = None
if 'page' not in st.session_state: st.session_state.page = "Upload"
if 'active_rules' not in st.session_state: st.session_state.active_rules = {}

# --- Sidebar Navigation ---
with st.sidebar:
    st.markdown("<h1 class='hero-gradient' style='font-size: 2rem;'>🛡️ CLEANPRO</h1>", unsafe_allow_html=True)
    st.markdown("---")
    
    nav_options = {
        "📁 Upload Data": "Upload",
        "📊 Data Overview (RAW)": "Overview",
        "🔍 Analyze Data": "Analyze",
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

    st.markdown("---")
    if st.button("🔄 System Reset", use_container_width=True, help="Force clear session state (use if UI errors persist)"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# --- Page Routing & Pipeline Status ---
page = st.session_state.page

# Render Persistent Pipeline Header
pipeline_steps = ["Upload", "Overview", "Analyze", "Cleaning", "Visualization", "Export"]
cols = st.columns(len(pipeline_steps))
for i, step in enumerate(pipeline_steps):
    is_active = page == step
    is_complete = False # Logic here based on session state
    is_fail = False
    
    # Simple Mock logic for now
    if i < pipeline_steps.index(page): is_complete = True
    if st.session_state.report and st.session_state.report.get('quality_score', 0) < 50:
         # Just as an example of failure viz
         pass 

    status_class = "active" if is_active else ("complete" if is_complete else "")
    with cols[i]:
        st.markdown(f"""
        <div class="step {status_class}">
            <div class="step-circle"></div>
            {step}
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

if page == "Upload":
    st.markdown("<h1 class='hero-gradient'>Start Your Journey</h1>", unsafe_allow_html=True)
    if render_upload_section():
        st.session_state.page = "Overview"
        st.rerun()

elif page == "Overview":
    render_overview_section(st.session_state.df_raw)

elif page == "Analyze":
    # Show analysis results which are used to suggest rules
    st.markdown("## 🔍 DATA ANALYSIS")
    analysis = st.session_state.processor.analyze_data_quality(st.session_state.df_raw)
    
    st.markdown(f"### Quality Score: {analysis['quality_score']:.1f}/100")
    
    with st.expander("📝 View Suggested Cleaning Strategy", expanded=True):
        st.json(analysis['suggested_rules'])
        
    st.info("The next step 'Cleaning Lab' will allow you to fine-tune these rules.")

elif page == "Cleaning":
    render_cleaning_lab(st.session_state.df_raw)

elif page == "Visualization":
    if st.session_state.df_cleaned is not None:
        render_visualization_section(st.session_state.df_raw, st.session_state.df_cleaned, st.session_state.report)
    else:
        st.warning("Please run the Cleaning Pipeline first.")

elif page == "Export":
    if st.session_state.report is not None:
        render_export_section(st.session_state.df_cleaned, st.session_state.report)
    else:
        st.warning("No quality report available. Process your data first.")
