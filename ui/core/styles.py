import streamlit as st

def load_css():
    """Injects custom CSS into the Streamlit app. Fails silently if an error occurs."""
    try:
        css = """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&display=swap');

        :root {
            --bg:         #0f172a;
            --surface:    #1e293b;
            --surface2:   #334155;
            --border:     #475569;
            --accent:     #3b82f6;
            --accent2:    #8b5cf6;
            --green:      #10b981;
            --amber:      #f59e0b;
            --red:        #ef4444;
            --text:       #f8fafc;
            --muted:      #94a3b8;
            --font-head:  'Space Grotesk', sans-serif;
            --font-body:  'Space Grotesk', sans-serif;
            --font-mono:  'Courier New', monospace;
        }

        /* ── Reset ─────────────────────────────────────────────── */
        .stApp, .main, .block-container {
            font-family: var(--font-body) !important;
            background-color: var(--bg) !important;
            color: var(--text) !important;
        }

        .main { background-color: var(--bg) !important; }
        .main .block-container {
            padding: 0 2rem 3rem 2rem !important;
            max-width: 1400px !important;
        }

        /* ── Top nav header ────────────────────────────────────── */
        .topnav {
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 1.1rem 0 1.4rem 0;
            border-bottom: 1px solid var(--border);
            margin-bottom: 0;
        }
        .topnav-brand {
            display: flex;
            align-items: center;
            gap: 0.6rem;
        }
        .topnav-logo {
            width: 34px; height: 34px;
            background: linear-gradient(135deg, var(--accent), var(--accent2));
            border-radius: 8px;
            display: flex; align-items: center; justify-content: center;
            font-size: 16px;
        }
        .topnav-title {
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 1.25rem !important;
            font-weight: 800 !important;
            color: var(--text) !important;
            letter-spacing: -0.02em;
            margin: 0 !important;
        }
        .topnav-tabs {
            display: flex;
            gap: 0.25rem;
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 0.3rem;
        }
        .nav-tab {
            padding: 0.45rem 1rem;
            border-radius: 7px;
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 0.82rem !important;
            font-weight: 500 !important;
            color: var(--muted) !important;
            cursor: pointer;
            transition: all 0.18s ease;
            border: none;
            background: transparent;
            white-space: nowrap;
            text-decoration: none !important;
        }
        .nav-tab:hover { color: var(--text) !important; }
        .nav-tab.active {
            background: var(--accent) !important;
            color: white !important;
        }
        .nav-tab.locked {
            opacity: 0.35;
            cursor: not-allowed !important;
            pointer-events: none;
        }
        .topnav-status {
            font-size: 0.78rem;
            color: var(--muted);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        .status-dot {
            width: 7px; height: 7px;
            border-radius: 50%;
            background: var(--green);
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50%       { opacity: 0.4; }
        }

        /* ── Page title ────────────────────────────────────────── */
        .page-title {
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 2rem !important;
            font-weight: 800 !important;
            letter-spacing: -0.03em !important;
            color: var(--text) !important;
            margin: 2rem 0 0.25rem 0 !important;
            line-height: 1.1 !important;
            background: none !important;
            -webkit-text-fill-color: var(--text) !important;
        }
        .page-subtitle {
            font-size: 0.9rem;
            color: var(--muted);
            margin-bottom: 1.75rem;
            font-weight: 400;
        }

        /* ── Cards ─────────────────────────────────────────────── */
        .card {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.25rem 1.5rem;
        }
        .card-sm {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 1rem 1.25rem;
        }

        /* ── KPI metric cards ──────────────────────────────────── */
        [data-testid="metric-container"] {
            background: var(--surface) !important;
            border: 1px solid var(--border) !important;
            border-radius: 12px !important;
            padding: 1.1rem 1.25rem !important;
        }
        [data-testid="metric-container"] label {
            font-size: 0.76rem !important;
            font-weight: 500 !important;
            color: var(--muted) !important;
            text-transform: uppercase !important;
            letter-spacing: 0.06em !important;
        }
        [data-testid="metric-container"] [data-testid="stMetricValue"] {
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 1.8rem !important;
            font-weight: 800 !important;
            color: var(--text) !important;
            line-height: 1.1 !important;
        }

        /* ── Score ring ────────────────────────────────────────── */
        .score-ring-wrap {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 0.5rem;
            padding: 1.5rem;
        }
        .score-ring {
            position: relative;
            width: 120px; height: 120px;
        }
        .score-ring svg { transform: rotate(-90deg); }
        .score-ring-label {
            position: absolute;
            top: 50%; left: 50%;
            transform: translate(-50%, -50%);
            text-align: center;
        }
        .score-number {
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 1.7rem;
            font-weight: 800;
            line-height: 1;
            color: var(--text);
        }
        .score-sub {
            font-size: 0.65rem;
            color: var(--muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        /* ── Pipeline state badge ──────────────────────────────── */
        .state-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            padding: 0.3rem 0.85rem;
            border-radius: 99px;
            font-size: 0.78rem;
            font-weight: 600;
            letter-spacing: 0.04em;
        }
        .state-pass  { background: rgba(34,197,94,0.12);  color: #22c55e; border: 1px solid rgba(34,197,94,0.25); }
        .state-warn  { background: rgba(245,158,11,0.12); color: #f59e0b; border: 1px solid rgba(245,158,11,0.25); }
        .state-fail  { background: rgba(239,68,68,0.12);  color: #ef4444; border: 1px solid rgba(239,68,68,0.25); }
        .state-ready { background: rgba(79,142,255,0.12); color: #4f8eff; border: 1px solid rgba(79,142,255,0.25); }

        /* ── Upload zone ───────────────────────────────────────── */
        .upload-hero {
            border: 2px dashed var(--border);
            border-radius: 16px;
            padding: 3rem 2rem;
            text-align: center;
            background: var(--surface);
            transition: border-color 0.2s ease;
        }
        .upload-hero:hover { border-color: var(--accent); }
        [data-testid="stFileUploader"] {
            background: transparent !important;
        }
        [data-testid="stFileUploadDropzone"] {
            background: var(--surface2) !important;
            border: 1px solid var(--border) !important;
            border-radius: 10px !important;
            color: var(--muted) !important;
        }

        /* ── Stage pipeline tracker ────────────────────────────── */
        .pipeline-track {
            display: flex;
            align-items: center;
            gap: 0;
            margin: 1.5rem 0;
            overflow-x: auto;
        }
        .pipe-step {
            display: flex;
            flex-direction: column;
            align-items: center;
            flex: 1;
            min-width: 90px;
            position: relative;
        }
        .pipe-step:not(:last-child)::after {
            content: '';
            position: absolute;
            top: 17px;
            left: calc(50% + 17px);
            right: calc(-50% + 17px);
            height: 2px;
            background: var(--border);
        }
        .pipe-step.done:not(:last-child)::after { background: var(--accent); }
        .pipe-dot {
            width: 34px; height: 34px;
            border-radius: 50%;
            border: 2px solid var(--border);
            background: var(--surface);
            display: flex; align-items: center; justify-content: center;
            font-size: 14px;
            position: relative; z-index: 1;
        }
        .pipe-step.done .pipe-dot {
            border-color: var(--accent);
            background: rgba(79,142,255,0.15);
        }
        .pipe-step.active .pipe-dot {
            border-color: var(--accent2);
            background: rgba(124,92,252,0.15);
            box-shadow: 0 0 12px rgba(124,92,252,0.4);
        }
        .pipe-label {
            font-size: 0.68rem;
            color: var(--muted);
            margin-top: 0.4rem;
            text-align: center;
            font-weight: 500;
        }
        .pipe-step.done .pipe-label, .pipe-step.active .pipe-label {
            color: var(--text);
        }

        /* ── Sidebar ───────────────────────────────────────────── */
        /* ── Sidebar ───────────────────────────────────────────── */
        [data-testid="stSidebar"] {
            background: var(--surface) !important;
            border-right: 1px solid var(--border) !important;
        }
        [data-testid="stSidebar"] * { color: var(--text) !important; }
        [data-testid="stSidebar"] .stRadio label {
            color: var(--muted) !important;
            font-size: 0.85rem !important;
        }

        /* ── Hide Streamlit default multipage navigation ───────── */
        [data-testid="stSidebarNav"] {
            display: none !important;
        }
        [data-testid="stSidebarNavSeparator"] {
            display: none !important;
        }

        /* ── Tabs ──────────────────────────────────────────────── */
        [data-baseweb="tab-list"] {
            background: var(--surface) !important;
            border-radius: 10px !important;
            padding: 0.3rem !important;
            gap: 0.2rem !important;
            border: 1px solid var(--border) !important;
        }
        [data-baseweb="tab"] {
            background: transparent !important;
            border-radius: 7px !important;
            color: var(--muted) !important;
            font-size: 0.84rem !important;
            font-weight: 500 !important;
            border: none !important;
        }
        [aria-selected="true"][data-baseweb="tab"] {
            background: var(--accent) !important;
            color: white !important;
        }

        /* ── Tables / dataframes ───────────────────────────────── */
        [data-testid="stDataFrame"] {
            border: 1px solid var(--border) !important;
            border-radius: 10px !important;
            overflow: hidden !important;
        }
        .stTable table {
            background: var(--surface) !important;
            border-radius: 10px !important;
        }
        .stTable th {
            background: var(--surface2) !important;
            color: var(--muted) !important;
            font-size: 0.75rem !important;
            text-transform: uppercase !important;
            letter-spacing: 0.05em !important;
        }

        /* ── Alerts ────────────────────────────────────────────── */
        [data-testid="stAlert"] {
            border-radius: 10px !important;
            border: none !important;
        }

        /* ── Buttons ───────────────────────────────────────────── */
        [data-testid="stDownloadButton"] button,
        .stButton > button {
            background: var(--accent) !important;
            color: white !important;
            border: none !important;
            border-radius: 8px !important;
            font-weight: 600 !important;
            font-size: 0.85rem !important;
            padding: 0.55rem 1.4rem !important;
            transition: all 0.18s ease !important;
        }
        [data-testid="stDownloadButton"] button:hover,
        .stButton > button:hover {
            background: var(--accent2) !important;
            transform: translateY(-1px) !important;
            box-shadow: 0 4px 14px rgba(79,142,255,0.35) !important;
        }

        /* ── Inputs / selects ──────────────────────────────────── */
        [data-testid="stSelectbox"] > div,
        [data-testid="stTextInput"] input {
            background: var(--surface2) !important;
            border: 1px solid var(--border) !important;
            border-radius: 8px !important;
            color: var(--text) !important;
        }
        [data-testid="stSlider"] { color: var(--accent) !important; }

        /* ── Expanders ─────────────────────────────────────────── */
        [data-testid="stExpander"] {
            background: var(--surface) !important;
            border: 1px solid var(--border) !important;
            border-radius: 10px !important;
        }

        /* ── Dividers ──────────────────────────────────────────── */
        hr { border-color: var(--border) !important; }

        /* ── Scrollbar ─────────────────────────────────────────── */
        ::-webkit-scrollbar { width: 5px; height: 5px; }
        ::-webkit-scrollbar-track { background: var(--bg); }
        ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 99px; }

        /* ── Home hero ─────────────────────────────────────────── */
        .hero-wrap {
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 3.5rem 3rem;
            margin: 1.5rem 0;
            position: relative;
            overflow: hidden;
        }
        .hero-wrap::before {
            content: '';
            position: absolute;
            top: -60px; right: -60px;
            width: 280px; height: 280px;
            background: radial-gradient(circle, rgba(79,142,255,0.12) 0%, transparent 70%);
            border-radius: 50%;
        }
        .hero-wrap::after {
            content: '';
            position: absolute;
            bottom: -40px; left: 20%;
            width: 200px; height: 200px;
            background: radial-gradient(circle, rgba(124,92,252,0.08) 0%, transparent 70%);
            border-radius: 50%;
        }
        .hero-eyebrow {
            font-size: 0.75rem;
            font-weight: 600;
            color: var(--accent);
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-bottom: 0.75rem;
        }
        .hero-h1 {
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 3rem !important;
            font-weight: 900 !important;
            letter-spacing: -0.04em !important;
            line-height: 1.05 !important;
            color: var(--text) !important;
            margin-bottom: 1rem !important;
            background: none !important;
        }
        .hero-desc {
            font-size: 1rem;
            color: var(--muted);
            max-width: 520px;
            line-height: 1.65;
            margin-bottom: 2rem;
            font-weight: 300;
        }
        .feature-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1rem;
            margin-top: 2rem;
        }
        .feature-card {
            background: var(--surface2);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.25rem;
        }
        .feature-icon {
            font-size: 1.5rem;
            margin-bottom: 0.6rem;
        }
        .feature-title {
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 0.9rem !important;
            font-weight: 700 !important;
            color: var(--text) !important;
            margin-bottom: 0.3rem !important;
            background: none !important;
        }
        .feature-desc {
            font-size: 0.78rem;
            color: var(--muted);
            line-height: 1.5;
            font-weight: 300;
        }

        /* ── Section label ─────────────────────────────────────── */
        .section-label {
            font-size: 0.72rem;
            font-weight: 600;
            color: var(--accent);
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-bottom: 0.6rem;
            font-family: 'Space Grotesk', sans-serif !important;
            background: none !important;
        }
        .section-title {
            font-family: 'Space Grotesk', sans-serif !important;
            font-size: 1.35rem !important;
            font-weight: 700 !important;
            letter-spacing: -0.02em !important;
            color: var(--text) !important;
            margin-bottom: 0.25rem !important;
            background: none !important;
        }

        /* ── CRITICAL: Kill Streamlit's default markdown code highlight ── */
        .stMarkdown div, .stMarkdown p, .stMarkdown span,
        .element-container .stMarkdown {
            background: none !important;
        }
        /* Kill any blue selection highlight on markdown */
        .stMarkdown [class^="st-"],
        .stMarkdown code, .stMarkdown pre {
            background: transparent !important;
            color: inherit !important;
            border: none !important;
            padding: 0 !important;
        }

        /* ── Issues table severity colors ──────────────────────── */
        .sev-info  { color: #4f8eff; font-weight: 600; }

        /* ── Studio Theme (Global) ────────────────────────────── */
        .studio-shell {
            background: radial-gradient(circle at 5% 5%, rgba(255, 190, 118, 0.14), transparent 40%),
                        radial-gradient(circle at 95% 10%, rgba(70, 130, 180, 0.16), transparent 42%),
                        linear-gradient(170deg, rgba(15, 23, 42, 0.95), rgba(17, 24, 39, 0.94));
            border: 1px solid rgba(148, 163, 184, 0.25);
            border-radius: 18px;
            padding: 1.2rem 1.2rem 1rem 1.2rem;
            margin: 1rem 0 1.2rem 0;
            box-shadow: 0 18px 40px rgba(2, 6, 23, 0.28);
        }

        .studio-head {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: flex-start;
            margin-bottom: 0.6rem;
        }

        .studio-title {
            font-size: 2.1rem;
            font-weight: 700;
            letter-spacing: -0.03em;
            color: #f8fafc;
            line-height: 1.15;
            margin: 0;
        }

        .studio-sub {
            margin-top: 0.25rem;
            margin-bottom: 0;
            color: #cbd5e1;
            font-size: 0.95rem;
        }

        .studio-chip {
            background: rgba(249, 115, 22, 0.12);
            border: 1px solid rgba(251, 146, 60, 0.45);
            color: #fed7aa;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 600;
            padding: 0.3rem 0.65rem;
            white-space: nowrap;
            margin-top: 0.2rem;
        }

        .studio-card {
            background: rgba(248, 250, 252, 0.03);
            border: 1px solid rgba(148, 163, 184, 0.2);
            border-radius: 14px;
            padding: 1rem;
            margin-bottom: 1rem;
        }

        .studio-card-title {
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            color: #f59e0b;
            text-transform: uppercase;
            margin-bottom: 0.6rem;
        }
        
        /* Glassmorphic generic card */
        .glass-card {
            background: linear-gradient(130deg, rgba(17, 24, 39, 0.94), rgba(30, 41, 59, 0.92));
            border: 1px solid rgba(148, 163, 184, 0.26);
            border-radius: 18px;
            padding: 1.4rem;
            margin: 1rem 0;
            box-shadow: 0 16px 34px rgba(2, 6, 23, 0.23);
        }

        </style>
        """
        st.markdown(css, unsafe_allow_html=True)
    except Exception:
        pass
