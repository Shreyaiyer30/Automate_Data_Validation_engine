import streamlit as st
from src.core.session_state import CURRENT_PAGE, UPLOAD_FILENAME, RAW_DF, PIPELINE_STAGE, DEBUG_MODE

def sync_page_from_query_params(pages):
    """Syncs the current page in session state from the URL query parameters."""
    query_params = st.query_params
    if "page" in query_params:
        requested_page = query_params["page"]
        # Verify if the page exists and is unlocked
        for _, label, gate in pages:
            if label == requested_page:
                unlocked = gate is None or st.session_state.get(gate) is not None
                if unlocked:
                    st.session_state[CURRENT_PAGE] = label
                break

def render_topnav(pages):
    """Renders the top navigation bar with HTML/CSS."""
    current = st.session_state.get(CURRENT_PAGE, "Home")

    tabs_html = []
    for icon, label, gate in pages:
        unlocked = gate is None or st.session_state.get(gate) is not None
        active   = current == label
        css      = "nav-tab"
        if active:   css += " active"
        if not unlocked: css += " locked"
        
        # Using <a> tags as requested for no-JS navigation
        if unlocked:
            tabs_html.append(
                f'<a class="{css}" href="?page={label}" target="_self">'
                f'{icon} {label}</a>'
            )
        else:
            tabs_html.append(
                f'<span class="{css}">{icon} {label}</span>'
            )
            
    tabs_str = ''.join(tabs_html)

    # File info
    fname = st.session_state.get(UPLOAD_FILENAME)
    if fname:
        status_html = f'<div class="topnav-status"><div class="status-dot"></div>{fname}</div>'
    else:
        status_html = '<div class="topnav-status" style="color:var(--border)">No file loaded</div>'

    st.markdown(f"""
    <div class="topnav">
        <div class="topnav-brand">
            <div class="topnav-logo">🧹</div>
            <span class="topnav-title">DataClean Pro</span>
        </div>
        <div class="topnav-tabs">{tabs_str}</div>
        {status_html}
    </div>
    """, unsafe_allow_html=True)

def render_sidebar(pages, pipeline_steps):
    """Renders the sidebar with navigation radio, progress bar, and debug toggle."""
    with st.sidebar:
        st.markdown("""
        <div style="padding:1.25rem 0 1rem 0; border-bottom:1px solid #1e2535; margin-bottom:1rem;">
            <div style="font-family:'Outfit',sans-serif; font-weight:800; font-size:1.1rem; letter-spacing:-0.02em;">
                🧹 DataClean Pro
            </div>
            <div style="font-size:0.72rem; color:#64748b; margin-top:0.2rem; font-weight:400;">
                Automated Data Quality Engine
            </div>
        </div>
        """, unsafe_allow_html=True)

        current = st.session_state.get(CURRENT_PAGE, "Home")
        current_label = next(
            (f"{icon} {label}" for icon, label, _ in pages if label == current),
            "🏠 Home"
        )

        label_to_page = {}
        for icon, label, gate in pages:
            unlocked = gate is None or st.session_state.get(gate) is not None
            nav_label = f"{icon} {label}" if unlocked else f"🔒 {label}"
            label_to_page[nav_label] = label

        selected = st.radio(
            "Navigate",
            list(label_to_page.keys()),
            index=list(label_to_page.keys()).index(current_label)
                  if current_label in label_to_page else 0,
            label_visibility="collapsed"
        )

        page_name = label_to_page.get(selected, current)

        if selected.startswith("🔒"):
            st.warning("Complete previous steps to unlock this page.")
            page_name = current

        if page_name != st.session_state.get(CURRENT_PAGE):
            st.session_state[CURRENT_PAGE] = page_name
            st.rerun()

        # Pipeline progress
        st.markdown("<div style='margin-top:1.5rem;'>", unsafe_allow_html=True)
        stage = st.session_state.get(PIPELINE_STAGE, 0)
        st.progress(stage / len(pipeline_steps))
        st.caption(f"Pipeline: {stage}/{len(pipeline_steps)} steps complete")

        # Debug toggle
        st.markdown("<hr>", unsafe_allow_html=True)
        debug = st.toggle("🐛 Debug Mode", value=st.session_state.get(DEBUG_MODE, False))
        st.session_state[DEBUG_MODE] = debug

        # Dataset info
        if st.session_state.get(RAW_DF) is not None:
            raw = st.session_state[RAW_DF]
            st.markdown("<hr>", unsafe_allow_html=True)
            st.markdown(f"""
            <div style="font-size:0.75rem; color:var(--muted);">
                <div style="font-weight:600; color:var(--text); margin-bottom:0.4rem;">
                    Dataset Info
                </div>
                <div>📄 {st.session_state.get(UPLOAD_FILENAME,'—')}</div>
                <div>Rows: {len(raw):,}</div>
                <div>Cols: {len(raw.columns)}</div>
            </div>
            """, unsafe_allow_html=True)
