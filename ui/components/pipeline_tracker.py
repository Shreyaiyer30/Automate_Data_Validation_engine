import streamlit as st
import textwrap

def render_pipeline_tracker(active_idx: int, pipeline_steps):
    """Renders the visual pipeline stage tracker header."""
    stage = st.session_state.get("pipeline_stage", 0)

    cols = st.columns(len(pipeline_steps))
    for i, (icon, label) in enumerate(pipeline_steps):
        with cols[i]:
            if i < stage:
                # Completed
                dot_color = "#3d7eff"
                dot_bg    = "rgba(61,126,255,0.15)"
                txt_color = "#e2e8f0"
                border    = f"2px solid #3d7eff"
            elif i == active_idx:
                # Active
                dot_color = "#6c3fff"
                dot_bg    = "rgba(108,63,255,0.2)"
                txt_color = "#e2e8f0"
                border    = f"2px solid #6c3fff"
                icon      = f"{icon}"
            else:
                # Locked
                dot_color = "#1e2535"
                dot_bg    = "#0e1117"
                txt_color = "#64748b"
                border    = f"2px solid #1e2535"

            check = "✓ " if i < stage else ""
            
            html = textwrap.dedent(f"""
                <div style="text-align:center; padding:0.1rem 0;">
                    <div style="
                        width:36px; height:36px;
                        border-radius:50%;
                        background:{dot_bg};
                        border:{border};
                        display:inline-flex;
                        align-items:center;
                        justify-content:center;
                        font-size:15px;
                        margin-bottom:0.3rem;
                        box-shadow: {'0 0 10px rgba(108,63,255,0.4)' if i == active_idx else 'none'};
                    ">{icon}</div>
                    <div style="
                        font-size:0.7rem;
                        font-weight:600;
                        color:{txt_color};
                        letter-spacing:0.03em;
                    ">{check}{label}</div>
                </div>
            """).strip()
            
            st.markdown(html, unsafe_allow_html=True)
            
    progress_html = textwrap.dedent(f"""
        <div style="height:2px; background:linear-gradient(90deg,
            #3d7eff {int(stage/len(pipeline_steps)*100)}%,
            #1e2535 {int(stage/len(pipeline_steps)*100)}%);
            border-radius:2px; margin:0.3rem 0 1.2rem 0;"></div>
    """).strip()
    
    st.markdown(progress_html, unsafe_allow_html=True)
