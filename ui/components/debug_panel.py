import streamlit as st
import pandas as pd

def render_debug_panel():
    """Renders the developer debug panel if enabled in session state."""
    if not st.session_state.get("debug_mode"):
        return

    with st.expander("🐛 Debug Panel", expanded=False):
        dt1, dt2, dt3, dt4 = st.tabs([
            "Session State", "Header Mapping", "Config", "Exceptions"
        ])

        with dt1:
            skip_keys = {"raw_df", "clean_df", "analyzed_df"}
            safe_state = {}
            for k, v in st.session_state.items():
                if k in skip_keys:
                    continue
                try:
                    if isinstance(v, pd.DataFrame):
                        safe_state[k] = f"DataFrame {v.shape}"
                    elif isinstance(v, (str, int, float, bool, type(None))):
                        safe_state[k] = v
                    elif isinstance(v, list):
                        txt = str(v)
                        safe_state[k] = txt[:150] + "…" if len(txt) > 150 else txt
                    elif isinstance(v, dict):
                        safe_state[k] = {
                            dk: (str(dv)[:60] + "…" if len(str(dv)) > 60 else str(dv))
                            for dk, dv in list(v.items())[:10]
                        }
                    else:
                        safe_state[k] = f"<{type(v).__module__}.{type(v).__name__}>"
                except Exception:
                    safe_state[k] = "<unserializable>"
            st.json(safe_state)

        with dt2:
            mapping = st.session_state.get("header_mapping", {})
            if mapping:
                st.table(pd.DataFrame([
                    {"Raw": k, "Normalized": v} for k, v in mapping.items()
                ]))
            else:
                st.info("No header renaming in this run.")

        with dt3:
            config_val = st.session_state.get("config", {})
            if not isinstance(config_val, dict):
                st.info(f"Config type: {type(config_val).__name__} — {str(config_val)[:200]}")
            elif not config_val:
                st.info("Config is empty — using defaults. Run pipeline to populate.")
            else:
                try:
                    st.json(config_val)
                except Exception:
                    st.code(str(config_val), language="yaml")

        with dt4:
            excs = st.session_state.get("debug_exceptions", [])
            if excs:
                for ex in reversed(excs):
                    st.markdown(f"**{ex['timestamp']}** — `{ex['stage']}`")
                    st.code(ex["traceback"], language="python")
            else:
                st.info("No exceptions recorded.")
