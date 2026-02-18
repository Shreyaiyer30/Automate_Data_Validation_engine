import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime

from src.core.constants import PIPELINE_STEPS
from src.core.session_state import RAW_DF, CLEAN_DF, RUN_RESULT, VALIDATION_REPORT, CURRENT_PAGE
from ui.components.pipeline_tracker import render_pipeline_tracker
from ui.components.score_widgets import score_ring
from src.engine.atomic_engine import AtomicEngine


CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    --bg:       #f9fafb;
    --surface:  #ffffff;
    --border:   #e5e7eb;
    --border2:  #d1d5db;
    --text:     #111827;
    --muted:    #6b7280;
    --faint:    #9ca3af;
    --accent:   #2563eb;
    --accent-bg:#eff6ff;
    --green:    #16a34a;
    --green-bg: #f0fdf4;
    --warn:     #d97706;
    --danger:   #dc2626;
    --radius:   8px;
    --font:     'Inter', sans-serif;
    --mono:     'JetBrains Mono', monospace;
}

html, body, [class*="css"] { font-family: var(--font) !important; }
.stApp { background: var(--bg) !important; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 2rem 2.5rem 5rem !important; max-width: 1280px !important; }

/* Header */
.page-title { font-size: 1.35rem; font-weight: 600; color: var(--text); letter-spacing: -0.3px; margin: 0 0 2px; }
.page-sub   { font-size: 0.8rem; color: var(--muted); margin: 0 0 1.5rem; }

/* Stat pills */
.stat-row { display: flex; gap: 0.75rem; margin-bottom: 1.5rem; flex-wrap: wrap; }
.stat-pill {
    display: inline-flex; align-items: center; gap: 5px;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 5px 11px;
    font-size: 0.78rem; color: var(--muted); white-space: nowrap;
}
.stat-pill b { color: var(--text); font-weight: 600; font-family: var(--mono); font-size: 0.8rem; }
.stat-pill.warn b { color: var(--warn); }
.stat-pill.danger b { color: var(--danger); }

/* Section label */
.slabel {
    font-size: 0.67rem; font-weight: 600; letter-spacing: .1em;
    text-transform: uppercase; color: var(--faint); margin-bottom: 8px;
}

/* Expanders */
details {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: var(--radius) !important;
    margin-bottom: 4px !important;
}
details[open] { border-color: var(--border2) !important; }
details summary {
    font-size: 0.83rem !important; font-weight: 500 !important;
    color: var(--text) !important; padding: 10px 14px !important;
}
details summary:hover { background: var(--bg) !important; }

/* Widget labels */
.stToggle label p { font-size: 0.8rem !important; color: var(--muted) !important; }
label[data-testid="stWidgetLabel"] p { font-size: 0.78rem !important; color: var(--muted) !important; }

/* Inputs */
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: var(--bg) !important;
    border-color: var(--border) !important;
    border-radius: 6px !important;
    font-size: 0.82rem !important;
}

/* Buttons */
.stButton > button[kind="primary"] {
    background: var(--accent) !important; border: none !important;
    color: #fff !important; font-family: var(--font) !important;
    font-weight: 600 !important; font-size: 0.85rem !important;
    border-radius: var(--radius) !important; padding: 0.55rem 1.2rem !important;
    box-shadow: 0 1px 3px rgba(37,99,235,.2) !important;
    transition: opacity .15s !important;
}
.stButton > button[kind="primary"]:hover { opacity: .88 !important; }
.stButton > button[kind="primary"]:disabled {
    background: var(--border2) !important; color: var(--faint) !important; box-shadow: none !important;
}
.stButton > button[kind="secondary"] {
    background: var(--surface) !important; border: 1px solid var(--border2) !important;
    color: var(--text) !important; font-family: var(--font) !important;
    font-weight: 500 !important; font-size: 0.82rem !important; border-radius: var(--radius) !important;
}
.stDownloadButton > button {
    background: var(--green-bg) !important; border: 1px solid #bbf7d0 !important;
    color: var(--green) !important; font-family: var(--font) !important;
    font-weight: 600 !important; font-size: 0.82rem !important; border-radius: var(--radius) !important;
}

/* Callout */
.callout {
    background: var(--accent-bg); border-left: 3px solid var(--accent);
    border-radius: 0 6px 6px 0; padding: 8px 12px;
    font-size: 0.78rem; color: #1e40af; margin-bottom: 1rem;
}

/* Result card */
.result-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 1.2rem 1.4rem; margin-bottom: 0.75rem;
}
.result-card-title {
    font-size: 0.67rem; font-weight: 600; color: var(--faint);
    text-transform: uppercase; letter-spacing: .08em; margin-bottom: 1rem;
}
.result-metrics { display: flex; gap: 2rem; }
.rm-item { flex: 1; }
.rm-val {
    font-size: 1.4rem; font-weight: 600; color: var(--text);
    font-family: var(--mono); line-height: 1.2;
}
.rm-val.blue  { color: var(--accent); }
.rm-lbl  { font-size: 0.72rem; color: var(--muted); margin-top: 2px; }
.rm-delta { font-size: 0.72rem; font-family: var(--mono); margin-top: 2px; }
.rm-delta.neg { color: var(--green); }
.rm-delta.pos { color: var(--warn); }

/* Log strip */
.log-strip {
    font-family: var(--mono); font-size: 0.75rem;
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 6px; padding: 8px 12px; color: var(--muted);
    display: flex; gap: 2rem; flex-wrap: wrap; margin-bottom: 0.75rem;
}
.log-strip span { color: var(--text); font-weight: 500; }

/* Misc */
hr { border-color: var(--border) !important; margin: 1.2rem 0 !important; }
.stDataFrame { border-radius: 6px !important; overflow: hidden; }
.stAlert { border-radius: 6px !important; font-size: 0.82rem !important; }
[data-testid="stMetricValue"] { font-family: var(--mono) !important; }
div[data-testid="stTabs"] button {
    font-family: var(--font) !important; font-size: 0.8rem !important; font-weight: 500 !important;
}
</style>
"""


def _compute_quality_score(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return 0.0
    total_cells = df.size or 1
    missing_pct = df.isnull().sum().sum() / total_cells
    dup_pct = df.duplicated().sum() / max(len(df), 1)
    num_cols = df.select_dtypes(include=[np.number]).columns
    type_score = (
        sum(1 for c in num_cols if df[c].std(skipna=True) > 0) / max(len(num_cols), 1)
        if len(num_cols) else 1.0
    )
    score = (1 - missing_pct) * 40 + (1 - dup_pct) * 30 + type_score * 30
    return round(min(max(score, 0), 100), 1)


def render():
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    df: pd.DataFrame = st.session_state.get(RAW_DF)
    if df is None:
        st.info("Upload a dataset to get started.")
        return

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown('<p class="page-title">Cleaning Lab</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-sub">Configure modules on the left, then run the pipeline.</p>', unsafe_allow_html=True)

    render_pipeline_tracker(active_idx=3, pipeline_steps=PIPELINE_STEPS)

    # ── Stat pills ────────────────────────────────────────────────────────────
    missing_total = int(df.isnull().sum().sum())
    dup_total     = int(df.duplicated().sum())
    miss_cls      = "warn"   if missing_total > 0 else ""
    dup_cls       = "danger" if dup_total > 0 else ""

    st.markdown(
        f"""
        <div class="stat-row">
            <div class="stat-pill"><b>{len(df):,}</b>&nbsp;rows</div>
            <div class="stat-pill"><b>{df.shape[1]}</b>&nbsp;columns</div>
            <div class="stat-pill {miss_cls}"><b>{missing_total:,}</b>&nbsp;missing cells</div>
            <div class="stat-pill {dup_cls}"><b>{dup_total:,}</b>&nbsp;duplicate rows</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.expander("Preview dataset", expanded=False):
        st.dataframe(df.head(10), use_container_width=True)

    st.divider()

    col_left, col_right = st.columns([5, 6], gap="large")

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    text_cols    = df.select_dtypes(include=["object", "string"]).columns.tolist()
    all_cols     = df.columns.tolist()

    # ══════════════════════════════════════════════════════════════════════════
    # LEFT: MODULES
    # ══════════════════════════════════════════════════════════════════════════
    with col_left:
        st.markdown('<div class="slabel">Cleaning Modules</div>', unsafe_allow_html=True)

        # 1. Missing Values
        with st.expander("Missing Values"):
            do_miss = st.toggle("Enable", key="do_miss")
            if do_miss:
                miss_scope = st.selectbox("Scope", ["All columns", "Selected columns"], key="miss_scope")
                m_cols = all_cols if "All" in miss_scope else st.multiselect("Columns", all_cols, key="m_cols")
                miss_strat = st.selectbox("Strategy", [
                    "Drop rows with any missing", "Drop rows where ALL are missing",
                    "Drop column if missing % > threshold",
                    "Fill numeric -> mean", "Fill numeric -> median", "Fill numeric -> mode",
                    "Fill text -> mode", "Fill with custom value",
                    "Forward fill (ffill)", "Backward fill (bfill)",
                ], key="miss_strat")
                m_thresh = st.slider("Threshold %", 0, 100, 50, key="m_thresh") if "threshold" in miss_strat else 50
                m_custom = st.text_input("Custom fill value", key="m_custom") if "custom" in miss_strat else ""
            else:
                m_cols, miss_strat, m_thresh, m_custom = [], "", 50, ""

        # 2. Duplicates
        with st.expander("Duplicates"):
            do_dups = st.toggle("Enable", key="do_dups")
            if do_dups:
                dup_strat = st.selectbox("Strategy", [
                    "Exact duplicates (all columns)", "Duplicates by selected columns",
                ], key="dup_strat")
                dup_cols = st.multiselect("Key columns", all_cols, key="dup_cols") if "selected" in dup_strat else None
                dup_keep = st.selectbox("Keep", ["first", "last", "none (drop all)"], key="dup_keep")
            else:
                dup_cols, dup_keep = None, "first"

        # 3. Data Types
        with st.expander("Data Type Cleaning"):
            do_dtype = st.toggle("Enable", key="do_dtype")
            if do_dtype:
                dtype_num_cols = st.multiselect("Force numeric", all_cols, key="dt_num_cols")
                dtype_num_err  = st.radio("On failure", ["Set NaN", "Keep original"], horizontal=True, key="dt_num_err")
                int_auto       = [c for c in numeric_cols if not df[c].dropna().empty and (df[c].dropna() % 1 == 0).all()]
                dtype_int_cols = st.multiselect("Force integer (Int64)", numeric_cols, default=int_auto, key="dt_int_cols")
                curr_kw        = ["amount", "price", "salary", "cost", "revenue", "income", "payment", "fee"]
                curr_auto      = [c for c in all_cols if any(kw in str(c).lower() for kw in curr_kw)]
                curr_cols      = st.multiselect("Currency columns", all_cols, default=curr_auto, key="curr_cols")
            else:
                dtype_num_cols, dtype_num_err, dtype_int_cols, curr_cols = [], "Set NaN", [], []

        # 4. Outliers
        with st.expander("Outlier Cleaning"):
            do_out = st.toggle("Enable", key="do_out")
            if do_out:
                o_scope    = st.selectbox("Scope", ["All numeric columns", "Selected columns"], key="o_scope")
                o_cols     = numeric_cols if "All" in o_scope else st.multiselect("Columns", numeric_cols, key="o_cols")
                out_method = st.selectbox("Method", ["IQR", "Z-Score"], key="out_method")
                out_action = st.selectbox("Action", ["Clip to bounds", "Remove rows", "Replace with NaN"], key="out_action")
                out_iqr    = st.slider("IQR multiplier", 1.0, 4.0, 1.5, 0.1, key="out_iqr") if out_method == "IQR" else 1.5
                out_z      = st.slider("Z-Score threshold", 1.5, 5.0, 3.0, 0.1, key="out_z") if out_method == "Z-Score" else 3.0
            else:
                o_cols, out_method, out_action, out_iqr, out_z = [], "IQR", "Clip to bounds", 1.5, 3.0

        # 5. Skewness
        with st.expander("Skewness Treatment"):
            do_skew = st.toggle("Enable", key="do_skew")
            if do_skew:
                sk_scope  = st.selectbox("Scope", ["All numeric columns", "Selected columns"], key="sk_scope")
                sk_cols   = numeric_cols if "All" in sk_scope else st.multiselect("Columns", numeric_cols, key="sk_cols")
                sk_thresh = st.slider("Treat when |skew| >", 0.3, 2.0, 0.5, 0.1, key="sk_thresh")
                sk_action = st.selectbox("Transform", [
                    "Log transform (log1p)", "Square root transform",
                    "Box-Cox transform", "Yeo-Johnson transform", "Report only (no transform)",
                ], key="sk_action")
            else:
                sk_cols, sk_thresh, sk_action = [], 0.5, "Report only (no transform)"

        # 6. Date & Time
        with st.expander("Date & Time Cleaning"):
            do_date = st.toggle("Enable", key="do_date")
            if do_date:
                d_scope   = st.selectbox("Scope", ["Auto-detect", "Select columns"], key="d_scope")
                date_kw   = ["dob", "date", "joining", "created", "updated", "time", "birth", "start", "end", "dt"]
                date_auto = [c for c in all_cols if any(kw in str(c).lower() for kw in date_kw)]
                d_cols    = date_auto if "Auto" in d_scope else st.multiselect("Date columns", all_cols, key="d_cols")
                d_mode    = st.selectbox("Parsing", ["Day-first (DD/MM)", "Month-first (MM/DD)"], key="d_mode")
                d_fmt     = st.selectbox("Output format", [
                    "DD-MM-YYYY", "YYYY-MM-DD", "DD/MM/YYYY", "MM/DD/YYYY", "DD Month YYYY", "Keep as datetime",
                ], key="d_fmt")
                do_age       = st.toggle("Calculate age from DOB column", key="do_age")
                age_dob_col  = st.selectbox("DOB column", all_cols, key="age_dob_col") if do_age else ""
                age_col_name = st.text_input("Output age column name", "Age", key="age_col_name") if do_age else "Age"
                do_age_str   = st.toggle("Clean age strings", key="do_age_str")
                age_str_cols = st.multiselect("Age string columns", all_cols, key="age_str_cols") if do_age_str else []
            else:
                d_cols, d_mode, d_fmt, do_age, age_dob_col, age_col_name, do_age_str, age_str_cols = [], "Day-first (DD/MM)", "DD-MM-YYYY", False, "", "Age", False, []

        # 7. Text & Categorical
        with st.expander("Text & Categorical Cleaning"):
            do_text = st.toggle("Enable", key="do_text")
            if do_text:
                t_scope   = st.selectbox("Scope", ["All text columns", "Selected columns"], key="t_scope")
                tc_cols   = text_cols if "All" in t_scope else st.multiselect("Columns", text_cols, key="tc_cols")
                case_rule = st.selectbox("Case normalization", [
                    "none", "Title Case", "UPPERCASE", "lowercase", "Sentence case",
                ], key="case_rule")
                c1, c2 = st.columns(2)
                do_trim     = c1.checkbox("Trim spaces",       value=True, key="do_trim")
                do_collapse = c2.checkbox("Collapse spaces",   value=True, key="do_collapse")
                do_spec     = c1.checkbox("Remove special chars",          key="do_spec")
                do_std      = c2.checkbox("Standardize nulls", value=True, key="do_std")
                do_cat   = st.toggle("Categorical optimization", key="do_cat")
                cat_cols = st.multiselect("Columns to optimize", all_cols, key="cat_cols") if do_cat else []
            else:
                tc_cols, case_rule, do_trim, do_collapse, do_spec, do_std, do_cat, cat_cols = [], "none", True, True, False, True, False, []

        # 8. Phone & Email
        with st.expander("Phone & Email Validation"):
            do_pe = st.toggle("Enable", key="do_pe")
            if do_pe:
                ph_cols   = st.multiselect("Phone columns", text_cols, key="ph_cols")
                em_cols   = st.multiselect("Email columns", text_cols, key="em_cols")
                pe_action = st.selectbox("Action on invalid", [
                    "Flag only (add _valid column)", "Set invalid -> blank / NaN",
                ], key="pe_action")
            else:
                ph_cols, em_cols, pe_action = [], [], "Flag only"

        # 9. Range-Based
        with st.expander("Range-Based Cleaning"):
            do_range = st.toggle("Enable", key="do_range")
            if do_range and numeric_cols:
                r_col    = st.selectbox("Column", numeric_cols, key="r_col")
                r_min    = float(df[r_col].min()) if not df[r_col].dropna().empty else 0.0
                r_max    = float(df[r_col].max()) if not df[r_col].dropna().empty else 100.0
                r_vals   = st.slider("Valid range", r_min, r_max, (r_min, r_max), key="r_vals")
                r_action = st.selectbox("Action", ["Clip to range", "Remove rows", "Set to NaN"], key="r_action")
            else:
                r_col, r_vals, r_action = None, (0.0, 100.0), "Clip to range"

        # 10. Column Cleanup
        with st.expander("Column Name & Schema Cleanup"):
            do_col = st.toggle("Enable", key="do_col")
            if do_col:
                c1, c2, c3 = st.columns(3)
                do_snake      = c1.checkbox("Snake case",    value=True, key="do_snake")
                do_strip      = c2.checkbox("Strip spaces",  value=True, key="do_strip")
                do_drop_empty = c3.checkbox("Drop empty cols", value=True, key="do_drop_empty")
            else:
                do_snake, do_strip, do_drop_empty = False, False, False

        # Execution policy
        st.divider()
        st.markdown('<div class="slabel">Execution Policy</div>', unsafe_allow_html=True)
        val_report = st.session_state.get(VALIDATION_REPORT)
        if val_report and val_report["failed"] > 0:
            st.warning(f"{val_report['failed']:,} rows failed validation.")
            do_val_skip = st.toggle("Strict mode: auto-drop invalid rows", value=True,  key="do_val_skip")
        else:
            do_val_skip = st.toggle("Strict mode: auto-drop invalid rows", value=False, key="do_val_skip")

    # ══════════════════════════════════════════════════════════════════════════
    # RIGHT: OVERVIEW + RUN + RESULTS
    # ══════════════════════════════════════════════════════════════════════════
    with col_right:
        st.markdown('<div class="slabel">Column Overview</div>', unsafe_allow_html=True)
        dtype_df = pd.DataFrame({
            "Column":   df.columns,
            "Type":     [str(dt) for dt in df.dtypes],
            "Non-null": df.notna().sum().values,
            "Null %":   (df.isnull().mean() * 100).round(1).astype(str) + "%",
            "Unique":   df.nunique().values,
        })
        st.dataframe(dtype_df, use_container_width=True, hide_index=True, height=220)

        st.divider()

        # ── Run ───────────────────────────────────────────────────────────────
        enabled_modules = sum([
            do_miss, do_dups, do_dtype, do_out, do_skew,
            do_date, do_text, do_pe, do_range, do_col,
        ])

        if enabled_modules == 0:
            st.markdown(
                '<div class="callout">Enable at least one module on the left to run the pipeline.</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="callout">{enabled_modules} module{"s" if enabled_modules != 1 else ""} selected</div>',
                unsafe_allow_html=True,
            )

        run_clicked = st.button(
            "Run Cleaning Pipeline",
            use_container_width=True,
            type="primary",
            disabled=(enabled_modules == 0),
        )

        from src.engine.cleaning_engine import clean_dataset
        from src.engine.report_builder import build_corrections_report

        if run_clicked:
            with st.spinner("Running enterprise cleaning pipeline..."):
                # Map UI state to the new cleaning config
                config = {
                    "column_cleanup": {
                        "header_case": "Snake Case" if do_col and do_snake else "Title Case",
                        "remove_constant": do_col and do_drop_empty
                    },
                    "duplicates": {
                        "remove_duplicate_cols": do_dups,
                        "row_strategy": "first" if dup_keep == "first" else ("last" if dup_keep == "last" else "drop_all"),
                        "key_columns": dup_cols if do_dups and dup_cols else []
                    },
                    "numeric": {
                        "columns": dtype_num_cols if do_dtype else []
                    },
                    "text_categorical": {
                        "columns": tc_cols if do_text else [],
                        "case_normalization": case_rule if do_text else "none",
                        "remove_special_chars": do_text and do_spec,
                        "categorical_mapping": {}
                    },
                    "boolean": {
                        "columns": [] # To be implemented if UI toggle added
                    },
                    "dates": {
                        "columns": {c: d_fmt for c in d_cols} if do_date else {}
                    },
                    "ranges": {
                        "numeric_columns": {r_col: {"min": r_vals[0], "max": r_vals[1], "action": r_action}} if do_range and r_col else {},
                        "no_future_dates": True
                    },
                    "missing_values": {
                        "numeric_strategy": "mean" if "mean" in miss_strat else ("median" if "median" in miss_strat else ("mode" if "mode" in miss_strat else ("zero" if "zero" in miss_strat else ("drop" if "drop" in miss_strat else "nan")))),
                        "categorical_strategy": "mode" if "mode" in miss_strat else ("N/A" if "N/A" in miss_strat else ("drop" if "drop" in miss_strat else "nan")),
                        "date_strategy": "ffill" if "ffill" in miss_strat else ("bfill" if "bfill" in miss_strat else "nan"),
                        "custom_numeric_fill": m_custom if "custom" in miss_strat else 0
                    },
                    "scaling": {
                        "method": "none", # Extend UI later
                        "columns": []
                    }
                }

                # Run New Engine
                final_df, audit_trail = clean_dataset(df, config)

                # Build professional Markdown report
                report_md = build_corrections_report(audit_trail, df, final_df)

                # Assemble final report structure for session state
                report_obj = {
                    "audit_trail": audit_trail,
                    "corrections_report": report_md,
                    "quality_score": _compute_quality_score(final_df),
                    "summary": {
                        "total_actions": len([a for a in audit_trail if a.get("cells_changed", 0) > 0 or a.get("rows_affected", 0) > 0]),
                        "retention_rate": round((len(final_df) / len(df)) * 100, 1) if len(df) > 0 else 0
                    },
                    "statistics": {
                        "final": {
                            "rows": len(final_df),
                            "missing_pct": round((final_df.isnull().sum().sum() / (final_df.size or 1)) * 100, 1)
                        }
                    }
                }

                st.session_state[CLEAN_DF]   = final_df
                st.session_state[RUN_RESULT] = report_obj

        # ── Results ───────────────────────────────────────────────────────────
        if st.session_state.get(CLEAN_DF) is not None:
            report   = st.session_state.get(RUN_RESULT) or {}
            final_df = st.session_state[CLEAN_DF]

            if not isinstance(final_df, pd.DataFrame):
                st.error("Clean output unavailable. Re-run the pipeline.")
                st.stop()

            score   = report.get("quality_score", 0) if isinstance(report, dict) else 0
            summary = report.get("summary", {})      if isinstance(report, dict) else {}
            stats   = report.get("statistics", {}).get("final", {}) if isinstance(report, dict) else {}

            row_delta     = len(final_df) - len(df)
            col_delta     = len(final_df.columns) - len(df.columns)
            row_delta_cls = "neg" if row_delta <= 0 else "pos"
            col_delta_cls = "neg" if col_delta <= 0 else "pos"

            st.divider()
            st.markdown('<div class="slabel">Results</div>', unsafe_allow_html=True)

            st.markdown(
                f"""
                <div class="result-card">
                    <div class="result-card-title">Pipeline Output</div>
                    <div class="result-metrics">
                        <div class="rm-item">
                            <div class="rm-val">{len(final_df):,}</div>
                            <div class="rm-lbl">Rows</div>
                            <div class="rm-delta {row_delta_cls}">{row_delta:+,}</div>
                        </div>
                        <div class="rm-item">
                            <div class="rm-val">{len(final_df.columns)}</div>
                            <div class="rm-lbl">Columns</div>
                            <div class="rm-delta {col_delta_cls}">{col_delta:+,}</div>
                        </div>
                        <div class="rm-item">
                            <div class="rm-val">{final_df.isnull().sum().sum():,}</div>
                            <div class="rm-lbl">Remaining missing</div>
                        </div>
                        <div class="rm-item">
                            <div class="rm-val blue">{score}%</div>
                            <div class="rm-lbl">Quality score</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            st.markdown(
                f"""
                <div class="log-strip">
                    mutations:&nbsp;<span>{summary.get("total_actions", 0)}</span>
                    &nbsp;&nbsp; retention:&nbsp;<span>{summary.get("retention_rate", 100)}%</span>
                    &nbsp;&nbsp; missing remaining:&nbsp;<span>{stats.get("missing_pct", "N/A")}%</span>
                </div>
                """,
                unsafe_allow_html=True,
            )

            corrections_md = report.get("corrections_report", "")
            if corrections_md:
                with st.expander("Corrections report", expanded=False):
                    st.markdown(corrections_md)

            tab1, tab2 = st.tabs(["Raw", "Cleaned"])
            with tab1:
                st.dataframe(df.head(20), use_container_width=True)
            with tab2:
                st.dataframe(final_df.head(20), use_container_width=True)

            st.divider()
            b1, b2 = st.columns(2)
            csv_bytes = final_df.to_csv(index=False).encode("utf-8")
            b1.download_button(
                label="Download CSV",
                data=csv_bytes,
                file_name=f"cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
            if b2.button("Continue to Export", type="primary", use_container_width=True):
                st.session_state[CURRENT_PAGE] = "Export"
                st.rerun()