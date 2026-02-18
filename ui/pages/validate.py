import streamlit as st
import pandas as pd
import numpy as np
import json
from datetime import datetime
from src.core.constants import PIPELINE_STEPS
from src.core.session_state import RAW_DF, VALIDATION_RULES, VALIDATION_REPORT, PIPELINE_STAGE, CURRENT_PAGE
from ui.components.pipeline_tracker import render_pipeline_tracker
from ui.components.score_widgets import score_ring
from src.engine.validation_engine import DataValidationEngine


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
    --danger-bg:#fef2f2;
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
.stat-pill.active b { color: var(--accent); }

/* Section label */
.slabel {
    font-size: 0.67rem; font-weight: 600; letter-spacing: .1em;
    text-transform: uppercase; color: var(--faint); margin-bottom: 8px;
}

/* Column list button overrides */
.stButton > button[kind="secondary"] {
    background: var(--surface) !important;
    border: 1px solid var(--border) !important;
    color: var(--text) !important;
    font-family: var(--font) !important;
    font-weight: 400 !important;
    font-size: 0.82rem !important;
    border-radius: 6px !important;
    text-align: left !important;
    justify-content: flex-start !important;
    padding: 6px 10px !important;
}
.stButton > button[kind="secondary"]:hover {
    border-color: var(--border2) !important;
    background: var(--bg) !important;
}
.stButton > button[kind="primary"] {
    background: var(--accent) !important;
    border: none !important;
    color: #fff !important;
    font-family: var(--font) !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    border-radius: var(--radius) !important;
    padding: 0.55rem 1.2rem !important;
    box-shadow: 0 1px 3px rgba(37,99,235,.2) !important;
    transition: opacity .15s !important;
}
.stButton > button[kind="primary"]:hover { opacity: .88 !important; }

/* Download button */
.stDownloadButton > button {
    background: var(--green-bg) !important;
    border: 1px solid #bbf7d0 !important;
    color: var(--green) !important;
    font-family: var(--font) !important;
    font-weight: 600 !important;
    font-size: 0.82rem !important;
    border-radius: var(--radius) !important;
}

/* Rule pill */
.rule-pill {
    display: flex; align-items: center; justify-content: space-between;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 7px 10px;
    font-size: 0.8rem; color: var(--text); margin-bottom: 4px;
}
.rule-pill b { font-weight: 600; color: var(--accent); }
.rule-pill .rule-meta { font-size: 0.74rem; color: var(--muted); margin-left: 6px; }

/* Callout */
.callout {
    background: var(--accent-bg); border-left: 3px solid var(--accent);
    border-radius: 0 6px 6px 0; padding: 8px 12px;
    font-size: 0.78rem; color: #1e40af; margin-bottom: 1rem;
}
.callout.warn {
    background: #fffbeb; border-left-color: var(--warn); color: #92400e;
}
.callout.danger {
    background: var(--danger-bg); border-left-color: var(--danger); color: #991b1b;
}
.callout.success {
    background: var(--green-bg); border-left-color: var(--green); color: #14532d;
}

/* Report card */
.report-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 1.2rem 1.4rem; margin-bottom: 0.75rem;
}
.report-card-title {
    font-size: 0.67rem; font-weight: 600; color: var(--faint);
    text-transform: uppercase; letter-spacing: .08em; margin-bottom: 1rem;
}
.report-metrics { display: flex; gap: 2rem; }
.rm-item { flex: 1; }
.rm-val {
    font-size: 1.4rem; font-weight: 600; color: var(--text);
    font-family: var(--mono); line-height: 1.2;
}
.rm-val.green  { color: var(--green); }
.rm-val.danger { color: var(--danger); }
.rm-val.blue   { color: var(--accent); }
.rm-lbl { font-size: 0.72rem; color: var(--muted); margin-top: 2px; }

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
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background: var(--bg) !important;
    border-color: var(--border) !important;
    border-radius: 6px !important;
    font-size: 0.82rem !important;
}

/* Misc */
hr { border-color: var(--border) !important; margin: 1.2rem 0 !important; }
.stDataFrame { border-radius: 6px !important; overflow: hidden; }
.stAlert { border-radius: 6px !important; font-size: 0.82rem !important; }
[data-testid="stMetricValue"] { font-family: var(--mono) !important; }
div[data-testid="stTabs"] button {
    font-family: var(--font) !important; font-size: 0.8rem !important; font-weight: 500 !important;
}

/* Search input */
.stTextInput > div > div > input {
    background: var(--surface) !important;
    border-color: var(--border) !important;
    border-radius: 6px !important;
    font-size: 0.82rem !important;
    font-family: var(--font) !important;
}
</style>
"""


def render():
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    df = st.session_state.get(RAW_DF)
    if df is None:
        st.info("Upload a dataset to get started.")
        return

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown('<p class="page-title">Data Validation</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-sub">Build rule-driven quality checks before the cleaning stage.</p>', unsafe_allow_html=True)

    render_pipeline_tracker(active_idx=2, pipeline_steps=PIPELINE_STEPS)

    if VALIDATION_RULES not in st.session_state:
        st.session_state[VALIDATION_RULES] = {}

    rules        = st.session_state[VALIDATION_RULES]
    selected_col = st.session_state.get("_val_selected_col")
    all_cols     = df.columns.tolist()
    total_rules  = sum(len(v) for v in rules.values())

    # ── Stat pills ────────────────────────────────────────────────────────────
    st.markdown(
        f"""
        <div class="stat-row">
            <div class="stat-pill"><b>{len(all_cols)}</b>&nbsp;columns</div>
            <div class="stat-pill active"><b>{len(rules)}</b>&nbsp;configured</div>
            <div class="stat-pill active"><b>{total_rules}</b>&nbsp;total rules</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    col_left, col_right = st.columns([4, 7], gap="large")

    # ══════════════════════════════════════════════════════════════════════════
    # LEFT: COLUMN PICKER
    # ══════════════════════════════════════════════════════════════════════════
    with col_left:
        st.markdown('<div class="slabel">Columns</div>', unsafe_allow_html=True)

        search        = st.text_input("Search", placeholder="Filter columns...", label_visibility="collapsed", key="col_search")
        filtered_cols = [c for c in all_cols if search.lower() in str(c).lower()]

        if not filtered_cols:
            st.caption("No columns matched.")
        else:
            for col in filtered_cols:
                rule_count = len(rules.get(col, []))
                label      = f"{col}  ({rule_count})" if rule_count else col
                btn_type   = "primary" if selected_col == col else "secondary"
                if st.button(label, key=f"sel_{col}", use_container_width=True, type=btn_type):
                    st.session_state["_val_selected_col"] = col
                    st.rerun()

        st.divider()
        st.markdown('<div class="slabel">Global Actions</div>', unsafe_allow_html=True)

        if st.button("Apply Common Templates", use_container_width=True):
            _apply_templates(all_cols)
            st.rerun()

        if st.button("Clear All Rules", use_container_width=True):
            st.session_state[VALIDATION_RULES] = {}
            st.session_state.pop("_val_selected_col", None)
            st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # RIGHT: RULE BUILDER
    # ══════════════════════════════════════════════════════════════════════════
    with col_right:
        if not selected_col:
            st.markdown(
                '<div class="callout">Select a column on the left to configure validation rules.</div>',
                unsafe_allow_html=True,
            )
            if rules:
                st.markdown('<div class="slabel">Configured Rules Summary</div>', unsafe_allow_html=True)
                _render_summary_view(rules)
            return

        st.markdown(f'<div class="slabel">Rules &mdash; <span style="color:var(--text);font-weight:600;">{selected_col}</span></div>', unsafe_allow_html=True)

        col_rules = rules.get(selected_col, [])

        # Applied rules list
        if col_rules:
            for i, r in enumerate(col_rules):
                details = " ".join(filter(None, [
                    r.get("operator", ""), str(r.get("min", "")), str(r.get("max", "")),
                    str(r.get("value", "")), r.get("pattern_type", ""),
                ])).strip()
                rc1, rc2 = st.columns([9, 1])
                rc1.markdown(
                    f'<div class="rule-pill">'
                    f'<span><b>{r["type"]}</b>'
                    f'<span class="rule-meta">{details}</span></span>'
                    f'<span style="font-size:0.7rem;color:var(--muted);">{r.get("error_style","")}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                if rc2.button("x", key=f"del_{selected_col}_{i}", help="Remove rule"):
                    col_rules.pop(i)
                    rules[selected_col] = col_rules
                    st.rerun()
        else:
            st.markdown(
                '<div class="callout">No rules yet for this column. Add one below.</div>',
                unsafe_allow_html=True,
            )

        # Add rule expander
        with st.expander("Add a Rule", expanded=not col_rules):
            rule_type = st.selectbox("Rule Type", [
                "Whole number", "Decimal number", "Text length",
                "Date range", "Custom list", "Pattern",
                "Unique", "In Column", "Formula",
            ], key="rule_type_sel")

            new_rule = {"type": rule_type}

            if rule_type in ["Whole number", "Decimal number"]:
                op = st.selectbox("Operator", ["between", "greater than", "less than", "equal to"], key="num_op")
                new_rule["operator"] = op
                if op == "between":
                    c1, c2 = st.columns(2)
                    new_rule["min"] = c1.number_input("Min", value=0.0, key="num_min")
                    new_rule["max"] = c2.number_input("Max", value=100.0, key="num_max")
                else:
                    new_rule["value"] = st.number_input("Value", key="num_val")

            elif rule_type == "Text length":
                op = st.selectbox("Operator", ["between", "minimum", "maximum"], key="txt_op")
                new_rule["operator"] = op
                if op == "between":
                    c1, c2 = st.columns(2)
                    new_rule["min"] = c1.number_input("Min chars", value=0, step=1, key="txt_min")
                    new_rule["max"] = c2.number_input("Max chars", value=255, step=1, key="txt_max")
                else:
                    new_rule["value"] = st.number_input("Chars", step=1, key="txt_val")

            elif rule_type == "Date range":
                op = st.selectbox("Operator", ["between", "after", "before"], key="date_op")
                new_rule["operator"] = op
                if op == "between":
                    c1, c2 = st.columns(2)
                    new_rule["min"] = c1.date_input("Start Date", key="date_min").isoformat()
                    new_rule["max"] = c2.date_input("End Date",   key="date_max").isoformat()
                else:
                    new_rule["value"] = st.date_input("Target Date", key="date_val").isoformat()

            elif rule_type == "Custom list":
                raw_list = st.text_area("Values (comma-separated)", "Yes, No", key="list_vals")
                new_rule["values"] = [v.strip() for v in raw_list.split(",") if v.strip()]

            elif rule_type == "Pattern":
                p_type = st.selectbox("Pattern Type", ["Email", "Phone", "URL", "Custom Regex"], key="pat_type")
                new_rule["pattern_type"] = p_type
                if p_type == "Custom Regex":
                    new_rule["regex"] = st.text_input("Regex", r"^[A-Z]{3}-\d{4}$", key="pat_regex")
                    st.caption("Example: `^[A-Z]{3}-\\d{4}$` matches ABC-1234")

            elif rule_type == "In Column":
                other_cols = [c for c in all_cols if c != selected_col]
                new_rule["other_column"] = st.selectbox("Matching Column", other_cols, key="incol_sel")

            elif rule_type == "Formula":
                new_rule["formula"] = st.text_input("Pandas Eval Formula", f"`{selected_col}` > 0", key="formula_val")
                st.caption("Example: `` `Age` >= 18 ``")

            st.divider()
            c1, c2 = st.columns([3, 2])
            new_rule["error_message"] = c1.text_input("Error message", f"Invalid value for {selected_col}", key="err_msg")
            new_rule["error_style"]   = c2.radio("Style", ["Stop", "Warning", "Information"], horizontal=False, key="err_style")

            if st.button("Add Rule", use_container_width=True, type="primary"):
                if selected_col not in rules:
                    rules[selected_col] = []
                rules[selected_col].append(new_rule)
                st.session_state[VALIDATION_RULES] = rules
                st.rerun()

        # Copy rules
        other_cols = [c for c in all_cols if c != selected_col]
        if col_rules and other_cols:
            st.divider()
            st.markdown('<div class="slabel">Copy Rules</div>', unsafe_allow_html=True)
            copy_to = st.multiselect("Copy these rules to other columns", other_cols, key="copy_to_sel")
            if st.button("Copy Rules", disabled=not copy_to, key="copy_rules_btn"):
                for target in copy_to:
                    rules[target] = rules.get(target, []) + rules[selected_col]
                st.session_state[VALIDATION_RULES] = rules
                st.success(f"Rules copied to {len(copy_to)} column(s).")
                st.rerun()

    # ══════════════════════════════════════════════════════════════════════════
    # BOTTOM: RUN + SAVE/LOAD
    # ══════════════════════════════════════════════════════════════════════════
    st.divider()
    b1, b2, b3 = st.columns([2, 2, 3])

    if b1.button("Test (First 20 Rows)", use_container_width=True):
        res = DataValidationEngine.validate_dataset(df.head(20), rules)
        st.session_state["_val_preview"] = res

    if b2.button("Run Full Validation", use_container_width=True, type="primary"):
        with st.spinner("Validating..."):
            res = DataValidationEngine.validate_dataset(df, rules)
            st.session_state[VALIDATION_REPORT] = res
            st.session_state[PIPELINE_STAGE]    = max(st.session_state.get(PIPELINE_STAGE, 0), 3)

    with b3:
        with st.expander("Save / Load Rules"):
            if rules:
                st.download_button(
                    "Download Rules (JSON)",
                    json.dumps(rules, indent=2),
                    "validation_rules.json",
                    "application/json",
                    use_container_width=True,
                )
            uploaded = st.file_uploader("Upload Rules (JSON)", type="json", key="rules_upload")
            if uploaded:
                try:
                    st.session_state[VALIDATION_RULES] = json.load(uploaded)
                    st.success("Rules loaded.")
                    st.rerun()
                except Exception:
                    st.error("Invalid JSON file.")

    # ── Report ────────────────────────────────────────────────────────────────
    report = st.session_state.get(VALIDATION_REPORT) or st.session_state.get("_val_preview")
    if report:
        st.divider()
        _render_report_view(report, df)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _render_summary_view(rules: dict):
    rows = []
    for col, col_rules in rules.items():
        for r in col_rules:
            details = " ".join(filter(None, [
                r.get("operator", ""), str(r.get("min", "")), str(r.get("max", "")),
                str(r.get("value", "")), r.get("pattern_type", ""),
            ])).strip()
            rows.append({"Column": col, "Type": r["type"], "Details": details, "Style": r["error_style"]})
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_report_view(report: dict, df: pd.DataFrame):
    is_preview = report["total_rows"] < len(df)
    suffix     = " (Preview - First 20 Rows)" if is_preview else ""
    score      = (report["passed"] / report["total_rows"] * 100) if report["total_rows"] > 0 else 100.0

    st.markdown('<div class="slabel">Validation Report' + suffix + '</div>', unsafe_allow_html=True)

    # Metrics card
    pass_rate  = round(score, 1)
    fail_count = report["failed"]
    fail_cls   = "danger" if fail_count > 0 else "green"

    st.markdown(
        f"""
        <div class="report-card">
            <div class="report-card-title">Summary</div>
            <div class="report-metrics">
                <div class="rm-item">
                    <div class="rm-val blue">{pass_rate}%</div>
                    <div class="rm-lbl">Pass rate</div>
                </div>
                <div class="rm-item">
                    <div class="rm-val">{report['total_rows']:,}</div>
                    <div class="rm-lbl">Total rows</div>
                </div>
                <div class="rm-item">
                    <div class="rm-val green">{report['passed']:,}</div>
                    <div class="rm-lbl">Passed</div>
                </div>
                <div class="rm-item">
                    <div class="rm-val {fail_cls}">{fail_count:,}</div>
                    <div class="rm-lbl">Failed</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if fail_count > 0:
        st.markdown(
            '<div class="callout danger">Validation detected issues. Review the flagged rows below.</div>',
            unsafe_allow_html=True,
        )

        report_df = report["report_df"]

        def _style_failed(row):
            if row.get("_val_status") == "FAIL":
                return ["background-color: #fef2f2; color: #991b1b"] * len(row)
            return [""] * len(row)

        tab1, tab2 = st.tabs(["All Rows (flagged)", "Failed Only"])
        with tab1:
            st.dataframe(
                report_df.head(50).style.apply(_style_failed, axis=1),
                use_container_width=True,
            )
        with tab2:
            failed_df = report_df[report_df["_val_status"] == "FAIL"]
            st.dataframe(failed_df.head(100), use_container_width=True)

        with st.expander("Rule violations by column", expanded=False):
            for col, failures in report.get("results_by_col", {}).items():
                if failures:
                    st.markdown(f"**{col}**")
                    st.dataframe(pd.DataFrame(failures), use_container_width=True, hide_index=True)

    else:
        st.markdown(
            '<div class="callout success">All rows passed validation checks.</div>',
            unsafe_allow_html=True,
        )

    if not is_preview:
        st.divider()
        dl_col, nav_col = st.columns(2)
        dl_col.download_button(
            "Download Validation Status (CSV)",
            report["report_df"].to_csv(index=False).encode("utf-8"),
            "validation_status.csv",
            "text/csv",
            use_container_width=True,
        )
        if nav_col.button("Continue to Cleaning Lab", type="primary", use_container_width=True):
            st.session_state[CURRENT_PAGE] = "Clean"
            st.rerun()


def _apply_templates(cols: list):
    rules = st.session_state[VALIDATION_RULES]

    email_kw = ["email"]
    phone_kw = ["phone", "mobile", "contact"]
    amt_kw   = ["amount", "salary", "price", "cost", "revenue"]
    pin_kw   = ["pincode", "pin_code", "zip"]
    pan_kw   = ["pan"]

    for c in cols:
        cl = str(c).lower()

        if any(kw in cl for kw in email_kw):
            rules.setdefault(c, []).append(
                {"type": "Pattern", "pattern_type": "Email", "error_message": "Invalid email format", "error_style": "Stop"}
            )
        if any(kw in cl for kw in phone_kw):
            rules.setdefault(c, []).append(
                {"type": "Pattern", "pattern_type": "Phone", "error_message": "Invalid phone format", "error_style": "Stop"}
            )
        if any(kw in cl for kw in amt_kw):
            rules.setdefault(c, []).append(
                {"type": "Decimal number", "operator": "greater than", "value": 0.0, "error_message": "Must be positive", "error_style": "Stop"}
            )
        if any(kw in cl for kw in pin_kw):
            rules.setdefault(c, []).append(
                {"type": "Pattern", "pattern_type": "Custom Regex", "regex": r"^\d{6}$", "error_message": "Invalid PIN code (6 digits)", "error_style": "Stop"}
            )
        if any(kw in cl for kw in pan_kw):
            rules.setdefault(c, []).append(
                {"type": "Pattern", "pattern_type": "Custom Regex", "regex": r"^[A-Z]{5}\d{4}[A-Z]{1}$", "error_message": "Invalid PAN format (ABCDE1234F)", "error_style": "Stop"}
            )
        if cl == "age":
            rules.setdefault(c, []).append(
                {"type": "Whole number", "operator": "between", "min": 0, "max": 120, "error_message": "Age must be 0-120", "error_style": "Stop"}
            )

    st.session_state[VALIDATION_RULES] = rules
    st.toast("Templates applied.")