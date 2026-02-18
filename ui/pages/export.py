import streamlit as st
import pandas as pd
import io
import json
from datetime import datetime
from src.core.constants import PIPELINE_STEPS
from src.core.session_state import RAW_DF, CLEAN_DF, RUN_RESULT, UPLOAD_FILENAME, VALIDATION_REPORT, CURRENT_PAGE
from ui.components.pipeline_tracker import render_pipeline_tracker
from src.engine.exporter import assemble_json_report


def _build_multi_sheet_excel(raw_df, clean_df, validation_report, run_result):
    """Build strict 4-sheet workbook: Raw_Data, Clean_Data, Validation_Report, Corrections_Applied."""
    output = io.BytesIO()

    val_df = pd.DataFrame()
    if isinstance(validation_report, dict):
        if isinstance(validation_report.get("report_df"), pd.DataFrame):
            val_df = validation_report["report_df"].copy()
        elif validation_report.get("results_by_col"):
            rows = []
            for col, issues in validation_report.get("results_by_col", {}).items():
                for item in issues:
                    rows.append({
                        "column": col,
                        "row": item.get("row"),
                        "value": item.get("value"),
                        "rule": item.get("rule"),
                        "message": item.get("message"),
                        "style": item.get("style"),
                    })
            val_df = pd.DataFrame(rows)

    corrections = []
    if isinstance(run_result, dict):
        for entry in run_result.get("audit_trail", []):
            if isinstance(entry, dict):
                corrections.append(
                    {
                        "timestamp": entry.get("timestamp", ""),
                        "stage": entry.get("stage", ""),
                        "event": entry.get("event", ""),
                        "column": entry.get("column", ""),
                        "rows_affected": entry.get("rows_affected", ""),
                        "message": entry.get("message", ""),
                        "details": entry.get("details", ""),
                    }
                )

    corr_df = pd.DataFrame(corrections)
    if corr_df.empty:
        corr_df = pd.DataFrame([{"message": "No corrections logged"}])

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        raw_df.to_excel(writer, sheet_name="Raw_Data", index=False)
        clean_df.to_excel(writer, sheet_name="Clean_Data", index=False)
        (val_df if not val_df.empty else pd.DataFrame([{"message": "No validation report available"}])).to_excel(
            writer, sheet_name="Validation_Report", index=False
        )
        corr_df.to_excel(writer, sheet_name="Corrections_Applied", index=False)

    output.seek(0)
    return output.getvalue()


def render():
    """Renders the Export page."""
    run_result = st.session_state.get(RUN_RESULT)
    clean_df = st.session_state.get(CLEAN_DF)
    raw_df = st.session_state.get(RAW_DF)
    validation_report = st.session_state.get(VALIDATION_REPORT)

    st.markdown(
        """
        <div class="studio-shell">
            <div class="studio-head">
                <div>
                    <p class="studio-title">Export Studio</p>
                    <p class="studio-sub">Package the run output with traceable validation and correction sheets.</p>
                </div>
                <span class="studio-chip">Delivery</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_pipeline_tracker(active_idx=5, pipeline_steps=PIPELINE_STEPS)

    if clean_df is None:
        st.warning("Run the cleaning pipeline first.")
        return

    if run_result:
        state = run_result.get("state", "PASS")
        if state == "PASS":
            st.success("Pipeline passed. Export is ready.")
        elif state == "WARN":
            st.warning("Pipeline completed with warnings. Review outputs before distribution.")
        else:
            st.error("Pipeline failed. Resolve issues before export.")
            return

    fname = st.session_state.get(UPLOAD_FILENAME, "data.csv")
    stem = fname.rsplit(".", 1)[0]

    excel_bytes = _build_multi_sheet_excel(raw_df, clean_df, validation_report, run_result)
    report_json = assemble_json_report(run_result or {}, fname, raw_df, clean_df)

    st.markdown('<div class="studio-card">', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    c1.download_button(
        label="Download Cleaned CSV",
        data=clean_df.to_csv(index=False).encode("utf-8"),
        file_name=f"{stem}_cleaned_data.csv",
        mime="text/csv",
        use_container_width=True,
    )
    with c2:
        try:
            # New exporter takes: raw_df, clean_df, validation_report, corrections_df
            audit_trail = run_result.get("audit_trail", [])
            corr_df = pd.DataFrame(audit_trail)
            excel_data = generate_formatted_excel(raw_df, clean_df, validation_report, corr_df)
            st.download_button(
                label="⬇️ Download Excel",
                data=excel_data,
                file_name=f"{stem}_cleaned_package.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        except Exception as e:
            st.error(f"Excel export error: {e}")
    c3.download_button(
        label="Download JSON Report",
        data=report_json.encode("utf-8"),
        file_name=f"{stem}_report.json",
        mime="application/json",
        use_container_width=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="studio-card">', unsafe_allow_html=True)
    st.markdown('<div class="studio-card-title">Run Summary</div>', unsafe_allow_html=True)
    summary_df = pd.DataFrame(
        {
            "Field": ["Rows (Raw -> Clean)", "Cols (Raw -> Clean)", "State", "Actions Logged"],
            "Value": [
                f"{len(raw_df):,} -> {len(clean_df):,}",
                f"{len(raw_df.columns)} -> {len(clean_df.columns)}",
                (run_result or {}).get("state", "-"),
                len((run_result or {}).get("audit_trail", [])),
            ],
        }
    )
    st.table(summary_df)
    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("Back to Home", use_container_width=False):
        st.session_state[CURRENT_PAGE] = "Home"
        st.rerun()
