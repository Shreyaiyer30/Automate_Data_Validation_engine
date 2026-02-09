import streamlit as st
import pandas as pd
from typing import Dict

def render_dynamic_rules_editor(df: pd.DataFrame, processor):
    """UI for viewing and editing dynamic cleaning rules"""
    
    st.markdown("### 🔧 Dynamic Cleaning Rules")
    st.info("AI has analyzed your dataset and suggested the following cleaning rules. You can override them below.")
    
    # Generate rules automatically
    # analysis = processor.analyze_data_quality(df)
    # auto_rules = analysis['suggested_rules']
    # quality_score = analysis.get('quality_score', 100)
    # st.metric("Data Quality Score", f"{quality_score:.1f}/100")
    
    analysis = processor.analyze_data_quality(df)

    auto_rules = analysis.get('suggested_rules', {})

    if not auto_rules:
        st.warning("⚠️ No suggested rules available.")
        return {}

    quality_score = analysis.get('quality_score', 100)
    st.metric("Data Quality Score", f"{quality_score:.1f}/100")

    # Create the two-pane layout
    left_col, right_col = st.columns([1, 3])
    
    # 1. Left Pane: Column Selector with Health Indicators
    with left_col:
        st.markdown("#### 📑 Columns")
        selected_col = None
        
        # Global settings first
        if st.button("🌐 Global Rules", use_container_width=True, type="primary" if st.session_state.get('selected_col') == "_global" else "secondary"):
            st.session_state.selected_col = "_global"
            st.rerun() if 'selected_col' not in st.session_state else None

        for col in df.columns:
            patterns = analysis['column_analysis'].get(col, {})
            has_issues = patterns.get('missing_count', 0) > 0 or patterns.get('has_outliers', False) or patterns.get('has_special_chars', False)
            badge = "🔴" if has_issues else "🟢"
            
            label = f"{badge} {col}"
            if st.button(label, key=f"sel_{col}", use_container_width=True, 
                         type="primary" if st.session_state.get('selected_col') == col else "secondary"):
                st.session_state.selected_col = col
                st.rerun()

    # 2. Right Pane: Detailed Configuration
    with right_col:
        current_selection = st.session_state.get('selected_col', "_global")
        
        # --- ARCHITECTURAL CONTRACT: CAPABILITY CHECK ---
        has_simulation = hasattr(processor, 'simulate_impact')
        
        if has_simulation:
            # Real-time Simulation Feedback
            projected_score = processor.simulate_impact(df, auto_rules)
            score_delta = projected_score - quality_score
            delta_color = "green" if score_delta >= 0 else "red"
            delta_sign = "+" if score_delta > 0 else ""
            
            st.markdown(f"""
            <div class="glass-card" style="padding: 10px; border-left: 5px solid #00ffff; margin-bottom: 20px;">
                <p style="margin:0; font-size: 0.8rem; color: #888;">Projected Quality Score</p>
                <h3 style="margin:0;">{projected_score:.1f}% <span style="color: {delta_color}; font-size: 0.9rem;">({delta_sign}{score_delta:.1f}%)</span></h3>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.warning("⚠️ Rule impact preview unavailable – cleaning logic not affected.")

        if current_selection == "_global":
            st.markdown("#### 🌐 Global Cleaning Configuration")
            remove_dups = st.checkbox(
                "Remove duplicate rows", 
                value=auto_rules.get('_global', {}).get('remove_duplicates', False)
            )
            if '_global' not in auto_rules: auto_rules['_global'] = {}
            auto_rules['_global']['remove_duplicates'] = remove_dups
            
        elif current_selection in df.columns:
            col = current_selection
            rules = auto_rules.get(col, {})
            patterns = analysis['column_analysis'][col]
            
            st.markdown(f"#### 📊 Column: {col}")
            
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Field Health**")
                st.write(f"Type: `{df[col].dtype}`")
                st.write(f"Missing Values: {patterns['missing_count']} ({patterns['missing_percentage']:.1f}%)")
                if patterns.get('has_outliers'):
                    st.warning(f"⚠️ {patterns['outlier_count']} outliers detected")
            
            with c2:
                st.markdown("**Importance & Logic**")
                importance = st.selectbox(
                    "Business Criticality",
                    ["High", "Medium", "Low"],
                    index=["HIGH", "MEDIUM", "LOW"].index(rules.get('importance_level', 'MEDIUM')),
                    key=f"imp_sel_{col}"
                )
                auto_rules[col]['importance_level'] = importance.upper()

            st.markdown("---")
            st.markdown("**Active Transformations**")
            
            # Grouping transformations
            t_col1, t_col2 = st.columns(2)
            
            with t_col1:
                if df[col].dtype == 'object':
                    handle_missing = st.selectbox("Handle Missing Values",
                        ["fill_with_mode", "fill_with_unknown", "forward_fill", "backward_fill", "do_nothing"],
                        key=f"miss_pane_{col}")
                else:
                    handle_missing = st.selectbox("Handle Missing Values",
                        ["impute_with_median", "impute_with_mean", "forward_fill", "backward_fill", "do_nothing"],
                        key=f"miss_pane_{col}")
                auto_rules[col]['handle_missing'] = handle_missing
                
            with t_col2:
                if pd.api.types.is_numeric_dtype(df[col]):
                    st.write("Method: IQR Clipping (Global)")
                elif pd.api.types.is_datetime64_any_dtype(df[col]) or df[col].dtype == 'object':
                    date_fmt = st.text_input("Expected Date Format", value=rules.get('date_format', ''), key=f"date_pane_{col}")
                    if date_fmt: auto_rules[col]['date_format'] = date_fmt

            # Toggles for others
            st.markdown("**Refinements**")
            ref_cols = st.columns(3)
            idx = 0
            for r_key, r_val in rules.items():
                if r_key in ['handle_missing', 'importance_level', 'date_format']: continue
                if isinstance(r_val, bool):
                    with ref_cols[idx % 3]:
                        auto_rules[col][r_key] = st.toggle(r_key.replace('_', ' ').title(), value=r_val, key=f"tog_{col}_{r_key}")
                    idx += 1
        
    return auto_rules
    
    return auto_rules