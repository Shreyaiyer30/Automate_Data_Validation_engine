# ui/components/type_inspector.py
import streamlit as st
import pandas as pd
import plotly.express as px

def render_type_inspector(df, processor):
    """Show detailed type detection information"""
    
    st.header("🔍 Data Type Inspector")
    st.markdown("Analyze and convert data types in your dataset with intelligent suggestions.")
    
    if df is None or df.empty:
        st.warning("No data loaded. Please upload a dataset first.")
        return
    
    # Generate type detection report
    with st.spinner("Analyzing data types..."):
        type_report = processor.detect_data_types_report(df)
    
    # Show summary cards
    col1, col2, col3, col4 = st.columns(4)
    
    total_columns = len(df.columns)
    numeric_cols = sum(1 for col in df.columns if pd.api.types.is_numeric_dtype(df[col]))
    date_cols = sum(1 for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col]))
    string_cols = total_columns - numeric_cols - date_cols
    
    with col1:
        st.metric("Total Columns", total_columns)
    with col2:
        st.metric("Numeric", numeric_cols)
    with col3:
        st.metric("Dates", date_cols)
    with col4:
        st.metric("Text/Other", string_cols)
    
    # Type conversion suggestions
    st.subheader("📋 Conversion Suggestions")
    
    suggestions = []
    for col, info in type_report.items():
        current = info['current_dtype']
        detected = info['detected_type']
        
        # Suggest if it's currently an object but detected as something specific
        if current == 'object' and detected not in ['string', 'categorical', 'unknown']:
            suggestions.append({
                'column': col,
                'current': current,
                'suggested': detected,
                'confidence': info['confidence'],
                'sample': info['sample_values'][:3] if info['sample_values'] else []
            })
    
    if suggestions:
        st.info(f"Found {len(suggestions)} columns that can be optimized.")
        
        for i, sug in enumerate(suggestions):
            with st.expander(f"✨ {sug['column']}: {sug['current']} → {sug['suggested']} ({sug['confidence']:.0%} confidence)"):
                c1, c2, c3 = st.columns([2, 2, 1])
                with c1:
                    st.markdown(f"**Sample values:**\n`{sug['sample']}`")
                with c2:
                    st.write(f"**Missing:** {df[sug['column']].isna().sum()}")
                    st.write(f"**Unique:** {df[sug['column']].nunique()}")
                with c3:
                    if st.button(f"Apply Fix", key=f"fix_{i}"):
                        # Manual trigger for specific column logic
                        try:
                            from src.data_type_detector import DataTypeDetector
                            df[sug['column']] = DataTypeDetector.convert_column(df[sug['column']], sug['suggested'])
                            st.success(f"Fixed {sug['column']}!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
    else:
        st.success("🎉 All columns appear to have appropriate data types!")
    
    st.markdown("---")
    
    # Detailed analysis
    st.subheader("📊 Column-level deep dive")
    selected_col = st.selectbox("Select column to inspect", df.columns)
    
    if selected_col:
        info = type_report[selected_col]
        
        d1, d2 = st.columns([1, 2])
        with d1:
            st.markdown(f"""
            **Current Type:** `{info['current_dtype']}`  
            **Inferred As:** `{info['detected_type']}`  
            **Confidence:** `{info['confidence']:.2f}`  
            **Nulls:** `{info['missing_count']}`  
            **Uniques:** `{info['unique_count']}`
            """)
        
        with d2:
            if pd.api.types.is_numeric_dtype(df[selected_col]):
                fig = px.histogram(df, x=selected_col, title=f"Distribution: {selected_col}", color_discrete_sequence=['#764ba2'])
                st.plotly_chart(fig, use_container_width=True)
            else:
                top_10 = df[selected_col].value_counts().head(10)
                fig = px.bar(x=top_10.index, y=top_10.values, title=f"Top 10 Values: {selected_col}", color_discrete_sequence=['#667eea'])
                st.plotly_chart(fig, use_container_width=True)
