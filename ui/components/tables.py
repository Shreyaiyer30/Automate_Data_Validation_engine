import streamlit as st
import pandas as pd

def render_excel_table(df: pd.DataFrame, max_rows: int = 200, height: int = 380):
    """Renders a pandas DataFrame with Excel-like styling in Streamlit."""
    if df is None:
        st.info("No data available to display.")
        return

    # Limit rows for performance if needed
    view_df = df.head(max_rows)
    
    # Apply Excel-like styling using Pandas Styler
    styled_df = view_df.style.set_properties(**{
        'background-color': '#0e1117',
        'color': '#e2e8f0',
        'border-color': '#1e2535'
    }).set_table_styles([
        {'selector': 'th', 'props': [('background-color', '#161b27'), ('color', '#64748b'), ('font-weight', '600')]},
        {'selector': 'td', 'props': [('border', '1px solid #1e2535')]}
    ])
    
    st.dataframe(
        styled_df,
        use_container_width=True,
        height=height
    )
