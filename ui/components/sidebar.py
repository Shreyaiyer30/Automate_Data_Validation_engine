import streamlit as st

def render_sidebar():
    """
    Renders a modern, section-based sidebar for navigation and cleaning controls.
    """
    st.sidebar.image("https://cdn-icons-png.flaticon.com/512/1802/1802276.png", width=80)
    st.sidebar.title("Data Engine v2")
    st.sidebar.markdown("---")
    
    # 1. Navigation
    st.sidebar.subheader("📍 Navigation")
    selection = st.sidebar.radio(
        "Go to",
        [
            "📁 Upload Dataset", 
            "📊 Data Overview", 
            "🔍 Type Inspector", 
            "🧹 Cleaning Lab", 
            "📉 Visualization", 
            "📥 Export Results"
        ]
    )
    
    st.sidebar.markdown("---")
    
    # 2. Type Conversion Controls
    st.sidebar.subheader("⚙️ Type Conversion")
    fix_dtypes = st.sidebar.checkbox("Smart Type Detection", value=True, help="Automatically detect and convert complex types (currency, objects, etc.)")
    
    with st.sidebar.expander("Conversion Options", expanded=False):
        convert_quoted = st.checkbox("Quoted Numbers", value=True, help='Convert "475" to 475')
        convert_currency = st.checkbox("Currency", value=True, help="Convert $1,234 to 1234.0")
        convert_percent = st.checkbox("Percentages", value=True, help="Convert 25% to 0.25")
        convert_bool = st.checkbox("Booleans", value=True, help="Convert yes/no, true/false")
        
        conf_threshold = st.slider(
            "Confidence Threshold",
            0.5, 1.0, 0.7, 0.05,
            help="Higher value = more cautious conversion"
        )
    
    st.sidebar.markdown("---")
    
    # 3. Cleaning Lab Controls
    st.sidebar.subheader("🧪 Cleaning Lab")
    
    with st.sidebar.expander("Control Center", expanded=True):
        remove_dups = st.checkbox("Remove Duplicates", value=True)
        standardize_text = st.checkbox("Standardize Text", value=True)
        
        missing_strategy = st.selectbox(
            "Missing Strategy",
            ["auto", "mean", "mode", "drop"]
        )
        
        z_threshold = st.slider(
            "Outlier Threshold",
            1.0, 5.0, 3.0, 0.5
        )

    st.sidebar.markdown("---")
    st.sidebar.caption("v2.2.0 | Advanced Mode")
    
    settings = {
        "fix_dtypes": fix_dtypes,
        "convert_quoted_numbers": convert_quoted,
        "convert_currency": convert_currency,
        "convert_percentages": convert_percent,
        "convert_booleans": convert_bool,
        "confidence_threshold": conf_threshold,
        "remove_duplicates": remove_dups,
        "standardize_text": standardize_text,
        "missing_value_strategy": missing_strategy,
        "outlier_zscore_threshold": z_threshold
    }
    
    return selection, settings