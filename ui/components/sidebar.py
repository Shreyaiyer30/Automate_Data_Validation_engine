
import streamlit as st
import pandas as pd
import sys
from pathlib import Path
import tempfile
import os

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from data_processor import DataProcessor
from config import Config

def render_sidebar():
    """Render sidebar with file upload and configuration options"""
    
    st.sidebar.title("🔧 Configuration")
    
    # ========== File Upload Section ==========
    st.sidebar.header("📁 Data Upload")
    
    uploaded_file = st.sidebar.file_uploader(
        "Choose a file",
        type=['csv', 'xlsx', 'xls', 'json'],
        help="Upload CSV, Excel, or JSON file for validation and cleaning"
    )
    
    # Process uploaded file
    if uploaded_file is not None:
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
                tmp_file.write(uploaded_file.getbuffer())
                tmp_path = tmp_file.name
            
            # Load data using DataProcessor
            processor = DataProcessor()
            st.session_state.df_original = processor.load_data(tmp_path)
            
            # Clean up temp file
            os.unlink(tmp_path)
            
            # Success message
            st.sidebar.success(f"✅ Successfully loaded!")
            
            # Display basic info
            df = st.session_state.df_original
            
            st.sidebar.info(f"""
            **Dataset Summary:**
            - **Rows:** {len(df):,}
            - **Columns:** {len(df.columns)}
            - **Size:** {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB
            - **Missing:** {df.isnull().sum().sum():,} values
            - **Duplicates:** {df.duplicated().sum():,} rows
            """)
            
        except Exception as e:
            st.sidebar.error(f"❌ Error loading file: {str(e)}")
            st.session_state.df_original = None
    
    # ========== Configuration Options ==========
    if st.session_state.df_original is not None:
        st.sidebar.markdown("---")
        
        # Validation Settings
        st.sidebar.header("🔍 Validation Settings")
        
        with st.sidebar.expander("Validation Options", expanded=False):
            st.session_state.missing_threshold = st.slider(
                "Missing Value Threshold (%)",
                min_value=0,
                max_value=100,
                value=30,
                step=5,
                help="Maximum percentage of missing values allowed per column"
            ) / 100.0
            
            st.session_state.outlier_detection_method = st.selectbox(
                "Outlier Detection Method",
                ['iqr', 'zscore', 'modified_zscore'],
                index=0,
                help="Statistical method for detecting outliers"
            )
            
            st.session_state.outlier_threshold = st.slider(
                "Outlier Threshold",
                min_value=1.0,
                max_value=5.0,
                value=3.0,
                step=0.1,
                help="Threshold for outlier detection (Z-score or IQR multiplier)"
            )
        
        # Cleaning Settings
        st.sidebar.header("🧹 Cleaning Settings")
        
        # Missing value strategy
        st.session_state.missing_strategy = st.sidebar.selectbox(
            "Missing Values Strategy",
            ['auto', 'mean', 'median', 'mode', 'forward_fill', 'backward_fill', 'interpolate', 'drop'],
            index=0,
            help="""Strategy for handling missing values:
            - auto: Median for numeric, mode for categorical
            - mean: Replace with column mean
            - median: Replace with column median
            - mode: Replace with most frequent value
            - forward_fill: Use previous value
            - backward_fill: Use next value
            - interpolate: Linear interpolation
            - drop: Remove rows with missing values
            """
        )
        
        # Outlier handling method
        st.session_state.outlier_method = st.sidebar.selectbox(
            "Outlier Handling Method",
            ['cap', 'remove', 'winsorize'],
            index=0,
            help="""Method for handling outliers:
            - cap: Cap values to IQR bounds
            - remove: Remove rows containing outliers
            - winsorize: Replace with 5th/95th percentile
            """
        )
        
        # Duplicate handling
        st.session_state.remove_duplicates = st.sidebar.checkbox(
            "Remove Duplicate Rows",
            value=True,
            help="Remove duplicate rows from the dataset"
        )
        
        if st.session_state.remove_duplicates:
            st.session_state.duplicate_keep = st.sidebar.radio(
                "Keep which duplicate?",
                ['first', 'last'],
                index=0,
                help="Which duplicate row to keep"
            )
        
        # Advanced Options
        st.sidebar.markdown("---")
        
        with st.sidebar.expander("⚙️ Advanced Options", expanded=False):
            # Text standardization
            st.session_state.standardize_text = st.checkbox(
                "Standardize Text Columns",
                value=False,
                help="Apply text standardization (lowercase, trim whitespace)"
            )
            
            if st.session_state.standardize_text:
                col1, col2 = st.columns(2)
                st.session_state.text_lowercase = col1.checkbox("Lowercase", value=True)
                st.session_state.text_strip = col2.checkbox("Strip spaces", value=True)
            
            # Data type conversion
            st.session_state.auto_convert_types = st.checkbox(
                "Auto-convert Data Types",
                value=False,
                help="Automatically detect and convert data types"
            )
            
            # Schema validation
            st.session_state.enable_schema_validation = st.checkbox(
                "Enable Schema Validation",
                value=False,
                help="Validate data against expected schema"
            )
            
            # Random seed for reproducibility
            st.session_state.random_seed = st.number_input(
                "Random Seed",
                min_value=0,
                max_value=9999,
                value=42,
                help="Set random seed for reproducible results"
            )
        
        # Export Settings
        st.sidebar.markdown("---")
        
        with st.sidebar.expander("💾 Export Options", expanded=False):
            st.session_state.export_format = st.radio(
                "Default Export Format",
                ['CSV', 'Excel', 'JSON', 'Parquet'],
                index=0,
                help="Default format for exporting cleaned data"
            )
            
            st.session_state.include_index = st.checkbox(
                "Include Index in Export",
                value=False,
                help="Include row index when exporting"
            )
            
            st.session_state.generate_html_report = st.checkbox(
                "Generate HTML Report",
                value=True,
                help="Generate HTML quality report"
            )
        
        # Action Buttons
        st.sidebar.markdown("---")
        st.sidebar.header("🎯 Actions")
        
        col1, col2 = st.sidebar.columns(2)
        
        # Reset button
        if col1.button("🔄 Reset", use_container_width=True):
            # Clear session state
            for key in list(st.session_state.keys()):
                if key != 'df_original':
                    del st.session_state[key]
            st.rerun()
        
        # Clear data button
        if col2.button("🗑️ Clear", use_container_width=True):
            # Clear all session state
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
        
        # Column selection for analysis
        st.sidebar.markdown("---")
        
        with st.sidebar.expander("📊 Column Selection", expanded=False):
            df = st.session_state.df_original
            
            # Select columns for analysis
            st.session_state.selected_columns = st.multiselect(
                "Select columns to analyze",
                options=df.columns.tolist(),
                default=df.columns.tolist(),
                help="Choose which columns to include in validation and cleaning"
            )
            
            # Quick filters
            st.markdown("**Quick Filters:**")
            
            if st.button("Select All Numeric", use_container_width=True):
                numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                st.session_state.selected_columns = numeric_cols
                st.rerun()
            
            if st.button("Select All Categorical", use_container_width=True):
                cat_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
                st.session_state.selected_columns = cat_cols
                st.rerun()
        
        # Display current configuration summary
        st.sidebar.markdown("---")
        
        with st.sidebar.expander("📋 Configuration Summary", expanded=False):
            st.markdown(f"""
            **Validation:**
            - Missing threshold: {st.session_state.get('missing_threshold', 0.3) * 100:.0f}%
            - Outlier method: {st.session_state.get('outlier_detection_method', 'iqr')}
            - Threshold: {st.session_state.get('outlier_threshold', 3.0)}
            
            **Cleaning:**
            - Missing strategy: {st.session_state.get('missing_strategy', 'auto')}
            - Outlier handling: {st.session_state.get('outlier_method', 'cap')}
            - Remove duplicates: {st.session_state.get('remove_duplicates', True)}
            
            **Export:**
            - Format: {st.session_state.get('export_format', 'CSV')}
            - Include index: {st.session_state.get('include_index', False)}
            """)
        
    else:
        # Show instructions when no file is uploaded
        st.sidebar.info("""
        ### 📤 Getting Started
        
        1. **Upload your dataset** using the file uploader above
        2. **Configure** validation and cleaning options
        3. **Analyze** your data in the dashboard
        4. **Clean** your data with one click
        5. **Export** the cleaned dataset
        
        ### 📊 Supported Formats
        - CSV files (.csv)
        - Excel files (.xlsx, .xls)
        - JSON files (.json)
        
        ### 💡 Tips
        - Start with a small sample for testing
        - Review validation results before cleaning
        - Always keep a backup of original data
        """)
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    <div style='text-align: center; color: #666; font-size: 0.8em;'>
        <p>Data Validation & Cleaning Engine v1.0</p>
        <p>Powered by Pandas, NumPy & Streamlit</p>
    </div>
    """, unsafe_allow_html=True)


# Initialize default session state values
def initialize_session_state():
    """Initialize session state with default values"""
    
    defaults = {
        'df_original': None,
        'df_cleaned': None,
        'validation_results': None,
        'quality_report': None,
        'outlier_info': None,
        'correction_log': None,
        'missing_strategy': 'auto',
        'outlier_method': 'cap',
        'outlier_detection_method': 'iqr',
        'remove_duplicates': True,
        'duplicate_keep': 'first',
        'missing_threshold': 0.3,
        'outlier_threshold': 3.0,
        'standardize_text': False,
        'text_lowercase': True,
        'text_strip': True,
        'auto_convert_types': False,
        'enable_schema_validation': False,
        'random_seed': 42,
        'export_format': 'CSV',
        'include_index': False,
        'generate_html_report': True,
        'selected_columns': []
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# Call initialization
initialize_session_state()