import streamlit as st
import pandas as pd
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_processor import DataProcessor

# Page Layout
st.set_page_config(page_title="Data Validation & Cleaning Engine", layout="wide")

# CSS for styling
def local_css(file_name):
    if Path(file_name).exists():
        with open(file_name) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

local_css("ui/assets/style.css")

def main():
    st.title("🛡️ Automated Data Validation & Cleaning Engine")
    st.markdown("---")

    # Initialization
    if 'processor' not in st.session_state:
        st.session_state.processor = DataProcessor()

    # Sidebar
    st.sidebar.header("Navigation")
    uploaded_file = st.sidebar.file_uploader("Upload Raw CSV", type=["csv"])
    
    if uploaded_file:
        st.success(f"File uploaded: {uploaded_file.name}")
        
        # Process data
        with st.spinner("Analyzing and cleaning data..."):
            try:
                # Read for preview
                df_raw = pd.read_csv(uploaded_file)
                uploaded_file.seek(0) # Reset pointer
                
                # Full processing
                df_cleaned, report = st.session_state.processor.process_data(uploaded_file)
                
                # Tabbed UI
                tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "🔍 Raw Data", "✨ Cleaned Data"])
                
                with tab1:
                    col1, col2, col3, col4 = st.columns(4)
                    summary = report['summary']
                    col1.metric("Missing Values Fixed", summary['total_missing_before'])
                    col2.metric("Duplicates Removed", summary['total_duplicates_before'])
                    col3.metric("Outliers Capped", summary['total_outliers_detected'])
                    col4.metric("Total Corrections", summary['total_corrections_made'])
                    
                    st.markdown("### Quality Report Details")
                    st.json(report)
                    
                    st.sidebar.markdown("---")
                    csv_cleaned = df_cleaned.to_csv(index=False).encode('utf-8')
                    st.sidebar.download_button(
                        label="Download Cleaned CSV",
                        data=csv_cleaned,
                        file_name=f"cleaned_{uploaded_file.name}",
                        mime='text/csv'
                    )

                with tab2:
                    st.subheader("Raw Data Preview (First 100 rows)")
                    st.dataframe(df_raw.head(100))
                    st.info(f"Shape: {df_raw.shape}")

                with tab3:
                    st.subheader("Cleaned Data Preview (First 100 rows)")
                    st.dataframe(df_cleaned.head(100))
                    st.info(f"Shape: {df_cleaned.shape}")

            except Exception as e:
                st.error(f"Error processing data: {e}")
    else:
        st.info("Please upload a CSV file from the sidebar to begin.")

if __name__ == "__main__":
    main()
