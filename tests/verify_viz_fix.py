import pandas as pd
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

def verify_fix():
    print("Starting verification of visualization mapping-aware logic...")
    
    # 1. Setup Data
    df_raw = pd.DataFrame({'num_critic_for_reviews': [100, 200, 150]})
    df_cleaned = pd.DataFrame({'Num Critic For Reviews': [100, 200, 150]})
    report = {
        'header_normalization': {
            'mapping': {'num_critic_for_reviews': 'Num Critic For Reviews'}
        }
    }
    
    # 2. Mock Streamlit and Plotly
    # We need to mock streamlit BEFORE importing the component if it's imported at top level
    # But here we import it inside or after mocking.
    
    import streamlit as st
    st.markdown = MagicMock()
    st.selectbox = MagicMock(return_value='Num Critic For Reviews')
    st.radio = MagicMock(return_value='Overlaid')
    st.plotly_chart = MagicMock()
    st.error = MagicMock()
    st.columns = MagicMock(return_value=[MagicMock(), MagicMock()])
    
    # 3. Import and Call the Component
    from ui.components.visualization import render_visualization_section
    
    try:
        render_visualization_section(df_raw, df_cleaned, report)
        
        # Verify no error was logged in UI
        if st.error.called:
            print(f"FAILED: st.error called with: {st.error.call_args}")
            return False
            
        print("SUCCESS: render_visualization_section completed without KeyError.")
        return True
        
    except KeyError as e:
        print(f"FAILED: KeyError raised: {str(e)}")
        return False
    except Exception as e:
        print(f"FAILED: Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = verify_fix()
    if not success:
        sys.exit(1)
