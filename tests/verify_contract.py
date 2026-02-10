import pandas as pd
import numpy as np
import sys
from pathlib import Path
import os
import tempfile

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from src.utils.file_loader import EnhancedDataLoader, FileFormat
from src.data_processor import DataProcessor

def verify_contract():
    print("Starting verification of DataFrame architectural contract...")
    
    # 1. Create a multi-sheet Excel file
    df1 = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
    df2 = pd.DataFrame({'c': [5, 6], 'd': [7, 8]})
    
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        with pd.ExcelWriter(tmp.name) as writer:
            df1.to_excel(writer, sheet_name='Sheet1', index=False)
            df2.to_excel(writer, sheet_name='Sheet2', index=False)
        tmp_path = tmp.name

    try:
        # 2. Load the file using the loader
        loader = EnhancedDataLoader(verbose=True)
        # Default behavior should now be sheet_name=0 -> returns DataFrame
        df_raw, metadata = loader.load(tmp_path)
        
        print(f"Loaded object type: {type(df_raw)}")
        if not isinstance(df_raw, pd.DataFrame):
            print(f"FAILED: Expected pandas.DataFrame, got {type(df_raw)}")
            return False
            
        print(f"Loaded sheet: {metadata.sheets_loaded[0] if metadata.sheets_loaded else 'Unknown'}")
        
        # 3. Pass to DataProcessor
        processor = DataProcessor()
        # This should NOT raise AttributeError anymore
        try:
            analysis = processor.analyze_data_quality(df_raw)
            print("SUCCESS: DataProcessor.analyze_data_quality executed on loaded data.")
        except Exception as e:
            print(f"FAILED: DataProcessor failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
            
        # 4. Explicit Negative Test: Pass a dict to DataProcessor
        print("Testing explicit type enforcement...")
        try:
            processor.analyze_data_quality({"not": "a dataframe"})
            print("FAILED: DataProcessor accepted a dict without raising TypeError.")
            return False
        except TypeError as e:
            print(f"PASSED: DataProcessor correctly rejected dict: {str(e)}")
            
        return True
        
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

if __name__ == "__main__":
    if verify_contract():
        print("\nAll contract verification tests PASSED.")
        sys.exit(0)
    else:
        print("\nContract verification tests FAILED.")
        sys.exit(1)
