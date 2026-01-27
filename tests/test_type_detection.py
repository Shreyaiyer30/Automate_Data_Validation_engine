import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent))

from src.data_type_detector import DataTypeDetector

def test_conversions():
    # Improved test data with clear class-wide patterns (>70% match)
    test_data = {
        'quoted_numbers': ['"475"', '"123"', '"999"', '"42"', '400', '500'],
        'currency': ['$1,234.56', '€1.234,56', '$999.00', '£50.00', '¥1000', '$0.01'],
        'percentages': ['25%', '50.5%', '100%', '0.5%', '10%', '99%'],
        'booleans': ['true', 'false', 'yes', 'no', 'true', 'false'],
        'times': ['14:30', '09:15', '14:30', '11:45', '12:00', '23:00'],
        'dates': ['10/13/2014', '05/22/2018', '12/05/2015', '08/30/2019', '03/14/2016', '01/01/2020']
    }

    df = pd.DataFrame(test_data)
    
    print("Testing DataTypeDetector (Refined)...")
    print("-" * 30)

    for col in df.columns:
        detection = DataTypeDetector.detect_column_type(df[col])
        print(f"Column: {col}")
        print(f"  Detected Type: {detection['type']}")
        print(f"  Confidence: {detection['confidence']:.2f}")
        
        # Convert
        converted = DataTypeDetector.convert_column(df[col])
        print(f"  New Dtype: {converted.dtype}")
        
        # assertions
        if col == 'quoted_numbers':
            assert pd.api.types.is_numeric_dtype(converted), f"Failed to convert {col}"
        elif col == 'currency':
            assert pd.api.types.is_numeric_dtype(converted), f"Failed to convert {col}"
        elif col == 'percentages':
            assert pd.api.types.is_numeric_dtype(converted), f"Failed to convert {col}"
            
        print("-" * 15)

    print("\nSUCCESS: All core conversions verified successfully!")

if __name__ == "__main__":
    test_conversions()
