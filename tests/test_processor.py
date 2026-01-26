import pandas as pd
import os
from src.data_processor import DataProcessor

def test_data_processor_flow(tmp_path):
    # Create a dummy CSV
    csv_file = tmp_path / "test.csv"
    df = pd.DataFrame({
        'age': [25, 30, 200, None], # 200 is outlier, None is missing
        'email': ['test@test.com', 'test@test.com', 'x@y.com', 'z@w.com'], # Duplicates
        'income': [50000, 60000, 70000, 80000]
    })
    df.to_csv(csv_file, index=False)
    
    # Custom rules
    rules_file = tmp_path / "rules.yaml"
    rules_content = """
columns:
  age:
    dtype: "int64"
    min: 0
    max: 120
    required: true
  email:
    required: true
settings:
  outlier_zscore_threshold: 1.5
  remove_duplicates: true
"""
    with open(rules_file, 'w') as f:
        f.write(rules_content)
        
    processor = DataProcessor(config_path=str(rules_file))
    df_cleaned, report = processor.process_data(str(csv_file))
    
    assert len(df_cleaned) < len(df) # Duplicates should be gone
    assert df_cleaned['age'].isnull().sum() == 0 # Missing should be filled
    assert report['summary']['total_corrections_made'] > 0
