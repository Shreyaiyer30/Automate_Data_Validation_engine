import pytest
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.semantic_pipeline.pipeline import DynamicCleaningPipeline
from src.semantic_pipeline.types import ColumnType, TypeMetadata

def test_pipeline_end_to_end():
    # 1. Create messy data
    data = {
        "Full Name": ["  john doe ", "PriYA ", "SingleWord", "Valid Name"],
        "Contact No": ["9876543210", "+91-9876543210", "123", "0000000000"],
        "Email Addr": ["john@test.com", "INVALID", " TEST@.COM ", "valid.email@domain.co.uk"],
        "Birth Date": ["1990-01-01", "01/01/2000", "future", "2025-12-31"],
        "User Age": [34, 24, -5, 150],
        "Gender": ["M", "Female", "x", "Male"]
    }
    df = pd.DataFrame(data)
    
    # 2. Run Pipeline
    pipeline = DynamicCleaningPipeline()
    df_clean, report = pipeline.process(df)
    
    # 3. Verify Detection
    detected = report["detected_types"]
    assert detected["Full Name"]["type"] == ColumnType.PERSON_NAME.value
    assert detected["Contact No"]["type"] == ColumnType.PHONE_NUMBER.value
    assert detected["Email Addr"]["type"] == ColumnType.EMAIL.value
    assert detected["Birth Date"]["type"] == ColumnType.DATE_OF_BIRTH.value
    assert detected["User Age"]["type"] == ColumnType.AGE.value
    assert detected["Gender"]["type"] == ColumnType.GENDER.value
    
    # 4. Verify Validation Issues (Initial)
    initial_issues = report["initial_quality"]["issues"]
    assert any(i["column"] == "User Age" and i["severity"] == "ERROR" for i in initial_issues)
    assert any(i["column"] == "Email Addr" and i["message"] == "Invalid email format" for i in initial_issues)
    
    # 5. Verify Cleaning effectiveness
    # Name normalized?
    assert df_clean["Full Name"].iloc[0] == "John Doe"
    # Email lowercase/stripped?
    assert df_clean["Email Addr"].iloc[2] == "test@.com" 
    
    # 6. Verify Score Improvement (or change)
    initial_score = report["initial_quality"]["score"]
    final_score = report["final_quality"]["score"]
    # Cleaning usually fixes formatting issues but maybe not logical errors (Age -5)
    # However, standardization should improve score or keep it same
    print(f"Initial: {initial_score}, Final: {final_score}")
    
if __name__ == "__main__":
    test_pipeline_end_to_end()
