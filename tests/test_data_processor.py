"""Tests for data processor module"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from data_processor import DataProcessor, DataProfile

@pytest.fixture
def sample_df():
    """Create sample DataFrame for testing"""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', None],
        'age': [25, 30, None, 35, 28],
        'salary': [50000, 60000, 75000, None, 55000],
        'department': ['HR', 'IT', 'HR', 'IT', 'Finance']
    })

@pytest.fixture
def processor():
    """Create DataProcessor instance"""
    return DataProcessor()

def test_profile_data(processor, sample_df):
    """Test data profiling"""
    profile = processor.profile_data(sample_df)
    
    assert profile.total_records == 5
    assert profile.total_columns == 5
    assert profile.missing_values == 3
    assert profile.duplicate_rows == 0
    assert len(profile.numerical_stats) == 2  # age, salary
    assert len(profile.categorical_stats) == 3  # name, department, and object columns

def test_detect_missing_patterns(processor, sample_df):
    """Test missing value detection"""
    missing = processor.detect_missing_patterns(sample_df)
    
    assert 'name' in missing
    assert 'age' in missing
    assert 'salary' in missing
    assert missing['name']['count'] == 1
    assert missing['age']['count'] == 1
    assert missing['salary']['count'] == 1

def test_detect_duplicates(processor, sample_df):
    """Test duplicate detection"""
    df_with_dupes = pd.concat([sample_df, sample_df.head(1)])
    result = processor.detect_duplicates(df_with_dupes)
    
    assert result['total_duplicates'] > 0

def test_load_data_csv(processor, tmp_path):
    """Test loading CSV file"""
    # Create temporary CSV
    csv_file = tmp_path / "test.csv"
    test_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    test_df.to_csv(csv_file, index=False)
    
    # Load and verify
    loaded_df = processor.load_data(str(csv_file))
    assert loaded_df.shape == (3, 2)
    assert list(loaded_df.columns) == ['a', 'b']

def test_load_data_unsupported_format(processor):
    """Test error handling for unsupported file format"""
    with pytest.raises(ValueError):
        processor.load_data("test.xyz")

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
