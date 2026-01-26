import pandas as pd
import pytest
from src.validation_rules import validate_dataframe

def test_validate_missing_columns():
    df = pd.DataFrame({'age': [25, 30]})
    rules = {'email': {'required': True}, 'age': {'required': True}}
    report = validate_dataframe(df, rules)
    assert "Missing required column: email" in report["schema_errors"]

def test_validate_missing_values():
    df = pd.DataFrame({'age': [25, None], 'email': ['a@b.com', 'c@d.com']})
    rules = {'age': {'required': True}}
    report = validate_dataframe(df, rules)
    assert report["missing_values"]["age"] == 1

def test_type_mismatch():
    df = pd.DataFrame({'age': ['twenty', 30]})
    rules = {'age': {'dtype': 'int64'}}
    report = validate_dataframe(df, rules)
    assert "age" in report["type_mismatches"]
