import pandas as pd
import numpy as np
import pytest
from src.engine.stages.missing_values import MissingValuesStage
from src.engine.stages.outliers import OutliersStage
from src.engine.stages.duplicates import DuplicatesStage
from src.engine.audit.audit_logger import AuditLogger
from src.engine.atomic_engine import AtomicEngine

def test_missing_values_imputation():
    df = pd.DataFrame({'A': [1, 2, None, 4], 'B': ['x', 'y', 'x', None]})
    stage = MissingValuesStage(AuditLogger())
    result = stage.execute(df, {"thresholds": {"max_missing_row_percentage": 100}})
    assert result['A'].isnull().sum() == 0
    assert result['B'].isnull().sum() == 0

def test_outlier_clipping():
    df = pd.DataFrame({'A': [10, 12, 11, 13, 1000]})
    stage = OutliersStage(AuditLogger())
    result = stage.execute(df, {"thresholds": {"outlier_method": "iqr"}})
    assert result['A'].max() < 1000

def test_duplicate_removal():
    df = pd.DataFrame({'id': [1, 1, 2], 'val': ['a', 'a', 'b']})
    stage = DuplicatesStage(AuditLogger())
    result = stage.execute(df, {})
    assert len(result) == 2

def test_engine_end_to_end():
    df = pd.DataFrame({'id': [1, 1, 2], 'age': [30, 30, None]})
    engine = AtomicEngine()
    df_cleaned, report = engine.run(df)
    assert report['quality_score'] > 0
    assert len(df_cleaned) == 2
