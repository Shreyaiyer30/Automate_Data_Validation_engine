import pandas as pd
import numpy as np
from src.outlier_detector import detect_outliers_zscore

def test_detect_outliers():
    # Create data with an obvious outlier
    df = pd.DataFrame({'val': [10, 12, 11, 13, 100, 11, 12]})
    # With a low threshold to catch it on small sample
    report = detect_outliers_zscore(df, threshold=1.5)
    assert 'val' in report
    assert report['val'] == 1

def test_no_outliers():
    df = pd.DataFrame({'val': [10, 10, 10, 10, 10]})
    report = detect_outliers_zscore(df, threshold=3.0)
    assert report == {}
