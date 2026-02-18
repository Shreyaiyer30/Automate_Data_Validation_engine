import pandas as pd
from datetime import datetime

class QualityReport:
    """
    Legacy QualityReport component for generating data quality summaries.
    """
    def __init__(self, raw_df: pd.DataFrame, clean_df: pd.DataFrame):
        self.raw_df = raw_df
        self.clean_df = clean_df
        self.generated_at = datetime.now()

    def generate_summary(self) -> dict:
        return {
            "raw_rows": len(self.raw_df),
            "clean_rows": len(self.clean_df),
            "timestamp": self.generated_at.isoformat()
        }
