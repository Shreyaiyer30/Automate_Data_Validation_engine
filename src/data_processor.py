import pandas as pd
import io
from .config import load_config, get_column_rules, get_settings
from .validation_rules import validate_dataframe
from .outlier_detector import detect_outliers_zscore
from .data_corrector import correct_data
from .report_generator import setup_logger, generate_quality_report, log_audit

class DataProcessor:
    def __init__(self, config_path=None):
        self.config = load_config(config_path)
        self.rules = get_column_rules(self.config)
        self.settings = get_settings(self.config)
        self.logger = setup_logger()

    def process_data(self, input_data):
        """
        Orchestrate the validation and cleaning process.
        input_data can be a file path or a file-like object (e.g. from Streamlit).
        """
        # 1. Load Data
        if isinstance(input_data, str):
            df = pd.read_csv(input_data)
        elif hasattr(input_data, 'read'):
            # Handle file-like objects (e.g. Streamlit UploadedFile)
            df = pd.read_csv(input_data)
        else:
            raise ValueError("Input data must be a file path or a file-like object.")

        log_audit(self.logger, "DATA_LOADED", {"rows": len(df), "cols": len(df.columns)})

        # 2. Validate
        validation_report = validate_dataframe(df, self.rules)
        
        # 3. Detect Outliers
        threshold = self.settings.get('outlier_zscore_threshold', 3.0)
        outliers_report = detect_outliers_zscore(df, threshold)

        # 4. Correct Data
        df_cleaned, correction_summary = correct_data(df, self.rules, self.settings)
        
        log_audit(self.logger, "DATA_CLEANED", correction_summary)

        # 5. Generate Report
        quality_report = generate_quality_report(
            validation_report, 
            correction_summary, 
            outliers_report
        )

        return df_cleaned, quality_report