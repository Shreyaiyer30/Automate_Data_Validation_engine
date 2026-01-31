import pandas as pd
from typing import Dict, Any, Tuple
from src.engine.audit.audit_logger import AuditLogger
from src.engine.report.quality_report import QualityReport
from src.engine.lifecycle import LifecycleManager
from src.engine.config import load_config, asdict

class AtomicEngine:
    """
    Main entry point for the Automated Data Validation & Cleaning Engine.
    Orchestrates audit logging, stage execution, and comparative reporting.
    """
    def __init__(self, config: Any = None):
        if config is None:
            self.config_obj = load_config()
            self.config = asdict(self.config_obj)
        elif not isinstance(config, dict):
            self.config_obj = config
            self.config = asdict(config)
        else:
            self.config = config
            self.config_obj = None # Not used if config is already a dict
            
        self.logger = AuditLogger()
        self.reporter = QualityReport()
        self.lifecycle = LifecycleManager(self.logger)

    def run(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Execute the full data quality pipeline and return cleaned data + report."""
        initial_df = df.copy()
        
        # 1. Pipeline Execution
        try:
            final_df = self.lifecycle.run_pipeline(df, self.config)
        except Exception as e:
            self.logger.log_error("ENGINE", f"Pipeline crash: {str(e)}", critical=True)
            final_df = initial_df
            
        # 2. Audit Finalization
        self.logger.save_to_file()
        
        # 3. Comprehensive Comparative Report
        report = self.reporter.build_report(
            initial_df=initial_df, 
            final_df=final_df, 
            audit_entries=self.logger.entries,
            config=self.config
        )
        
        return final_df, report