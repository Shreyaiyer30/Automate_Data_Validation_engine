import pandas as pd
from typing import Dict, Any, Tuple
from src.rule_generator import DynamicRuleGenerator
from src.dynamic_cleaner import DynamicDataCleaner
from src.engine.atomic_engine import AtomicEngine
import logging

class DataProcessor:
    """
    High-level facade for data processing.
    Integrates the AtomicEngine with Dynamic Rule Generation and Cleaning.
    """
    
    def __init__(self, settings: Dict = None):
        self.engine = AtomicEngine() # Fallback to standard engine if needed
        self.rule_generator = DynamicRuleGenerator(settings)
        self.cleaner = DynamicDataCleaner(self.rule_generator, logger=self.engine.logger)
        self.logger = logging.getLogger(__name__)

    def analyze_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a deep analysis and suggested rules for the dataset."""
        if df is None:
            return {"suggested_rules": {}, "column_analysis": {}, "quality_score": 0}
            
        return self.rule_generator.generate_cleaning_rules(df)

    def process_data(self, df: pd.DataFrame, rules: Dict[str, Any] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Clean data using either the standard AtomicEngine or dynamic rules.
        If rules are provided, it uses the DynamicDataCleaner.
        """
        if df is None:
            return None, {}

        initial_df = df.copy()
        
        if rules:
            # Use Dynamic Cleaner
            self.logger.info("Using Dynamic Rules for cleaning")
            df_cleaned = self.cleaner.apply_rules(df, rules)
            
            # Generate a quality report (similar to AtomicEngine's report)
            report = self.engine.reporter.build_report(
                initial_df=initial_df,
                final_df=df_cleaned,
                audit_entries=self.engine.logger.get_audit_trail(),
                config={"rules_applied": rules}
            )
        else:
            # Fallback to AtomicEngine
            self.logger.info("Using Standard AtomicEngine for cleaning")
            df_cleaned, report = self.engine.run(df)
            
        return df_cleaned, report

    def _suggest_rules(self, df: pd.DataFrame) -> Dict:
        """Helper for compatibility with older code if any."""
        return self.rule_generator.generate_cleaning_rules(df)["suggested_rules"]
