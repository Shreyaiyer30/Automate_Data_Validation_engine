import pandas as pd
from typing import Dict, Any, Tuple
import logging
from src.semantic_pipeline.detector import ColumnTypeDetector
from src.semantic_pipeline.validator import ValidationEngine
from src.semantic_pipeline.cleaner import CleaningOperations
from src.semantic_pipeline.scorer import QualityScorer
from src.semantic_pipeline.types import TypeMetadata

class DynamicCleaningPipeline:
    """
    Orchestrates the end-to-end data cleaning process.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        import yaml
        from pathlib import Path

        self.logger = logging.getLogger("SemanticPipeline")

        if config is None:
            # Try loading from default path
            config_path = Path("config/pipeline_config.yaml")
            if config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        config = yaml.safe_load(f)
                    self.logger.info(f"Loaded configuration from {config_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to load config: {e}")
                    config = {}
            else:
                config = {}
        
        self.config = config
        
        # Modules initialized with config
        self.detector = ColumnTypeDetector() 
        self.validator = ValidationEngine() 
        self.cleaner = CleaningOperations(self.config) # Pass config
        self.scorer = QualityScorer()

    def process(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        if df.empty:
            return df, {"error": "Empty DataFrame"}
            
        report = {}
        
        # 1. Detect Types
        self.logger.info("Detecting types...")
        # Returns Dict[str, TypeMetadata]
        type_map = self.detector.detect_types(df)
        report["detected_types"] = {k: {"type": v.detected_type.value, "confidence": v.confidence} for k, v in type_map.items()}

        # 2. Initial Validation & Score
        self.logger.info("Validating initial state...")
        initial_issues = self.validator.validate(df, type_map)
        initial_score = self.scorer.calculate_score(df, initial_issues)
        
        report["initial_quality"] = {
            "score": initial_score,
            "issue_count": len(initial_issues),
            "issues": [vars(i) for i in initial_issues] # Convert dataclass to dict
        }
        
        # 3. Clean
        self.logger.info("Cleaning data...")
        df_clean, change_logs = self.cleaner.clean(df, type_map)
        report["cleaning_logs"] = [vars(l) for l in change_logs]
        
        # 4. Final Validation & Score
        self.logger.info("Validating final state...")
        final_issues = self.validator.validate(df_clean, type_map)
        final_score = self.scorer.calculate_score(df_clean, final_issues)
        
        report["final_quality"] = {
            "score": final_score,
            "issue_count": len(final_issues),
            "issues": [vars(i) for i in final_issues]
        }
        
        return df_clean, report
