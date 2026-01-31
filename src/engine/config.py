# from typing import Dict, Any

# def load_default_config() -> Dict[str, Any]:
#     """Provide a baseline configuration for the pipeline."""
#     return {
#         "engine": {
#             "enable_audit": True,
#             "save_reports": True
#         },
#         "stages": ["TYPE_DETECTION", "MISSING_VALUES", "DUPLICATES", "OUTLIERS"],
#         "thresholds": {
#              "max_missing_row_percentage": 50.0,
#              "outlier_method": "iqr"
#         }
#     }
"""
Configuration Management
Loads and validates configuration from YAML files.
"""

import yaml
import os
from typing import Dict, Any, Optional, List
from pathlib import Path
import logging
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class SchemaConfig:
    """Schema configuration."""
    required_columns: List[str] = None
    data_types: Dict[str, str] = None
    
    def __post_init__(self):
        if self.required_columns is None:
            self.required_columns = []
        if self.data_types is None:
            self.data_types = {}

@dataclass
class MissingValuesConfig:
    """Missing values handling configuration."""
    strategies: Dict[str, str] = None
    thresholds: Dict[str, float] = None
    mandatory_columns: List[str] = None
    
    def __post_init__(self):
        if self.strategies is None:
            self.strategies = {
                'numeric': 'median',
                'categorical': 'mode',
                'datetime': 'forward_fill'
            }
        if self.thresholds is None:
            self.thresholds = {
                'column_threshold': 50.0,
                'row_threshold': 50.0
            }
        if self.mandatory_columns is None:
            self.mandatory_columns = []

@dataclass
class OutlierConfig:
    """Outlier detection and handling configuration."""
    method: str = 'zscore'
    zscore_threshold: float = 3.0
    iqr_multiplier: float = 1.5
    strategy: str = 'cap'  # 'cap', 'remove', 'mark'
    
@dataclass
class DeduplicationConfig:
    """Deduplication configuration."""
    remove_full_row_duplicates: bool = True
    primary_key: List[str] = None
    keep: str = 'first'  # 'first', 'last', False
    
    def __post_init__(self):
        if self.primary_key is None:
            self.primary_key = []

@dataclass
class TextCleaningConfig:
    """Text cleaning configuration."""
    strip_whitespace: bool = True
    normalize_case: Optional[str] = None  # 'lower', 'upper', None
    remove_special_characters: bool = False
    allowed_values: Dict[str, List[str]] = None
    
    def __post_init__(self):
        if self.allowed_values is None:
            self.allowed_values = {}

@dataclass
class ValidationConfig:
    """Validation configuration."""
    max_missing_row_percentage: float = 50.0
    max_duplicate_percentage: float = 10.0
    
@dataclass
class EngineConfig:
    """Complete engine configuration."""
    # Core configuration
    schema: SchemaConfig
    missing_values: MissingValuesConfig
    outliers: OutlierConfig
    duplicates: DeduplicationConfig
    text_cleaning: TextCleaningConfig
    validation: ValidationConfig
    
    # Pipeline stages configuration
    stages: Dict[str, Dict[str, Any]] = None
    
    # Performance settings
    max_file_size_mb: float = 100.0
    enable_parallel_processing: bool = False
    max_workers: int = 4
    
    # Output settings
    output_format: str = 'csv'  # 'csv', 'parquet', 'excel'
    compress_output: bool = False
    
    def __post_init__(self):
        if self.stages is None:
            self.stages = {}

def load_config(config_path: Optional[str] = None) -> EngineConfig:
    """
    Load configuration from YAML file or use defaults.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        EngineConfig object
    """
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config_dict = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            config_dict = {}
    else:
        if config_path:
            logger.warning(f"Config file {config_path} not found, using defaults")
        config_dict = {}
    
    # Merge with defaults
    config_dict = _merge_with_defaults(config_dict)
    
    # Create configuration objects
    schema_config = SchemaConfig(
        required_columns=config_dict.get('schema', {}).get('required_columns', []),
        data_types=config_dict.get('schema', {}).get('data_types', {})
    )
    
    missing_config = MissingValuesConfig(
        strategies=config_dict.get('missing_values', {}).get('strategies', {}),
        thresholds=config_dict.get('missing_values', {}).get('thresholds', {}),
        mandatory_columns=config_dict.get('missing_values', {}).get('mandatory_columns', [])
    )
    
    outlier_config = OutlierConfig(
        method=config_dict.get('outliers', {}).get('method', 'zscore'),
        zscore_threshold=config_dict.get('outliers', {}).get('zscore_threshold', 3.0),
        iqr_multiplier=config_dict.get('outliers', {}).get('iqr_multiplier', 1.5),
        strategy=config_dict.get('outliers', {}).get('strategy', 'cap')
    )
    
    dedup_config = DeduplicationConfig(
        remove_full_row_duplicates=config_dict.get('duplicates', {}).get('remove_full_row_duplicates', True),
        primary_key=config_dict.get('duplicates', {}).get('primary_key', []),
        keep=config_dict.get('duplicates', {}).get('keep', 'first')
    )
    
    text_config = TextCleaningConfig(
        strip_whitespace=config_dict.get('text_cleaning', {}).get('strip_whitespace', True),
        normalize_case=config_dict.get('text_cleaning', {}).get('normalize_case'),
        remove_special_characters=config_dict.get('text_cleaning', {}).get('remove_special_characters', False),
        allowed_values=config_dict.get('text_cleaning', {}).get('allowed_values', {})
    )
    
    validation_config = ValidationConfig(
        max_missing_row_percentage=config_dict.get('validation', {}).get('max_missing_row_percentage', 50.0),
        max_duplicate_percentage=config_dict.get('validation', {}).get('max_duplicate_percentage', 10.0)
    )
    
    # Create main config
    engine_config = EngineConfig(
        schema=schema_config,
        missing_values=missing_config,
        outliers=outlier_config,
        duplicates=dedup_config,
        text_cleaning=text_config,
        validation=validation_config,
        stages=config_dict.get('stages', {}),
        max_file_size_mb=config_dict.get('max_file_size_mb', 100.0),
        enable_parallel_processing=config_dict.get('enable_parallel_processing', False),
        max_workers=config_dict.get('max_workers', 4),
        output_format=config_dict.get('output_format', 'csv'),
        compress_output=config_dict.get('compress_output', False)
    )
    
    return engine_config

def save_config(config: EngineConfig, filepath: str):
    """
    Save configuration to YAML file.
    
    Args:
        config: EngineConfig object
        filepath: Path to save YAML file
    """
    config_dict = asdict(config)
    
    # Ensure directory exists
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w') as f:
        yaml.dump(config_dict, f, default_flow_style=False, indent=2)
    
    logger.info(f"Configuration saved to {filepath}")

def validate_config(config: EngineConfig) -> List[str]:
    """
    Validate configuration for potential issues.
    
    Args:
        config: EngineConfig object
        
    Returns:
        List of validation issues
    """
    issues = []
    
    # Validate schema
    if config.schema.required_columns:
        for col in config.schema.required_columns:
            if not isinstance(col, str):
                issues.append(f"Required column '{col}' must be a string")
    
    # Validate data types
    valid_dtypes = ['int', 'float', 'string', 'datetime', 'bool', 'category']
    for col, dtype in config.schema.data_types.items():
        if dtype.lower() not in valid_dtypes:
            issues.append(f"Invalid data type '{dtype}' for column '{col}'")
    
    # Validate missing value strategies
    valid_strategies = ['mean', 'median', 'mode', 'forward_fill', 'backward_fill', 
                       'drop_row', 'drop_column', 'constant']
    for dtype, strategy in config.missing_values.strategies.items():
        if strategy not in valid_strategies:
            issues.append(f"Invalid missing value strategy '{strategy}' for dtype '{dtype}'")
    
    # Validate thresholds
    if config.missing_values.thresholds:
        col_threshold = config.missing_values.thresholds.get('column_threshold', 50.0)
        row_threshold = config.missing_values.thresholds.get('row_threshold', 50.0)
        
        if not 0 <= col_threshold <= 100:
            issues.append(f"Column threshold must be between 0 and 100, got {col_threshold}")
        if not 0 <= row_threshold <= 100:
            issues.append(f"Row threshold must be between 0 and 100, got {row_threshold}")
    
    # Validate outlier configuration
    if config.outliers.method not in ['zscore', 'iqr']:
        issues.append(f"Invalid outlier method: {config.outliers.method}")
    if config.outliers.zscore_threshold <= 0:
        issues.append(f"Z-score threshold must be positive, got {config.outliers.zscore_threshold}")
    if config.outliers.iqr_multiplier <= 0:
        issues.append(f"IQR multiplier must be positive, got {config.outliers.iqr_multiplier}")
    if config.outliers.strategy not in ['cap', 'remove', 'mark']:
        issues.append(f"Invalid outlier strategy: {config.outliers.strategy}")
    
    # Validate deduplication
    if config.duplicates.keep not in ['first', 'last', False]:
        issues.append(f"Invalid deduplication keep value: {config.duplicates.keep}")
    
    # Validate output format
    if config.output_format not in ['csv', 'parquet', 'excel']:
        issues.append(f"Invalid output format: {config.output_format}")
    
    return issues

def _merge_with_defaults(user_config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge user configuration with defaults."""
    default_config = {
        'schema': {
            'required_columns': [],
            'data_types': {}
        },
        'missing_values': {
            'strategies': {
                'numeric': 'median',
                'categorical': 'mode',
                'datetime': 'forward_fill'
            },
            'thresholds': {
                'column_threshold': 50.0,
                'row_threshold': 50.0
            },
            'mandatory_columns': []
        },
        'outliers': {
            'method': 'zscore',
            'zscore_threshold': 3.0,
            'iqr_multiplier': 1.5,
            'strategy': 'cap'
        },
        'duplicates': {
            'remove_full_row_duplicates': True,
            'primary_key': [],
            'keep': 'first'
        },
        'text_cleaning': {
            'strip_whitespace': True,
            'normalize_case': None,
            'remove_special_characters': False,
            'allowed_values': {}
        },
        'validation': {
            'max_missing_row_percentage': 50.0,
            'max_duplicate_percentage': 10.0
        },
        'stages': {},
        'max_file_size_mb': 100.0,
        'enable_parallel_processing': False,
        'max_workers': 4,
        'output_format': 'csv',
        'compress_output': False
    }
    
    def deep_merge(default: Dict, user: Dict) -> Dict:
        """Recursively merge two dictionaries."""
        merged = default.copy()
        
        for key, value in user.items():
            if key in merged:
                if isinstance(merged[key], dict) and isinstance(value, dict):
                    merged[key] = deep_merge(merged[key], value)
                else:
                    merged[key] = value
            else:
                merged[key] = value
        
        return merged
    
    return deep_merge(default_config, user_config)

def get_config_summary(config: EngineConfig) -> Dict[str, Any]:
    """Get a human-readable summary of the configuration."""
    return {
        'schema': {
            'required_columns_count': len(config.schema.required_columns),
            'data_types_count': len(config.schema.data_types)
        },
        'missing_values': {
            'strategies': config.missing_values.strategies,
            'thresholds': config.missing_values.thresholds,
            'mandatory_columns_count': len(config.missing_values.mandatory_columns)
        },
        'outliers': asdict(config.outliers),
        'duplicates': asdict(config.duplicates),
        'text_cleaning': {
            'strip_whitespace': config.text_cleaning.strip_whitespace,
            'normalize_case': config.text_cleaning.normalize_case,
            'remove_special_characters': config.text_cleaning.remove_special_characters,
            'allowed_values_count': len(config.text_cleaning.allowed_values)
        },
        'performance': {
            'max_file_size_mb': config.max_file_size_mb,
            'enable_parallel_processing': config.enable_parallel_processing,
            'max_workers': config.max_workers
        },
        'output': {
            'format': config.output_format,
            'compress': config.compress_output
        }
    }