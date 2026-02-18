"""
Configuration loader module for the Automated Data Validation Engine.
Provides validation and loading of YAML configuration files.
"""

import yaml
from pathlib import Path
from typing import Dict, Any


class ConfigError(Exception):
    """Custom exception for configuration errors."""
    pass


# Default configuration - fallback for missing keys
DEFAULT_CONFIG = {
    "missing_limit": 0.3,
    "outlier_method": "iqr",
    "outlier_action": "clip",
    "stop_on_schema_error": True,
    "stop_on_type_error": False,
    "duplicate_strategy": "drop",
    "header_normalization": True,
    "pipeline": {
        "stop_on_fail": True,
        "emit_warn_as_fail": False
    },
    "scoring": {
        "start_score": 100
    },
    "export": {
        "naming_contract": "{name}_cleaned_data.{ext}",
        "block_export_on_fail": True
    }
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries. Override values take precedence.
    
    Args:
        base: Base dictionary
        override: Dictionary with override values
        
    Returns:
        Merged dictionary
    """
    result = base.copy()
    
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    
    return result


def _validate_config(config: Dict[str, Any]) -> None:
    """
    Validate configuration values and types.
    
    Args:
        config: Configuration dictionary to validate
        
    Raises:
        ConfigError: If validation fails
    """
    # Validate missing_limit
    if not isinstance(config.get("missing_limit"), (int, float)):
        raise ConfigError("missing_limit must be a number")
    if not (0.0 <= config["missing_limit"] <= 1.0):
        raise ConfigError("missing_limit must be between 0.0 and 1.0 inclusive")
    
    # Validate outlier_method
    valid_outlier_methods = ["zscore", "iqr", "none"]
    if config.get("outlier_method") not in valid_outlier_methods:
        raise ConfigError(f"outlier_method must be one of {valid_outlier_methods}")
    
    # Validate outlier_action
    valid_outlier_actions = ["remove", "clip"]
    if config.get("outlier_action") not in valid_outlier_actions:
        raise ConfigError(f"outlier_action must be one of {valid_outlier_actions}")
    
    # Validate duplicate_strategy
    valid_duplicate_strategies = ["drop", "flag"]
    if config.get("duplicate_strategy") not in valid_duplicate_strategies:
        raise ConfigError(f"duplicate_strategy must be one of {valid_duplicate_strategies}")
    
    # Validate boolean fields
    bool_fields = [
        "stop_on_schema_error",
        "stop_on_type_error",
        "header_normalization"
    ]
    for field in bool_fields:
        if not isinstance(config.get(field), bool):
            raise ConfigError(f"{field} must be a boolean")
    
    # Validate pipeline section
    if "pipeline" not in config or not isinstance(config["pipeline"], dict):
        raise ConfigError("pipeline section must be a dictionary")
    
    if not isinstance(config["pipeline"].get("stop_on_fail"), bool):
        raise ConfigError("pipeline.stop_on_fail must be a boolean")
    
    if not isinstance(config["pipeline"].get("emit_warn_as_fail"), bool):
        raise ConfigError("pipeline.emit_warn_as_fail must be a boolean")
    
    # Validate scoring section
    if "scoring" not in config or not isinstance(config["scoring"], dict):
        raise ConfigError("scoring section must be a dictionary")
    
    start_score = config["scoring"].get("start_score")
    if not isinstance(start_score, (int, float)):
        raise ConfigError("scoring.start_score must be a number")
    
    if start_score != 100:
        raise ConfigError("scoring.start_score must be exactly 100 (score must always start at 100)")
    
    # Validate export section
    if "export" not in config or not isinstance(config["export"], dict):
        raise ConfigError("export section must be a dictionary")
    
    naming_contract = config["export"].get("naming_contract")
    if not isinstance(naming_contract, str):
        raise ConfigError("export.naming_contract must be a string")
    
    if "{name}" not in naming_contract or "{ext}" not in naming_contract:
        raise ConfigError('export.naming_contract must contain "{name}" and "{ext}"')
    
    if not isinstance(config["export"].get("block_export_on_fail"), bool):
        raise ConfigError("export.block_export_on_fail must be a boolean")


def load_config(path: str = "config/default.yaml") -> Dict[str, Any]:
    """
    Load and validate configuration from YAML file.
    
    Args:
        path: Path to YAML configuration file
        
    Returns:
        Validated configuration dictionary
        
    Raises:
        ConfigError: If configuration is invalid or file cannot be loaded
    """
    config_path = Path(path)
    
    # Check if file exists
    if not config_path.exists():
        raise ConfigError(f"Configuration file not found: {path}")
    
    # Load YAML file
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            loaded_config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Failed to parse YAML file: {e}")
    except Exception as e:
        raise ConfigError(f"Failed to read configuration file: {e}")
    
    # Handle empty file
    if loaded_config is None:
        loaded_config = {}
    
    # Deep merge with defaults
    config = _deep_merge(DEFAULT_CONFIG, loaded_config)
    
    # Validate configuration
    _validate_config(config)
    
    return config


def get_export_filename(original_name: str, config: Dict[str, Any]) -> str:
    """
    Apply naming_contract to produce the cleaned filename.
    
    Args:
        original_name: Original filename (e.g., "movies.csv")
        config: Configuration dictionary
        
    Returns:
        Cleaned filename following the naming contract
        
    Example:
        >>> get_export_filename("movies.csv", config)
        "movies_cleaned_data.csv"
        >>> get_export_filename("my.data.csv", config)
        "my.data_cleaned_data.csv"
    """
    naming_contract = config.get("export", {}).get("naming_contract", "{name}_cleaned_data.{ext}")
    
    # Split on the LAST dot only to handle names like "my.data.csv"
    if "." in original_name:
        last_dot_idx = original_name.rfind(".")
        name = original_name[:last_dot_idx]
        ext = original_name[last_dot_idx + 1:]
    else:
        name = original_name
        ext = "csv"  # Default extension
    
    # Apply naming contract
    cleaned_filename = naming_contract.format(name=name, ext=ext)
    
    return cleaned_filename
