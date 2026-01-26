import yaml
import os
from pathlib import Path

def load_config(config_path=None):
    """
    Load validation rules from a YAML file.
    """
    if config_path is None:
        # Default path
        config_path = Path(__file__).parent.parent / "config" / "rules.yaml"
    
    if not os.path.exists(config_path):
        return {}
        
    with open(config_path, 'r') as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(f"Error loading configuration: {e}")
            return {}

def get_column_rules(config):
    """Extract column rules from config."""
    return config.get('columns', {})

def get_settings(config):
    """Extract general settings from config."""
    return config.get('settings', {})
