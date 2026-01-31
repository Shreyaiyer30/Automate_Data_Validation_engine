"""
Core engine module for the Automated Data Validation Engine.
Contains the main pipeline orchestration components.
"""

from .atomic_engine import AtomicEngine
from .lifecycle import LifecycleManager
from .config import load_config, save_config, validate_config, EngineConfig
from .state import PipelineState
from .pipeline_stage import PipelineStage

# Version information
__version__ = "1.0.0"

# Export all public classes and functions
__all__ = [
    'AtomicEngine',
    'LifecycleManager',
    'load_config',
    'save_config',
    'validate_config',
    'EngineConfig',
    'PipelineState',
    'PipelineStage'
]

print(f"Engine module v{__version__} loaded")