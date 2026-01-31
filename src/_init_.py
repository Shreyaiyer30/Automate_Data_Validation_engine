"""
Automated Data Validation Engine
A comprehensive pipeline for data validation, cleaning, and quality reporting.
"""

# Version information
__version__ = "1.0.0"
__author__ = "Data Validation Team"
__email__ = ""
__description__ = "Automated Data Validation and Cleaning Pipeline"

# Export main components
__all__ = [
    'engine',
    'utils',
    'report',
    'audit'
]

# Package initialization
def get_version():
    """Get the package version."""
    return __version__

def get_info():
    """Get package information."""
    return {
        "name": "automated-data-validation-engine",
        "version": __version__,
        "author": __author__,
        "description": __description__
    }

# Print welcome message when imported
import sys
if not sys.argv[0].endswith('setup.py'):  # Don't print during installation
    print(f"Automated Data Validation Engine v{__version__}")
    print("Data Engine modules loaded: engine, utils, report, audit")