"""
Audit logging module for tracking pipeline operations.
"""

from .audit_logger import AuditLogger

# Version information
__version__ = "1.0.0"

# Export all public classes and functions
__all__ = [
    'AuditLogger'
]

print(f"Audit module v{__version__} loaded")