"""
Pipeline Stage Enumeration - Optional module.
If this causes circular imports, we can work without it.
"""

try:
    from enum import Enum, auto
    
    class PipelineStage(Enum):
        """Enumeration of pipeline stages."""
        INITIALIZATION = auto()
        DETECT_TYPES = auto()
        NORMALIZE_STRUCTURE = auto()
        ENFORCE_SCHEMA = auto()
        CLEAN_TEXT = auto()
        DEDUPLICATE = auto()
        HANDLE_MISSING = auto()
        HANDLE_OUTLIERS = auto()
        VERIFICATION = auto()
        
        @classmethod
        def from_string(cls, stage_str: str):
            """Convert string to PipelineStage enum."""
            stage_map = {
                'initialization': cls.INITIALIZATION,
                'detect_types': cls.DETECT_TYPES,
                'normalize_structure': cls.NORMALIZE_STRUCTURE,
                'enforce_schema': cls.ENFORCE_SCHEMA,
                'clean_text': cls.CLEAN_TEXT,
                'deduplicate': cls.DEDUPLICATE,
                'handle_missing': cls.HANDLE_MISSING,
                'handle_outliers': cls.HANDLE_OUTLIERS,
                'verification': cls.VERIFICATION
            }
            return stage_map.get(stage_str.lower())
        
        def __str__(self):
            return self.name.lower()
    
except ImportError:
    # If enum module is not available, create a simple version
    class PipelineStage:
        """Simple string-based pipeline stage."""
        INITIALIZATION = "initialization"
        DETECT_TYPES = "detect_types"
        NORMALIZE_STRUCTURE = "normalize_structure"
        ENFORCE_SCHEMA = "enforce_schema"
        CLEAN_TEXT = "clean_text"
        DEDUPLICATE = "deduplicate"
        HANDLE_MISSING = "handle_missing"
        HANDLE_OUTLIERS = "handle_outliers"
        VERIFICATION = "verification"
        
        @classmethod
        def from_string(cls, stage_str: str):
            return stage_str
        
        def __str__(self):
            return self