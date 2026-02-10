from enum import Enum, auto
from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict

class ColumnType(Enum):
    PERSON_NAME = "PERSON_NAME"
    PHONE_NUMBER = "PHONE_NUMBER"
    EMAIL = "EMAIL"
    DATE_OF_BIRTH = "DATE_OF_BIRTH"
    AGE = "AGE"
    LOCATION = "LOCATION"
    GENDER = "GENDER"
    NUMERIC = "NUMERIC"
    CATEGORICAL = "CATEGORICAL"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"
    UNKNOWN = "UNKNOWN"

@dataclass
class TypeMetadata:
    detected_type: ColumnType
    confidence: float
    reasons: List[str] = field(default_factory=list)
    secondary_matches: List[Dict[str, Any]] = field(default_factory=list)

@dataclass
class ValidationIssue:
    column: str
    detected_type: ColumnType
    severity: str  # INFO, WARNING, ERROR
    rule_id: str
    message: str
    rows_affected: int
    examples: List[Any] = field(default_factory=list)
    suggested_fix: Optional[str] = None

@dataclass
class ChangeLog:
    column: str
    operation: str
    rows_changed: int
    examples: List[Dict[str, Any]] = field(default_factory=list)
