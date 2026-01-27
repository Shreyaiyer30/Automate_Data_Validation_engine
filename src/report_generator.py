import json
import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logger(log_file=None):
    """Set up audit logging."""
    if log_file is None:
        log_file = Path(__file__).parent.parent / "logs" / "audit.log"
    
    # Ensure logs directory exists
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def count_total_corrections(correction_summary):
    """
    Recursively count all numeric corrections from the correction summary.
    Handles nested dicts, lists, and mixed types.
    """
    total = 0
    
    if not isinstance(correction_summary, dict):
        return 0
    
    for key, value in correction_summary.items():
        # Skip boolean values
        if isinstance(value, bool):
            continue
        # Add numeric values directly
        elif isinstance(value, (int, float)):
            total += value
        # Recursively handle nested dictionaries
        elif isinstance(value, dict):
            total += count_total_corrections(value)
        # Handle lists (sum numeric items, ignore others)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, (int, float)) and not isinstance(item, bool):
                    total += item
                elif isinstance(item, dict):
                    total += count_total_corrections(item)
    
    return int(total)

def generate_quality_report(validation_report, correction_summary, outliers_report):
    """
    Combine all metrics into a single structured quality report.
    """
    # Safely get values with defaults
    missing_values = validation_report.get("missing_values", {})
    total_missing = sum(v for v in missing_values.values() if isinstance(v, (int, float))) if isinstance(missing_values, dict) else 0
    
    duplicate_rows = validation_report.get("duplicate_rows", 0)
    if not isinstance(duplicate_rows, (int, float)):
        duplicate_rows = 0
    
    total_outliers = sum(v for v in outliers_report.values() if isinstance(v, (int, float))) if isinstance(outliers_report, dict) else 0
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_missing_before": int(total_missing),
            "total_duplicates_before": int(duplicate_rows),
            "total_outliers_detected": int(total_outliers),
            "total_corrections_made": int(count_total_corrections(correction_summary))
        },
        "validation_details": {
            "missing_values": {k: int(v) for k, v in missing_values.items()} if isinstance(missing_values, dict) else {},
            "duplicate_rows": int(duplicate_rows),
            "schema_errors": validation_report.get("schema_errors", []),
            "type_mismatches": validation_report.get("type_mismatches", {})
        },
        "correction_details": correction_summary,
        "outlier_details": {k: int(v) for k, v in outliers_report.items()} if isinstance(outliers_report, dict) else {}
    }
    return report

def save_report_json(report, output_path):
    """Save report to a JSON file."""
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=4)

def log_audit(logger, action, details):
    """Log an action to the audit log."""
    # Ensure details is JSON serializable
    serializable_details = {}
    if isinstance(details, dict):
        for k, v in details.items():
            if isinstance(v, (str, int, float, bool, list, dict)):
                serializable_details[k] = v
            else:
                serializable_details[k] = str(v)
    else:
        serializable_details = str(details)
        
    logger.info(f"ACTION: {action} | DETAILS: {json.dumps(serializable_details)}")