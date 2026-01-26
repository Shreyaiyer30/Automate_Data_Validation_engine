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

def generate_quality_report(validation_report, correction_summary, outliers_report):
    """
    Combine all metrics into a single structured quality report.
    """
    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_missing_before": sum(validation_report.get("missing_values", {}).values()),
            "total_duplicates_before": validation_report.get("duplicate_rows", 0),
            "total_outliers_detected": sum(outliers_report.values()),
            "total_corrections_made": sum(correction_summary.values())
        },
        "validation_details": validation_report,
        "correction_details": correction_summary,
        "outlier_details": outliers_report
    }
    return report

def save_report_json(report, output_path):
    """Save report to a JSON file."""
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=4)

def log_audit(logger, action, details):
    """Log an action to the audit log."""
    logger.info(f"ACTION: {action} | DETAILS: {json.dumps(details)}")