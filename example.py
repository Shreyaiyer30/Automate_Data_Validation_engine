"""
Example script showing complete pipeline execution with reporting.
"""

import pandas as pd
from pathlib import Path
import logging
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_complete_pipeline_example():
    """Example of complete pipeline execution with reporting."""
    
    # 1. Load sample data
    from src.utils.file_loader import smart_load
    from src.utils.helpers import generate_sample_data
    
    logger.info("Generating sample data...")
    df = generate_sample_data(rows=1000, include_problems=True)
    
    # Save sample data
    sample_path = Path("data/raw/unclean_data.csv")
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(sample_path, index=False)
    logger.info(f"Sample data saved to {sample_path}")
    
    # 2. Initialize engine
    from src.engine.atomic_engine import AtomicEngine
    
    logger.info("Initializing Atomic Engine...")
    engine = AtomicEngine(
        config_path=None,  # Use defaults
        enable_audit=True
    )
    
    # 3. Run pipeline
    logger.info("Running pipeline...")
    cleaned_df, report = engine.run(df)
    
    # 4. Generate reports
    logger.info("Generating reports...")
    from src.report.report_builder import ReportBuilder
    from src.report.quality_report import QualityReport
    from src.report.export import DataExporter
    
    # Create report builder
    builder = ReportBuilder()
    comprehensive_report = builder.build_report(
        df, cleaned_df, report['step_logs'], engine.config
    )
    
    # Generate HTML report
    reporter = QualityReport(comprehensive_report)
    html_report = reporter.generate_html_report("data/reports/quality_report.html")
    
    # Generate JSON report
    json_report = reporter.generate_json_report("data/reports/quality_report.json")
    
    # 5. Export everything
    logger.info("Exporting results...")
    exporter = DataExporter(output_dir="data/cleaned")
    
    # Export data
    data_files = exporter.export_data(
        df=cleaned_df,
        format="csv",
        filename="cleaned_data"
    )
    
    # Export reports
    report_files = exporter.export_report(
        report=comprehensive_report,
        formats=['html', 'json', 'md']
    )
    
    # Export audit log
    audit_log = engine.get_audit_summary()
    audit_file = exporter.export_audit_log(audit_log)
    
    # Export config snapshot
    config_file = exporter.export_config_snapshot(engine.config)
    
    # 6. Print summary
    logger.info("=" * 80)
    logger.info("PIPELINE EXECUTION COMPLETE")
    logger.info("=" * 80)
    
    logger.info(f"Initial shape: {df.shape}")
    logger.info(f"Final shape: {cleaned_df.shape}")
    logger.info(f"Quality score: {comprehensive_report['metrics']['overall_score']:.2f}")
    logger.info(f"Status: {comprehensive_report['status']}")
    
    logger.info("\n📁 Exported Files:")
    logger.info(f"  • Cleaned data: {data_files}")
    logger.info(f"  • HTML report: {report_files.get('html', 'N/A')}")
    logger.info(f"  • JSON report: {report_files.get('json', 'N/A')}")
    logger.info(f"  • Audit log: {audit_file}")
    logger.info(f"  • Config snapshot: {config_file}")
    
    # 7. Print recommendations
    logger.info("\n💡 Recommendations:")
    for rec in comprehensive_report['recommendations']:
        severity_icon = {
            'high': '🔴',
            'medium': '🟡',
            'low': '🟢',
            'none': '✅'
        }.get(rec.get('severity', ''), '')
        
        logger.info(f"  {severity_icon} {rec.get('message', '')}")
    
    return cleaned_df, comprehensive_report

if __name__ == "__main__":
    # Ensure directories exist
    Path("data/cleaned").mkdir(parents=True, exist_ok=True)
    Path("data/reports").mkdir(parents=True, exist_ok=True)
    Path("data/audit_logs").mkdir(parents=True, exist_ok=True)
    Path("data/configs").mkdir(parents=True, exist_ok=True)
    
    # Run the example
    try:
        cleaned_df, report = run_complete_pipeline_example()
        
        print("\n" + "="*60)
        print("✅ Pipeline execution completed successfully!")
        print(f"📊 Final quality score: {report['metrics']['overall_score']:.2f}/100")
        print("="*60)
        print("\n📋 Quick Summary:")
        print(f"  Rows: {report['initial_stats']['rows']} → {report['final_stats']['rows']}")
        print(f"  Missing values: {report['initial_stats']['missing_total']} → {report['final_stats']['missing_total']}")
        print(f"  Duplicates: {report['initial_stats']['duplicates']} → {report['final_stats']['duplicates']}")
        print(f"  Total fixes: {report['summary']['total_fixes_applied']}")
        print("\n📁 Output saved to: data/cleaned/")
        
    except ImportError as e:
        print(f"\n❌ Import error: {e}")
        print("Please ensure all dependencies are installed and the project structure is correct.")
        print("Run: pip install -r requirements.txt")
    except Exception as e:
        print(f"\n❌ Error running pipeline: {e}")
        import traceback
        traceback.print_exc()