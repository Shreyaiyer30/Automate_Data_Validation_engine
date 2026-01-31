# import sys
# import subprocess
# from pathlib import Path

# def run_ui():
#     """Start the Streamlit UI."""
#     print("🚀 Starting DataClean Engine v2 UI...")
#     try:
#         subprocess.run(["streamlit", "run", "ui/app.py"], check=True)
#     except KeyboardInterrupt:
#         print("\n👋 UI Stopped.")

# def run_cli():
#     """Start CLI mode (Not implemented in minimal version)."""
#     print("🛠️ CLI mode coming soon.")

# if __name__ == "__main__":
#     if len(sys.argv) > 1 and sys.argv[1] == "--cli":
#         run_cli()
#     else:
#         run_ui()
#!/usr/bin/env python3
"""
Main entry point for the Automated Data Validation Engine.
Supports both CLI and UI modes.
"""

import sys
import argparse
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('data_validation.log')
        ]
    )

def run_cli(args):
    """Run in CLI mode."""
    from src.cli.main import main as cli_main
    cli_main(args)

def run_ui(args):
    """Run in UI mode."""
    try:
        import streamlit
    except ImportError:
        print("Error: Streamlit is not installed. Install with: pip install streamlit")
        sys.exit(1)
    
    # Run Streamlit app
    import subprocess
    import os
    
    ui_path = Path(__file__).parent / 'ui' / 'app.py'
    
    cmd = ['streamlit', 'run', str(ui_path)]
    if args.port:
        cmd.extend(['--server.port', str(args.port)])
    
    subprocess.run(cmd)

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Automated Data Validation Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run in CLI mode with auto-clean
  python run.py --input data/raw/sample.csv --output data/cleaned/ --auto
  
  # Run in UI mode
  python run.py --ui
  
  # Run with custom configuration
  python run.py --input data.csv --config config/custom.yaml --profile
  
  # Batch process multiple files
  python run.py --input "data/raw/*.csv" --batch --output reports/
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--ui', action='store_true', 
                          help='Launch web UI (Streamlit)')
    mode_group.add_argument('--cli', action='store_true', 
                          help='Run in CLI mode (default)')
    
    # Input/output
    parser.add_argument('--input', '-i', type=str,
                       help='Input file or directory')
    parser.add_argument('--output', '-o', type=str,
                       help='Output directory')
    
    # Processing options
    parser.add_argument('--config', '-c', type=str,
                       help='Configuration file')
    parser.add_argument('--auto', action='store_true',
                       help='Automatically clean data')
    parser.add_argument('--profile', action='store_true',
                       help='Generate data profile report')
    parser.add_argument('--batch', action='store_true',
                       help='Process multiple files')
    
    # UI options
    parser.add_argument('--port', type=int, default=8501,
                       help='Port for UI (default: 8501)')
    
    # Verbosity
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Determine mode
    if args.ui:
        run_ui(args)
    else:
        run_cli(args)

if __name__ == '__main__':
    main()