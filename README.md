# Automated Data Validation & Cleaning Engine

A robust, modular Python tool to validate and clean raw CSV datasets. Supports both a Streamlit UI for interactive analysis and a CLI for automated pipelines.

## Project Structure
- `src/`: Core logic (Validation, Outlier Detection, Correction, Reporting)
- `ui/`: Streamlit dashboard
- `cli/`: Command-line interface
- `config/`: Validation rules defined in YAML
- `tests/`: Pytest suite

## Features
- **Schema Validation**: Checks for missing columns and data type mismatches.
- **Missing Value Correction**: Automatically fills missing values using mean or mode.
- **Outlier Detection**: Statistical outlier detection using Z-score.
- **Outlier Capping**: Automatically caps outliers to keep data within statistical bounds.
- **Duplicate Removal**: Removes duplicate rows based on all columns.
- **Quality Reports**: Generates detailed JSON reports with before/after metrics.
- **Audit Logging**: Keeps an audit trail of all operations in `logs/audit.log`.

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Streamlit UI
Run the interactive dashboard:
```bash
streamlit run ui/app.py
```

### CLI
Process a file via command line:
```bash
python cli/main.py --file path/to/data.csv --report report.json --output cleaned_data.csv
```

## Configuration
Customize validation rules in `config/rules.yaml`. You can define:
- Required columns
- Expected data types
- Minimum and maximum value ranges
- Outlier sensitivity (Z-score threshold)
- Missing value strategies

## Testing
Run tests using pytest:
```bash
pytest
```
# Automate_Data_Validation_engine
