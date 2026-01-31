# Automated Data Validation & Cleaning Engine (v2)

A production-ready data cleaning pipeline with a modern Streamlit UI. Designed for high-performance data validation, cleaning, and reporting.

## 🚀 Key Features
- **Clean Architecture**: Decoupled core engine, validation stages, and UI.
- **Atomic Pipeline**: Orchestrated stages with robust error handling.
- **Audit Logging**: Full traceability for every data mutation.
- **Quality Reporting**: Automated scoring (0-100) and descriptive metrics.
- **Modern UI**: Dark-themed, glassmorphic Streamlit interface.

## 🛠️ Project Structure
```text
automated-data-validation-engine/
├── data/              # Dataset storage
├── src/
│   ├── engine/        # Core pipeline logic
│   ├── cli/           # CLI tools
│   └── utils/         # Helper functions
├── ui/                # Streamlit interface
├── tests/             # Quality assurance
└── run.py             # Entry point
```

## 🚦 Quick Start
1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
2. **Launch the UI**:
   ```bash
   python run.py
   ```
   Or directly:
   ```bash
   streamlit run ui/app.py
   ```

## 🔧 Extending the Pipeline
Add new stages to `src/engine/stages/` by inheriting from `BaseStage` and registering them in `LifecycleManager`.
