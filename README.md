# 🧹 Automated Data Validation & Cleaning Engine (DataClean Pro)

A fully automated, rule-driven **data quality, validation, and cleaning platform** built with Python, Pandas, NumPy, and Streamlit.  
Designed with **enterprise-grade data integrity guarantees**, deterministic pipelines, and audit-level traceability.

---

## 🚀 Key Features

- 📊 **Data Quality Profiling & Overview**
- 🧠 **Rule-Based Cleaning Lab**
- ✅ **Post-Clean Validation Engine**
- 📈 **Raw vs Cleaned Data Visualization**
- 📥 **Deterministic CSV / Excel Export**
- 🧾 **Full Audit & Lineage Tracking**
- 🎯 **Objective Data Quality Scoring (0–100)**

---

## 🏗️ Logical Architecture Overview

The system enforces a **strict linear pipeline**.  
Each stage acts as a gatekeeper for the next.

UPLOAD
↓
RAW OVERVIEW
↓
PROFILING & ANALYSIS
↓
CLEANING LAB
↓
VALIDATION
↓
VISUALIZATION
↓
EXPORT & REPORTING


### Pipeline Failure Semantics

Each stage emits a terminal state:

| State | Meaning |
|------|--------|
| PASS | Stage completed successfully |
| WARN | Non-fatal issues detected (penalties applied) |
| FAIL | Critical integrity breach — pipeline stops |

---

## 🧬 Data State Model (Immutable by Design)

| State | Description | Mutability |
|-----|------------|-----------|
| Raw Dataset | Original uploaded data | READ-ONLY |
| Analyzed Dataset | Raw data + statistical metadata | READ-ONLY |
| Cleaned Dataset | Output of deterministic cleaning rules | IMMUTABLE |

---

## 🔍 Data Profiling & Analysis

The profiling engine generates a **Statistical Signature** without modifying data.

### Guarantees
- Intelligent **data type inference** (majority-based)
- Precise **missing value detection**
  - `null`, `NaN`, empty strings, whitespace
- **Cardinality analysis** (ID vs categorical)
- **Outlier likelihood detection** via distribution skew

---

## 🧪 Cleaning Lab (Logic-Only, Deterministic)

Cleaning transforms Raw → Cleaned data **only through explicit rules**.

### Supported Cleaning Operations

- Missing value imputation (type-aware)
- Date & time normalization
- Outlier clipping (IQR / Z-score)
- Text sanitization & standardization
- Data type enforcement
- Range & constraint enforcement
- Cross-column consistency checks
- Scaling & normalization
- Duplicate row & column removal

### Rule Precedence Hierarchy

1. Domain constraints (hard rules)
2. Data type enforcement
3. Statistical heuristics
4. Cosmetic standardization

Every transformation is logged in the **Audit Trail**.

---

## 🏷️ Column Header Normalization (Critical Contract)

Column headers in the **Cleaned Dataset** are normalized for clarity and consistency.

### Normalization Rules
- Trim whitespace
- Replace `_` and `.` with spaces
- Resolve duplicate columns
- Convert to **Title Case**

Example:
title_year → Title Year
title_year.1 → Title Year (2)


### Dataset Scope

| Dataset | Header Behavior |
|------|----------------|
| Raw | Original headers preserved |
| Analyzed | Mirrors raw headers |
| Cleaned | Normalized (Title Case) |

---

## ✅ Validation Engine

Validation is **read-only** and runs *after cleaning*.

### Validation Checks
- Missing value validation
- Duplicate detection
- Range & constraint validation
- Outlier validation
- Categorical validation
- Date & time validation
- Cross-column validation
- Statistical drift detection

❌ Validation never modifies data  
✅ Only flags issues & adjusts Quality Score

---

## 📊 Data Quality Score (0–100)

An objective measurement of dataset fitness.

### Scoring Logic
- Start at **100**
- Missing value penalties (weighted)
- Duplicate density penalties (exponential)
- Constraint violations (fixed deductions)
- Outlier volume penalties
- Semantic drift penalties

Column importance can be **user-defined or auto-inferred**.

---

## 📈 Visualization Layer

- Raw vs Cleaned comparisons
- Missing value heatmaps
- Distribution & outlier plots
- Drift indicators

📌 All visuals use the **same statistics** as the scoring engine  
📌 No recomputation or divergence allowed

---

## 📤 Export & Download Behavior

### Cleaned File Naming Contract (MANDATORY)

<original_filename>_cleaned_data.<extension>


#### Examples

| Original | Cleaned |
|--------|--------|
| abc.csv | abc_cleaned_data.csv |
| movies.xlsx | movies_cleaned_data.xlsx |

### Export Guarantees
- No overwriting original files
- One-to-one raw → cleaned mapping
- CSV → CSV, Excel → Excel
- Export blocked if cleaning FAILS

### Excel Enhancements
- Fixed / frozen header row
- Table formatting
- Auto-width columns

---

## 🧾 Audit & Traceability

Every operation is recorded:

- Rule ID
- Affected rows
- Value deltas
- Original vs cleaned filename
- Header normalization mapping
- Cleaning configuration version

This ensures **full lineage and reproducibility**.

---

## 🖥️ UI Consistency Rules

- Navigation is **state-locked**
- Visualization unavailable until Cleaned Dataset exists
- Changing rules recalculates **Projected Quality Score**
- Cleaned headers displayed consistently across UI

---

## 🛠️ Tech Stack

- **Python**
- **Pandas**
- **NumPy**
- **Streamlit**
- **OpenPyXL / CSV**

---

## 🎯 Why This Project Matters

This system demonstrates:
- Real-world data engineering discipline
- Deterministic pipelines
- Audit-safe data transformations
- Enterprise-grade UX + data integrity
- Production-ready validation logic

---

## 📌 Status

✅ Core pipeline implemented  
🔧 Actively enhancing UI, validation depth, and scalability

---

## 📜 License

MIT License
