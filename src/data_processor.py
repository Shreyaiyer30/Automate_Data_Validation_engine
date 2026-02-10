import pandas as pd
from typing import Dict, Any, Tuple
from src.rule_generator import DynamicRuleGenerator
from src.dynamic_cleaner import DynamicDataCleaner
from src.engine.atomic_engine import AtomicEngine
import logging

class DataProcessor:
    """
    High-level facade for data processing.
    Integrates the AtomicEngine with Dynamic Rule Generation and Cleaning.
    """
    VERSION = "1.1.0" # Architectural Contract Version
    
    def __init__(self, settings: Dict = None):
        self.settings = settings or {}
        self.engine = AtomicEngine(config=self.settings) 
        self.rule_generator = DynamicRuleGenerator(self.settings)
        self.cleaner = DynamicDataCleaner(self.rule_generator, logger=self.engine.logger)
        self.logger = logging.getLogger(__name__)

    def analyze_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a deep analysis and detect duplicate columns for the dataset."""
        if df is None:
            return {"suggested_rules": {}, "column_analysis": {}, "quality_score": 0, "duplicates": {}}
            
        # --- ARCHITECTURAL CONTRACT: DATA TYPE ENFORCEMENT ---
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"DataProcessor expects a pandas.DataFrame, but received {type(df)}")
            
        analysis = self.rule_generator.generate_cleaning_rules(df)
        _, merged_info = self._resolve_duplicate_columns(df)
        analysis['resolution_summary'] = merged_info
        return analysis

    def simulate_impact(self, df: pd.DataFrame, rules: Dict[str, Any]) -> float:
        """
        --- ARCHITECTURAL CONTRACT: RULE IMPACT SIMULATION ---
        Responsibility: Predict quality score delta without mutating state or logging.
        Integrity: Uses a shadow cleaner to prevent contamination of the main audit log.
        ------------------------------------------------------
        """
        if df is None: return 0.0
        
        try:
            # 1. Shadow Isolation: Use a temporary cleaner with NO logger
            shadow_cleaner = DynamicDataCleaner(self.rule_generator, logger=None)
            
            # 2. Sample for Performance (Non-destructive)
            sample_df = df.head(1000) if len(df) > 1000 else df
            
            # 3. Predict Transformation
            df_sim = shadow_cleaner.apply_rules(sample_df, rules)
            
            # 4. Score Projection
            initial_stats = self.analyze_data_quality(sample_df).get("column_analysis", {})
            report = self.engine.reporter.build_report(
                initial_df=sample_df,
                final_df=df_sim,
                audit_entries=[], # Strictly empty for simulation
                config=rules,
                initial_stats=initial_stats
            )
            return report['quality_score']
        except Exception as e:
            self.logger.warning(f"Simulation failed: {str(e)}")
            return 0.0

    def process_data(self, df: pd.DataFrame, rules: Dict[str, Any] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Orchestrate the end-to-end cleaning pipeline with failure semantics.
        1. Capture initial stats
        2. Run Atomic pipeline (fail-fast)
        3. Apply Dynamic rules (precedence-aware)
        4. Generate final report
        """
        if df is None:
            return None, {}

        # --- ARCHITECTURAL CONTRACT: DATA TYPE ENFORCEMENT ---
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"DataProcessor expects a pandas.DataFrame, but received {type(df)}")

        # 0. Structural Resolution (Duplicate Column Merging)
        df, merged_info = self._resolve_duplicate_columns(df)
        if merged_info:
            self.logger.info(f"Duplicate columns resolved: {merged_info}")

        # 1. Baseline Analysis (Capture Initial Stats)
        initial_analysis = self.analyze_data_quality(df)
        initial_stats = initial_analysis.get("column_analysis", {})
        
        # 2. Stage 1: Atomic Engine (Core Pipeline)
        # LifecycleManager handles PASS/WARN/FAIL
        df_atomic, report_atomic = self.engine.run(df, initial_stats=initial_stats)
        
        # 3. Stage 2: Dynamic Overlay (Rule-based cleaning if provided)
        config_version = self._generate_config_version(rules or {})
        
        if rules:
            self.logger.info(f"Applying Dynamic Rules overlay [{config_version}]")
            final_df = self.cleaner.apply_rules(df_atomic, rules)
            
            # Re-generate report for final state
            final_report = self.engine.reporter.build_report(
                initial_df=df,
                final_df=final_df,
                audit_entries=self.engine.logger.entries,
                config=rules,
                initial_stats=initial_stats
            )
            final_report['config_version'] = config_version
            final_report['audit_log'] = self.engine.logger.entries
            final_report['resolution_summary'] = merged_info # Exported for UI
        else:
            final_df = df_atomic
            final_report = report_atomic
            final_report['config_version'] = "default"
            final_report['audit_log'] = self.engine.logger.entries
            final_report['resolution_summary'] = merged_info
            
        # 4. Phase 4: Authoritative Header Normalization
        try:
            final_df, header_mapping = self._normalize_headers(final_df)
            final_report['header_normalization'] = {
                "applied": True,
                "mapping": header_mapping
            }
            self.engine.logger.log_mutation(
                "SEMANTIC_LAYER", 
                "normalize_headers", 
                {"mapping_count": len(header_mapping)}
            )
        except Exception as e:
            self.logger.error(f"Header normalization failed: {str(e)}")
            # In Phase 4, normalization failure is FATAL for the cleaned state
            final_report['pipeline_status'] = "FAIL"
            final_report['errors'] = final_report.get('errors', []) + [f"Semantic Error: {str(e)}"]

        return final_df, final_report

    def _suggest_rules(self, df: pd.DataFrame) -> Dict:
        """Helper for compatibility with older code if any."""
        return self.rule_generator.generate_cleaning_rules(df)["suggested_rules"]

    def _generate_config_version(self, rules: Dict[str, Any]) -> str:
        """Generate a deterministic version hash for the rule configuration."""
        import hashlib
        import json
        config_str = json.dumps(rules, sort_keys=True).encode('utf-8')
        return f"v_{hashlib.md5(config_str).hexdigest()[:8]}"

    def audit_export(self, format: str, columns: list):
        """Log an export event with presentation metadata."""
        self.engine.logger.log_mutation(
            "EXPORT_ENGINE", 
            f"generate_{format.lower()}", 
            {
                "format": format.upper(),
                "column_count": len(columns),
                "header_mode": "Title-Case-Normalized"
            }
        )

    def _normalize_headers(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        --- ARCHITECTURAL CONTRACT: HEADER NORMALIZATION ---
        1. Clean: trim, replace [._] with ' ', collapse spaces.
        2. Format: Title Case.
        3. Handle Duplicates: Suffix with (2), (3) instead of .1
        4. Scope: Cleaned Dataset only.
        ----------------------------------------------------
        """
        import re
        
        old_columns = df.columns.tolist()
        normalized_cols = []
        seen_names = {}
        mapping = {}
        
        for col in old_columns:
            # 1. Cleaning & Title Case
            norm = str(col).strip()
            norm = re.sub(r'[._]+', ' ', norm)
            norm = re.sub(r'\s+', ' ', norm).strip()
            norm = norm.title()
            
            # 2. Duplicate Check (Readable Suffixes)
            if norm not in seen_names:
                seen_names[norm] = 1
                final_name = norm
            else:
                seen_names[norm] += 1
                final_name = f"{norm} ({seen_names[norm]})"
            
            normalized_cols.append(final_name)
            mapping[col] = final_name
            
        df_norm = df.copy()
        df_norm.columns = normalized_cols
        return df_norm, mapping

    def _resolve_duplicate_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        --- ARCHITECTURAL CONTRACT: DYNAMIC DUPLICATE RESOLUTION ---
        1. Normalize: lowercase, strip (suffix: .1, _1, _x, _v1, etc.)
        2. Heuristic: Pick healthiest (lowest nulls, highest entropy) as canonical head.
        3. Merge: First Non-Null Precedence.
        4. Detect: Log value conflicts where multiple columns hold data.
        ------------------------------------------------------------
        """
        import re
        import numpy as np
        
        df = df.copy()
        merged_info = {}
        
        # 1. Advanced Normalization Path
        def normalize(name: str) -> str:
            name = str(name).strip().lower()
            # Remove common suffixes and repeated delimiters
            name = re.sub(r'(\d+|\.1|\._1|\._x|\._y)$', '', name) # Simple numeric/known suffixes
            name = re.sub(r'(_[xy1234]|\.[\dxy])$', '', name)   # Patterns like _x, .1
            name = re.sub(r'(_v\d+|_rev\d+)$', '', name)      # Versions: _v1, _rev2
            name = re.sub(r'[_\.\-\s]+', '_', name)           # Consolidate delimiters
            return name.strip('_')

        cols_groups = {}
        for col in df.columns:
            norm = normalize(col)
            if norm not in cols_groups: cols_groups[norm] = []
            cols_groups[norm].append(col)

        # 2. Resolve Groups with Heuristics
        resolved_df = pd.DataFrame(index=df.index)
        
        for norm, originals in cols_groups.items():
            if len(originals) > 1:
                # HEURISTIC SELECTION: Pick the healthiest column
                # Score = (1 - %Missing) * (Column Entropy/Cardinality)
                health_stats = []
                for o_col in originals:
                    null_pct = df[o_col].isna().mean()
                    nunique = df[o_col].nunique()
                    score = (1.0 - null_pct) * (nunique / len(df) if len(df) > 0 else 0)
                    health_stats.append((o_col, score, null_pct))
                
                # Sort: Best score first, tie-break with original position
                health_stats.sort(key=lambda x: (-x[1], x[2]))
                canonical_col = health_stats[0][0]
                secondary_cols = [x[0] for x in health_stats[1:]]
                
                # 3. Data-Driven Merge (First Non-Null Precedence)
                merged_series = df[canonical_col].copy()
                conflict_count = 0
                
                for sec in secondary_cols:
                    # Detect Conflicts (where both have different non-null values)
                    both_not_null = df[canonical_col].notna() & df[sec].notna()
                    conflicts = both_not_null & (df[canonical_col].astype(str) != df[sec].astype(str))
                    conflict_count += conflicts.sum()
                    
                    # Merge (preserve canonical values, fill from secondary)
                    merged_series = merged_series.combine_first(df[sec])
                
                resolved_df[canonical_col] = merged_series
                merged_info[canonical_col] = {
                    "merged_from": secondary_cols,
                    "strategy": "heuristic_performance_precedence",
                    "conflict_count": int(conflict_count),
                    "canonical_score": round(health_stats[0][1], 3)
                }
            else:
                resolved_df[originals[0]] = df[originals[0]]
        
        return resolved_df, merged_info
