"""
Execution Timeline Component
Visualizes the pipeline execution flow with stage-by-stage breakdown
"""
import streamlit as st
import pandas as pd
from datetime import datetime

def render_execution_timeline(report: dict):
    """
    Render a visual timeline of pipeline execution.
    
    Args:
        report: Pipeline execution report
    """
    st.header("⏱️ Execution Timeline")
    
    step_logs = report.get('step_logs', [])
    
    if not step_logs:
        st.warning("No execution data available")
        return
    
    # Create timeline visualization
    st.markdown("### Pipeline Stages")
    
    for i, step in enumerate(step_logs, 1):
        step_name = step.get('step', 'Unknown').replace('_', ' ').title()
        timestamp = step.get('timestamp', 'N/A')
        
        # Determine status icon
        if step.get('passed') == False:
            icon = "❌"
            status_color = "red"
        elif step.get('errors'):
            icon = "⚠️"
            status_color = "orange"
        else:
            icon = "✅"
            status_color = "green"
        
        # Create expandable section for each stage
        with st.expander(f"{icon} **Stage {i}: {step_name}**", expanded=(i == len(step_logs))):
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"**Status:** :{status_color}[{icon} {'Completed' if not step.get('errors') else 'Completed with warnings'}]")
                
                if timestamp != 'N/A':
                    st.caption(f"Executed at: {timestamp}")
                
                # Show key metrics
                if 'dropped_rows' in step and step['dropped_rows'] > 0:
                    st.metric("Rows Dropped", step['dropped_rows'])
                
                if 'imputed' in step:
                    imputed = step.get('imputed', {})
                    if imputed:
                        st.metric("Columns Imputed", len(imputed))
                
                if 'handled' in step:
                    handled = step.get('handled', [])
                    if handled:
                        st.metric("Outliers Handled", len(handled))
            
            with col2:
                # Show detailed logs
                if 'actions' in step:
                    st.markdown("**Actions:**")
                    for action in step['actions'][:5]:  # Show first 5
                        st.caption(f"• {action}")
                
                if 'errors' in step and step['errors']:
                    st.markdown("**Errors:**")
                    for error in step['errors'][:3]:
                        st.error(error, icon="🚫")
                
                if 'warnings' in step and step['warnings']:
                    st.markdown("**Warnings:**")
                    for warning in step['warnings'][:3]:
                        st.warning(warning, icon="⚠️")
    
    # Summary metrics
    st.markdown("---")
    st.markdown("### Execution Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Stages", len(step_logs))
    
    with col2:
        successful = sum(1 for s in step_logs if not s.get('errors'))
        st.metric("Successful", successful)
    
    with col3:
        with_errors = sum(1 for s in step_logs if s.get('errors'))
        st.metric("With Errors", with_errors)
    
    with col4:
        if 'quality_score' in report:
            st.metric("Quality Score", f"{report['quality_score']:.0f}%")

def render_stage_flow_diagram(step_logs: list):
    """
    Render a simple flow diagram of stages.
    
    Args:
        step_logs: List of step execution logs
    """
    st.markdown("### Stage Flow")
    
    # Create a simple text-based flow
    flow_text = " → ".join([
        step.get('step', 'Unknown').replace('_', ' ').title()
        for step in step_logs
    ])
    
    st.code(flow_text, language=None)

def render_performance_metrics(report: dict):
    """
    Render performance metrics.
    
    Args:
        report: Pipeline execution report
    """
    st.markdown("### Performance Metrics")
    
    initial_stats = report.get('initial_stats', {})
    final_stats = report.get('final_stats', {})
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Initial State**")
        st.write(f"- Rows: {initial_stats.get('rows', 0):,}")
        st.write(f"- Columns: {initial_stats.get('cols', 0)}")
        st.write(f"- Missing: {initial_stats.get('missing_total', 0):,}")
        st.write(f"- Duplicates: {initial_stats.get('duplicates', 0):,}")
    
    with col2:
        st.markdown("**Final State**")
        st.write(f"- Rows: {final_stats.get('rows', 0):,}")
        st.write(f"- Columns: {final_stats.get('cols', 0)}")
        st.write(f"- Missing: {final_stats.get('missing_total', 0):,}")
        st.write(f"- Duplicates: {final_stats.get('duplicates', 0):,}")
    
    # Calculate improvements
    if initial_stats.get('rows', 0) > 0:
        retention = (final_stats.get('rows', 0) / initial_stats.get('rows', 1)) * 100
        st.progress(retention / 100)
        st.caption(f"Data Retention: {retention:.1f}%")
