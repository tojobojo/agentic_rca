"""Validation functions for the UI Validation Tool."""
import streamlit as st


def validate_manifest_completeness():
    """Validate that all sources/targets have required fields filled."""
    errors = {}
    
    for task_key, task_data in st.session_state['task_data'].items():
        task_errors = []
        
        for idx, src in enumerate(task_data.get('sources', [])):
            if not src.get('identifier') or not src.get('identifier').strip():
                task_errors.append(f"Source #{idx+1}: Missing identifier")
            if not src.get('subtype') or not src.get('subtype').strip():
                task_errors.append(f"Source #{idx+1}: Missing subtype")
        
        for idx, tgt in enumerate(task_data.get('targets', [])):
            if not tgt.get('identifier') or not tgt.get('identifier').strip():
                task_errors.append(f"Target #{idx+1}: Missing identifier")
            if not tgt.get('subtype') or not tgt.get('subtype').strip():
                task_errors.append(f"Target #{idx+1}: Missing subtype")
            
            # Check Append Config
            load_type = tgt.get('load_type', 'FULL_REFRESH')
            filter_col = tgt.get('filter_column', '')
            if load_type == 'APPEND' and not filter_col.strip():
                task_errors.append(f"Target #{idx+1} ({tgt.get('identifier')}): Append mode requires a Filter Column.")
        
        if task_errors:
            errors[task_key] = task_errors
    
    return errors
