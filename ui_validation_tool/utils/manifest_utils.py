"""Manifest utility functions for the UI Validation Tool."""
import streamlit as st
from typing import Dict, Any


def get_cache_key(task):
    """Generate cache key for intelligent deduplication."""
    task_type = task.get('task_type', 'unknown')
    script_path = task.get('script_path', '')
    
    if task_type == 'wheel' and script_path:
        return f"wheel:{script_path}"
    elif task_type == 'notebook' and script_path:
        return f"notebook:{script_path}"
    elif task_type == 'python' and script_path:
        return f"python:{script_path}"
    elif task_type == 'sql':
        return f"sql:{task['task_key']}"  # SQL is usually unique
    else:
        return f"unknown:{task['task_key']}"


def check_manifest_changes(current_manifest: Dict[str, Any], loaded_manifest: Dict[str, Any]) -> bool:
    """
    Check if manifest has changed.
    Returns True if changes detected, False otherwise.
    """
    # Compare manifests (excluding _metadata for comparison)
    current_copy = {k: v for k, v in current_manifest.items() if k != '_metadata'}
    loaded_copy = {k: v for k, v in loaded_manifest.items() if k != '_metadata'}
    
    return current_copy != loaded_copy


def add_manifest_metadata(manifest: Dict[str, Any]) -> Dict[str, Any]:
    """Add metadata to manifest."""
    manifest['_metadata'] = {
        'excluded_tasks': st.session_state.get('excluded_tasks', []),
        'job_task_snapshot': st.session_state.get('job_task_snapshot', [])
    }
    return manifest
