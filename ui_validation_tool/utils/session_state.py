"""Session state management for the UI Validation Tool."""
import streamlit as st


def initialize_session_state():
    """Initialize all session state variables."""
    if 'job_tasks' not in st.session_state:
        st.session_state['job_tasks'] = []
    if 'task_data' not in st.session_state:
        st.session_state['task_data'] = {}
    if 'dq_rules' not in st.session_state:
        st.session_state['dq_rules'] = {}
    if 'column_cache' not in st.session_state:
        st.session_state['column_cache'] = {}
    if 'expanded_task' not in st.session_state:
        st.session_state['expanded_task'] = None
    if 'analysis_cache' not in st.session_state:
        st.session_state['analysis_cache'] = {}
    if 'excluded_tasks' not in st.session_state:
        st.session_state['excluded_tasks'] = []
    if 'job_task_snapshot' not in st.session_state:
        st.session_state['job_task_snapshot'] = []
    if 'loaded_manifest_version' not in st.session_state:
        st.session_state['loaded_manifest_version'] = None
    if 'validation_errors' not in st.session_state:
        st.session_state['validation_errors'] = {}

    # Analysis state
    if 'analysis_run_count' not in st.session_state:
        st.session_state['analysis_run_count'] = 0
    if 'analysis_running' not in st.session_state:
        st.session_state['analysis_running'] = False
    if 'analysis_output' not in st.session_state:
        st.session_state['analysis_output'] = None
    if 'trigger_analysis' not in st.session_state:
        st.session_state['trigger_analysis'] = False
    if 'confirm_rerun' not in st.session_state:
        st.session_state['confirm_rerun'] = False
    if 'last_completed_run' not in st.session_state:
        st.session_state['last_completed_run'] = 0

    # Validation state
    if 'trigger_validation' not in st.session_state:
        st.session_state['trigger_validation'] = False
    if 'validation_result' not in st.session_state:
        st.session_state['validation_result'] = None
    if 'save_submit_message' not in st.session_state:
        st.session_state['save_submit_message'] = None
    if 'validation_cache' not in st.session_state:
        st.session_state['validation_cache'] = {}   


def set_active_task(task_key):
    """Callback to keep target task expanded on interaction."""
    st.session_state['expanded_task'] = task_key


def initialize_task_data(task_key):
    """Initialize empty data structure for a task if not exists."""
    if task_key not in st.session_state['task_data']:
        st.session_state['task_data'][task_key] = {
            'sources': [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked'}],
            'targets': [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked', 'load_type': 'FULL_REFRESH', 'filter_column': ''}]
        }
