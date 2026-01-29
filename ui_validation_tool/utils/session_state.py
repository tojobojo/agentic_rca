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


def set_active_task(task_key):
    """Callback to keep target task expanded on interaction."""
    st.session_state['expanded_task'] = task_key


def initialize_task_data(task_key):
    """Initialize empty data structure for a task if not exists."""
    if task_key not in st.session_state['task_data']:
        st.session_state['task_data'][task_key] = {
            'sources': [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked'}],
            'targets': [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked'}]
        }
