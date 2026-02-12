import streamlit as st
import pandas as pd
import json
import logging
from backend.databricks_client import DatabricksService
from backend.config import get_config, setup_logging
from ai_agents.mapping_agent import MappingAgent

# Import utility modules
from utils.session_state import initialize_session_state, set_active_task, initialize_task_data
from utils.validators import validate_manifest_completeness
from utils.manifest_utils import get_cache_key, check_manifest_changes, add_manifest_metadata
from ui.components import render_sidebar

# Setup Page Config (MUST BE FIRST)
st.set_page_config(
    page_title="RCA Configuration & Validation",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize Logging
config = get_config()
setup_logging(config.log_level)
logger = logging.getLogger(__name__)

# --- Custom CSS for "Beautiful" Design ---
st.markdown("""
<style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    h1 {
        color: #1E3A8A; /* Dark Blue */
        font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
    }
    .stButton>button {
        background-color: #2563EB; /* Blue 600 */
        color: white;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 600;
        border: none;
    }
    .stButton>button:hover {
        background-color: #1D4ED8; /* Blue 700 */
        color: white;
    }
    .stSuccess {
        background-color: #D1FAE5;
        color: #065F46;
        padding: 1rem;
        border-radius: 8px;
    }
    .metric-card {
        background-color: #F3F4F6;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #2563EB;
    }
</style>
""", unsafe_allow_html=True)

# --- Sidebar ---
job_params_input, manifest_table = render_sidebar(config)

# --- Main Content ---
st.title("🔍 Agentic RCA Validator")
st.markdown("Generate and validate Unity Catalog lineage mappings for your Databricks Jobs.")

# Initialize Session State
initialize_session_state()

# --- Step 1: Job Configuration ---
with st.expander("1️⃣ Job Configuration", expanded=True):
    job_id_input = st.text_input("Databricks Job ID", help="The numeric ID of your Databricks Job",
        value=st.session_state.get("current_job_id", ""))
    # Store in session state for use elsewhere
    st.session_state["current_job_id"] = job_id_input if job_id_input else ""

    if st.button("Fetch Job"):
        if job_id_input:
            with st.spinner("Fetching Job Details..."):
                try:
                    db_service = DatabricksService()
                    tasks = db_service.get_job_tasks(int(job_id_input))

                    # Filter out unknown task types
                    valid_tasks = [
                        t for t in tasks
                        if t.get('task_type') and t['task_type'].lower() != "unknown"
                    ]

                    st.session_state['job_tasks'] = valid_tasks
                    st.session_state['job_task_snapshot'] = [t['task_key'] for t in valid_tasks]

                    # Initialize task data for all tasks
                    for task in valid_tasks:
                        initialize_task_data(task['task_key'])

                    # Auto-expand first task
                    if valid_tasks:
                        st.session_state['expanded_task'] = valid_tasks[0]['task_key']

                    # Clear analysis cache and counter on new job fetch
                    st.session_state['analysis_cache'] = {}
                    st.session_state['analysis_run_count'] = 0
                    st.session_state['excluded_tasks'] = []
                    st.session_state['loaded_manifest_version'] = None
                    st.session_state['validation_result'] = None  # Clear validation result
                    st.session_state['analysis_output'] = None    # Clear analysis output
                    st.session_state['validation_cache'] = {}     # Clear validation cache

                    st.success(f"✅ Found {len(valid_tasks)} valid tasks (filtered out {len(tasks)-len(valid_tasks)} unknown types).")

                    # Query for existing manifest
                    if manifest_table:
                        manifest_result = db_service.load_latest_manifest(manifest_table, job_id_input)

                        if manifest_result.get('found'):
                            st.session_state['existing_manifest'] = manifest_result
                        else:
                            st.session_state['existing_manifest'] = None

                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("Please enter Job ID.")

# --- Step 1.5: Load Existing Manifest ---
if st.session_state.get('existing_manifest') and st.session_state.get('loaded_manifest_version') is None:
    manifest_info = st.session_state['existing_manifest']

    st.markdown("---")
    st.markdown("### 📄 Existing Manifest Found")

    col_info, col_actions = st.columns([2, 1])

    with col_info:
        st.info(f"""
**Status:** {manifest_info['status']}
**Version:** {manifest_info['version']}
**Last Updated:** {manifest_info['date']}
**Created By:** {manifest_info['created_by']}
""")

    with col_actions:
        if st.button("📥 Load Existing", type="primary"):
            # Load manifest data
            manifest_data = manifest_info['manifest_data']

            # Detect changes
            current_task_keys = set([t['task_key'] for t in st.session_state['job_tasks']])
            manifest_task_keys = set(manifest_data.keys())
            excluded_tasks = set(manifest_data.get('_metadata', {}).get('excluded_tasks', []))
            job_snapshot = set(manifest_data.get('_metadata', {}).get('job_task_snapshot', manifest_task_keys))

            # Truly new tasks (not user-deleted)
            truly_new = current_task_keys - job_snapshot

            # Job-removed tasks
            job_removed = job_snapshot - current_task_keys

            # Load task data
            for task_key in current_task_keys:
                if task_key in manifest_data and task_key not in excluded_tasks:
                    # Load from manifest
                    st.session_state['task_data'][task_key] = {
                        'sources': manifest_data[task_key].get('sources', [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked'}]),
                        'targets': manifest_data[task_key].get('targets', [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked'}])
                    }

                    # Load DQ rules
                    dq_rules = manifest_data[task_key].get('dq_rules', {})
                    for asset_name, rules in dq_rules.items():
                        st.session_state['dq_rules'][asset_name] = rules

            # Track excluded tasks
            st.session_state['excluded_tasks'] = list(excluded_tasks)
            st.session_state['loaded_manifest_version'] = manifest_info['version']

            # Show change summary
            change_msg = f"📄 Loaded manifest v{manifest_info['version']}"
            if truly_new:
                change_msg += f" + {len(truly_new)} new task(s) detected"
            if job_removed:
                st.warning(f"⚠️ {len(job_removed)} task(s) removed from job: {', '.join(job_removed)}")

            st.success(change_msg)
            st.rerun()

        if st.button("🆕 Start Fresh"):
            st.session_state['existing_manifest'] = None
            st.session_state['loaded_manifest_version'] = "new"
            st.info("Starting fresh manifest creation.")
            st.rerun()


# --- Step 1.5: AI Analyze All Tasks ---
if st.session_state['job_tasks']:
    st.markdown("---")
    col_analyze, col_info = st.columns([1, 3])

    # Dialog for re-run confirmation
    @st.dialog("⚠️ Confirm Re-run Analysis")
    def confirm_rerun():
        st.warning(f"Analysis has already been run {st.session_state['analysis_run_count']} time(s).")
        st.markdown("**Re-running will incur additional AI costs. Are you sure you want to continue?**")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Yes, Re-run", type="primary", width="stretch"):
                st.session_state['confirm_rerun'] = True
                st.rerun()

        with col2:
            if st.button("❌ Cancel", width="stretch"):
                st.session_state['confirm_rerun'] = False
                st.rerun()

    with col_analyze:
        # Show appropriate button with dynamic state
        if st.session_state['analysis_running']:
            # Show progress in button if available
            if st.session_state.get('analysis_output') and st.session_state['analysis_output'].get('progress'):
                progress = st.session_state['analysis_output'].get('progress', 0)
                total = st.session_state['analysis_output'].get('total', 1)
                button_label = f"⏳ Analyzing... ({progress}/{total})"
            else:
                button_label = "⏳ Analysis Running..."
            button_disabled = True
        else:
            button_label = "🔍 Analyze All Tasks" if st.session_state['analysis_run_count'] == 0 else "🔄 Run Analysis"
            button_disabled = False

        button_clicked = st.button(button_label, type="primary", disabled=button_disabled)

        # Handle button click
        if button_clicked and not st.session_state['analysis_running']:
            if st.session_state['analysis_run_count'] > 0:
                # Show confirmation dialog
                confirm_rerun()
            else:
                # First run, set flag to trigger analysis
                st.session_state['trigger_analysis'] = True
                st.session_state['analysis_running'] = True  # Set running state immediately
                st.rerun()

        # Handle dialog response
        if st.session_state['confirm_rerun'] is True:
            st.session_state['confirm_rerun'] = None  # Reset
            st.session_state['trigger_analysis'] = True
            st.session_state['analysis_running'] = True  # Set running state immediately
            st.session_state['analysis_output'] = None    # Clear analysis output
            st.rerun()
        elif st.session_state['confirm_rerun'] is False:
            st.session_state['confirm_rerun'] = None  # Reset

    with col_info:
        # Calculate total tokens from cached analysis
        total_tokens = 0
        if st.session_state.get('analysis_cache'):
            for cache_key, mapping in st.session_state['analysis_cache'].items():
                if mapping and hasattr(mapping, 'token_stats'):
                    total_tokens += mapping.token_stats.get('total_tokens', 0)

        if total_tokens > 0:
            st.info(f"💡 **Tip:** AI will intelligently cache shared wheel files to avoid redundant analysis.\n\n📊 **Total Tokens Used:** {total_tokens:,}")
        else:
            st.info("💡 **Tip:** AI will intelligently cache shared wheel files to avoid redundant analysis.")

    # Show completion message at full width (outside columns)
    if st.session_state.get('analysis_output') and st.session_state['analysis_output']['status'] == 'complete' and not st.session_state['analysis_running']:
        st.success("✅ Analysis complete! Tables have been populated. View details in the Analysis Output section below.")

    # Check if we should run analysis
    if st.session_state.get('trigger_analysis'):
        # Reset trigger and set running state
        st.session_state['trigger_analysis'] = False
        st.session_state['analysis_running'] = True
        st.session_state['analysis_run_count'] += 1
        st.session_state['analysis_output'] = {'status': 'running', 'logs': []}

        # Rerun to update button state before starting analysis
        st.rerun()

    # Execute analysis if running
    if st.session_state.get('analysis_running') and st.session_state.get('analysis_output') and st.session_state['analysis_output'].get('status') == 'running':
        # Show the output expander immediately
        with st.expander("🧠 AI Analysis Output", expanded=True):
            analysis_container = st.container(height=400)

            with analysis_container:
                try:
                    with st.spinner("Initializing analysis..."):
                        agent = MappingAgent()
                        db_service = DatabricksService()

                    # Step 1: Build analysis groups
                    analysis_groups = {}  # {cache_key: [task_keys]}
                    task_to_cache_key = {}  # {task_key: cache_key}

                    for task in st.session_state['job_tasks']:
                        task_key = task['task_key']
                        cache_key = get_cache_key(task)

                        if cache_key not in analysis_groups:
                            analysis_groups[cache_key] = []
                        analysis_groups[cache_key].append(task_key)
                        task_to_cache_key[task_key] = cache_key

                    total_tasks = len(st.session_state['job_tasks'])
                    unique_sources = len(analysis_groups)

                    st.markdown(f"🔍 Analyzing **{unique_sources} unique code sources** for **{total_tasks} tasks**")
                    st.session_state['analysis_output']['logs'].append(
                        f"🔍 Analyzing **{unique_sources} unique code sources** for **{total_tasks} tasks**"
                    )

                    # Step 2: Analyze each unique code source once
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    analyzed_count = 0
                    st.session_state['analysis_output']['progress'] = 0
                    st.session_state['analysis_output']['total'] = unique_sources

                    for cache_key, task_keys in analysis_groups.items():
                        # Show task names using this cache key
                        task_names = ", ".join(task_keys)

                        # Check if already cached
                        if cache_key in st.session_state['analysis_cache']:
                            st.markdown("♻️ **Using cached result**")
                            log_msg = "♻️ Using cached result"
                            st.session_state['analysis_output']['logs'].append(log_msg)

                            task_msg = f"🧩 Tasks: {task_names}"
                            st.caption(task_msg)
                            st.session_state['analysis_output']['logs'].append(task_msg)

                            source_msg = f"📦 Source: {cache_key}"
                            st.caption(source_msg)
                            st.session_state['analysis_output']['logs'].append(source_msg)

                            analyzed_count += 1
                            progress_bar.progress(analyzed_count / unique_sources)
                            status_text.caption(f"Processing {analyzed_count}/{unique_sources} unique sources...")
                            st.session_state['analysis_output']['progress'] = analyzed_count
                            continue

                        # Get representative task
                        representative_task = next(t for t in st.session_state['job_tasks'] if t['task_key'] == task_keys[0])

                        st.markdown("---")
                        log_msg = f"🔍 **Analyzing new code source**"
                        st.markdown(log_msg)
                        st.session_state['analysis_output']['logs'].append(log_msg)

                        task_msg = f"🧩 Tasks: {task_names}"
                        st.caption(task_msg)
                        st.session_state['analysis_output']['logs'].append(task_msg)

                        source_msg = f"📦 Source: {cache_key}"
                        st.caption(source_msg)
                        st.session_state['analysis_output']['logs'].append(source_msg)

                        try:
                            code_context = db_service.get_task_code(representative_task)

                            if code_context and isinstance(code_context, dict) and "error.txt" not in code_context:
                                # Add job parameters
                                if job_params_input:
                                    current_meta = code_context.get("__metadata__", "")
                                    code_context["__metadata__"] = f"{current_meta}\nParameters: {job_params_input}"

                                # Analyze
                                mapping = agent.analyze_code(code_context)

                                # Cache result
                                st.session_state['analysis_cache'][cache_key] = mapping
                                result_msg = f"✅ Found {len(mapping.assets)} assets"
                                st.markdown(result_msg)
                                st.session_state['analysis_output']['logs'].append(result_msg)
                            else:
                                warning_msg = f"⚠️ Could not fetch code for {cache_key}"
                                st.warning(warning_msg)
                                st.session_state['analysis_output']['logs'].append(warning_msg)
                                st.session_state['analysis_cache'][cache_key] = None

                        except Exception as e:
                            error_msg = f"❌ Analysis failed: {e}"
                            st.error(error_msg)
                            st.session_state['analysis_output']['logs'].append(error_msg)
                            st.session_state['analysis_cache'][cache_key] = None

                        analyzed_count += 1
                        progress_bar.progress(analyzed_count / unique_sources)
                        status_text.caption(f"Processing {analyzed_count}/{unique_sources} unique sources...")
                        st.session_state['analysis_output']['progress'] = analyzed_count

                    # Step 3: Apply cached results to all tasks
                    st.markdown("\n📌 Applying results to tasks...")
                    st.session_state['analysis_output']['logs'].append("\n📌 Applying results to tasks...")

                    for task in st.session_state['job_tasks']:
                        task_key = task['task_key']
                        cache_key = task_to_cache_key[task_key]
                        mapping = st.session_state['analysis_cache'].get(cache_key)

                        if mapping and mapping.assets:
                            # Add task context to help differentiate
                            sources = []
                            targets = []

                            for asset in mapping.assets:
                                asset_dict = {
                                    'subtype': asset.subtype,
                                    'identifier': asset.identifier,
                                    'validation_status': '❔ Unchecked'
                                }
                                if asset.usage == "SOURCE":
                                    sources.append(asset_dict)
                                elif asset.usage == "TARGET":
                                    targets.append(asset_dict)

                            # Update session state
                            st.session_state['task_data'][task_key]['sources'] = sources if sources else [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked'}]
                            st.session_state['task_data'][task_key]['targets'] = targets if targets else [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked'}]

                    # Mark analysis as complete
                    summary_msg = f"✅ Analysis Complete! Processed {unique_sources} unique sources for {total_tasks} tasks."
                    st.success(summary_msg)
                    st.session_state['analysis_output']['status'] = 'complete'
                    st.session_state['analysis_output']['summary'] = summary_msg
                    st.session_state['analysis_running'] = False
                    st.session_state['last_completed_run'] = st.session_state['analysis_run_count']
                
                except Exception as e:
                    # Ensure analysis state is reset on error
                    st.error(f"❌ Analysis failed: {e}")
                    st.session_state['analysis_output']['status'] = 'error'
                    st.session_state['analysis_output']['logs'].append(f"❌ Fatal error: {e}")
                    st.session_state['analysis_running'] = False

        # Rerun to update the UI state
        st.rerun()

    # Display persisted analysis output (when not currently running)
    elif st.session_state.get('analysis_output') and st.session_state['analysis_output'].get('status') in ['complete', 'error']:
        # Expand if this is the most recently completed run
        is_recent_completion = st.session_state.get('last_completed_run', 0) == st.session_state.get('analysis_run_count', 0)

        with st.expander("🧠 AI Analysis Output", expanded=is_recent_completion):
            # Add scrollable container with max height
            analysis_container = st.container(height=400)

            with analysis_container:
                # Show all logs
                for log in st.session_state['analysis_output']['logs']:
                    if "---" in log:
                        st.markdown(log)  # Separator
                    elif "🧩 Tasks:" in log or "📦 Source:" in log:
                        st.caption(log)  # Task/source info as caption
                    elif "✅" in log or "🔍" in log or "📌" in log or "♻️" in log:
                        st.markdown(log)
                    elif "⚠️" in log:
                        st.warning(log.replace("⚠️", "").strip())
                    elif "❌" in log:
                        st.error(log.replace("❌", "").strip())
                    else:
                        st.markdown(log)

                # Show summary
                if st.session_state['analysis_output'].get('summary'):
                    st.success(st.session_state['analysis_output']['summary'])

# --- Step 2: Manual Lineage Entry ---
if st.session_state['job_tasks']:
    st.markdown("## 📝 Manual Lineage Entry")
    st.info("Review AI-suggested lineage or add sources/targets manually. Use 🗑️ to remove irrelevant tasks.")

    final_manifest = {}

    for task in st.session_state['job_tasks']:
        task_key = task['task_key']
        initialize_task_data(task_key)

        # Collapsible view per task with delete button
        col_expander, col_delete = st.columns([20, 1])

        with col_delete:
            # Add spacing to align with expander
            st.write("")
            if st.button("🗑️", key=f"delete_{task_key}", help="Remove this task"):
                # Track as excluded task
                if task_key not in st.session_state['excluded_tasks']:
                    st.session_state['excluded_tasks'].append(task_key)

                # Remove from job_tasks
                st.session_state['job_tasks'] = [
                    t for t in st.session_state['job_tasks']
                    if t['task_key'] != task_key
                ]

                # Clean up task data
                if task_key in st.session_state['task_data']:
                    del st.session_state['task_data'][task_key]
                if task_key in st.session_state['dq_rules']:
                    del st.session_state['dq_rules'][task_key]

                st.rerun()

        with col_expander:
            with st.expander(f"📌 {task_key} ({task['task_type']})", expanded=(task_key == st.session_state.get('expanded_task'))):

                # Sources Section
                st.markdown("### 🔽 Sources")
                sources_data = st.session_state['task_data'][task_key]['sources']

                # Create DataFrame with explicit columns and types to ensure stability
                sources_df = pd.DataFrame(
                    sources_data,
                    columns=['subtype', 'identifier', 'validation_status', 'load_type', 'filter_column']
                )
                
                # Fill NaN with empty strings for string columns; default status for validation
                sources_df[['subtype', 'identifier']] = sources_df[['subtype', 'identifier']].fillna("")
                sources_df['identifier'] = sources_df['identifier'].fillna("")
                sources_df['validation_status'] = sources_df['validation_status'].fillna('❔ Unchecked')
                sources_df['load_type'] = sources_df['load_type'].fillna('FULL_REFRESH')
                sources_df['filter_column'] = sources_df['filter_column'].fillna('')
                
                # Ensure string types to prevent type coercion issues
                sources_df = sources_df.astype({
                    'subtype': 'str', 
                    'identifier': 'str', 
                    'validation_status': 'str',
                    'load_type': 'str',
                    'filter_column': 'str'
                })

                edited_sources = st.data_editor(
                    sources_df,
                    num_rows="dynamic",
                    width="stretch",
                    key=f"sources_{task_key}",
                    column_config={
                        "validation_status": st.column_config.TextColumn(
                            "Status",
                            width="small",
                            help="Validation result",
                            default="❔ Unchecked"
                        ),
                        "subtype": st.column_config.SelectboxColumn(
                            "Type",
                            options=[
                                "UNITY_CATALOG_TABLE",
                                "HIVE_METASTORE_TABLE",
                                "ADLS",
                                "DBFS",
                                "PARQUET_FILE",
                                "CSV_FILE"
                            ],
                            required=False,
                            default="UNITY_CATALOG_TABLE",
                            width="medium"
                        ),
                        "identifier": st.column_config.TextColumn(
                            "Source Name",
                            required=False,
                            default="",
                            width="large",
                            help="Full path or table name"
                        ),
                        "load_type": st.column_config.SelectboxColumn(
                            "Load Type",
                            options=["FULL_REFRESH", "APPEND"],
                            required=True,
                            default="FULL_REFRESH",
                            width="medium",
                            help="How data is loaded. Use APPEND for incremental updates."
                        ),
                        "filter_column": st.column_config.TextColumn(
                            "Filter Col",
                            required=False,
                            default="",
                            width="medium",
                            help="Column to identify new data (e.g. run_id, date). Required for APPEND."
                        )
                    },
                    column_order=["validation_status", "subtype", "identifier", "load_type", "filter_column"],
                    disabled=["validation_status"],
                    hide_index=True
                )

                # Targets Section
                st.markdown("### 🎯 Targets")
                targets_data = st.session_state['task_data'][task_key]['targets']
                # Create DataFrame with explicit columns and types to ensure stability
                targets_df = pd.DataFrame(
                    targets_data,
                    columns=['subtype', 'identifier', 'validation_status', 'load_type', 'filter_column']
                )
                # Fill NaN with empty strings for string columns, default status for validation
                targets_df['subtype'] = targets_df['subtype'].fillna("")
                targets_df['identifier'] = targets_df['identifier'].fillna("")
                targets_df['validation_status'] = targets_df['validation_status'].fillna('❔ Unchecked')
                targets_df['load_type'] = targets_df['load_type'].fillna('FULL_REFRESH')
                targets_df['filter_column'] = targets_df['filter_column'].fillna('')
                
                # Ensure string types to prevent type coercion issues
                targets_df = targets_df.astype({
                    'subtype': 'str', 
                    'identifier': 'str', 
                    'validation_status': 'str',
                    'load_type': 'str',
                    'filter_column': 'str'
                })

                edited_targets = st.data_editor(
                    targets_df,
                    num_rows="dynamic",
                    width="stretch",
                    key=f"targets_{task_key}",
                    column_config={
                        "validation_status": st.column_config.TextColumn(
                            "Status",
                            width="small",
                            help="Validation result",
                            default="❔ Unchecked"
                        ),
                        "subtype": st.column_config.SelectboxColumn(
                            "Type",
                            options=[
                                "UNITY_CATALOG_TABLE",
                                "HIVE_METASTORE_TABLE",
                                "ADLS",
                                "DBFS",
                                "PARQUET_FILE",
                                "CSV_FILE"
                            ],
                            required=False,
                            default="UNITY_CATALOG_TABLE",
                            width="medium"
                        ),
                        "identifier": st.column_config.TextColumn(
                            "Target Name",
                            required=False,
                            default="",
                            width="large",
                            help="Full path or table name"
                        ),
                        "load_type": st.column_config.SelectboxColumn(
                            "Load Type",
                            options=["FULL_REFRESH", "APPEND"],
                            required=True,
                            default="FULL_REFRESH",
                            width="medium",
                            help="How data is loaded. Use APPEND for incremental updates."
                        ),
                        "filter_column": st.column_config.TextColumn(
                            "Filter Col",
                            required=False,
                            default="",
                            width="medium",
                            help="Column to identify new data (e.g. run_id, date). Required for APPEND."
                        )
                    },
                    column_order=["validation_status", "subtype", "identifier", "load_type", "filter_column"],
                    disabled=["validation_status"],
                    hide_index=True
                )

                # Update session state with edited data
                # Process and normalize the data before saving
                # Use validation cache to restore status for previously validated assets
                new_sources = []
                for row in edited_sources.to_dict('records'):
                    subtype = row.get('subtype', '') if pd.notna(row.get('subtype')) else ''
                    identifier = row.get('identifier', '') if pd.notna(row.get('identifier')) else ''

                    # Check validation cache for this asset
                    composite_key = f"{identifier.strip()}|{subtype}" if identifier.strip() else ""
                    cached_status = st.session_state.get('validation_cache', {}).get(composite_key) if composite_key else None

                    # Use cached status if available, otherwise default to Unchecked
                    validation_status = cached_status if cached_status else '❔ Unchecked'

                    normalized_row = {
                        'subtype': subtype,
                        'identifier': identifier,
                        'validation_status': validation_status,
                        'load_type': row.get('load_type', 'FULL_REFRESH'),
                        'filter_column': row.get('filter_column', '') if row.get('load_type') == 'APPEND' else ''
                    }
                    new_sources.append(normalized_row)

                new_targets = []
                for row in edited_targets.to_dict('records'):
                    subtype = row.get('subtype', '') if pd.notna(row.get('subtype')) else ''
                    identifier = row.get('identifier', '') if pd.notna(row.get('identifier')) else ''
                    load_type = row.get('load_type', 'FULL_REFRESH') if pd.notna(row.get('load_type')) else 'FULL_REFRESH'
                    # CLEANUP: If full refresh, clear filter column
                    filter_column = row.get('filter_column', '') if pd.notna(row.get('filter_column')) and load_type == 'APPEND' else ''

                    # Check validation cache for this asset
                    composite_key = f"{identifier.strip()}|{subtype}" if identifier.strip() else ""
                    cached_status = st.session_state.get('validation_cache', {}).get(composite_key) if composite_key else None

                    # Use cached status if available, otherwise default to Unchecked
                    validation_status = cached_status if cached_status else '❔ Unchecked'

                    normalized_row = {
                        'subtype': subtype,
                        'identifier': identifier,
                        'validation_status': validation_status,
                        'load_type': load_type,
                        'filter_column': filter_column
                    }
                    new_targets.append(normalized_row)

                # Ensure at least one empty row if list is empty
                if not new_sources:
                    new_sources = [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked', 'load_type': 'FULL_REFRESH', 'filter_column': ''}]

                if not new_targets:
                    new_targets = [{'subtype': '', 'identifier': '', 'validation_status': '❔ Unchecked', 'load_type': 'FULL_REFRESH', 'filter_column': ''}]

                st.session_state['task_data'][task_key]['sources'] = new_sources
                st.session_state['task_data'][task_key]['targets'] = new_targets

                # Collect all asset names for DQ rules (skip empty rows)
                all_assets = []
                for src in new_sources:
                    ident = src.get('identifier', '').strip()
                    if ident:
                        all_assets.append(ident)

                for tgt in new_targets:
                    ident = tgt.get('identifier', '').strip()
                    if ident:
                        all_assets.append(ident)

                # --- Data Quality Rules UI ---
                """ # DISABLED: Data Quality Rules UI
                st.markdown("#### 🧪 Data Quality Rules")

                dq_col1, dq_col2 = st.columns([1, 2])

                with dq_col1:
                    selected_asset_for_dq = st.selectbox(
                        "Select Asset to Add Rules",
                        options=["Select Asset..."] + sorted(list(set(all_assets))),
                        key=f"dq_sel_{task_key}",
                        on_change=set_active_task,
                        args=(task_key,)
                    )

                with dq_col2:
                    if selected_asset_for_dq and selected_asset_for_dq != "Select Asset...":
                        st.markdown("#### ➕ Add New Rule")

                        # Fetch columns
                        asset_columns = [""]
                        if selected_asset_for_dq in st.session_state['column_cache']:
                            asset_columns.extend(st.session_state['column_cache'][selected_asset_for_dq])
                        else:
                            try:
                                config_cols = DatabricksService().get_asset_columns(selected_asset_for_dq)
                                if config_cols:
                                    st.session_state['column_cache'][selected_asset_for_dq] = config_cols
                                    asset_columns.extend(config_cols)
                            except Exception:
                                pass

                        fr_c1, fr_c2, fr_c3 = st.columns(3)
                        r_cols = fr_c1.multiselect(
                            "Column Name(s)",
                            options=asset_columns,
                            help="Select one or more columns.",
                            default=None,
                            key=f"dqc_{task_key}_{selected_asset_for_dq}"
                        )

                        r_type = fr_c2.selectbox(
                            "Check Type",
                            [
                                "not_null",
                                "unique",
                                "row_count",
                                "range",
                                "accepted_values",
                                "regex"
                            ],
                            key=f"dqt_{task_key}_{selected_asset_for_dq}"
                        )

                        help_text = "Value/Param"
                        if r_type == "range":
                            help_text = "Format: min-max (e.g. 0-100)"
                        elif r_type == "accepted_values":
                            help_text = "Format: A,B,C"
                        elif r_type == "regex":
                            help_text = "Regular Expression Pattern"
                        elif r_type == "row_count":
                            help_text = "Minimum Row Count (Integer)"
                        elif r_type == "unique" or r_type == "not_null":
                            help_text = "Leave empty (Not needed)"

                        r_val = fr_c3.text_input(
                            "Value/Param",
                            help=help_text,
                            placeholder=help_text,
                            key=f"dqv_{task_key}_{selected_asset_for_dq}"
                        )

                        if st.button("Save Rule", key=f"btn_save_{task_key}_{selected_asset_for_dq}"):
                            if not r_cols:
                                st.error("❌ Please select at least one column.")
                            else:
                                for r_col in r_cols:
                                    is_valid = True
                                    err_msg = ""

                                    if r_col.strip() == "*" and r_type != "row_count":
                                        is_valid = False
                                        err_msg = f"❌ '*' selection can only be used with 'row_count' (Invalid for {r_type})."

                                    if is_valid and r_type == "row_count" and not r_val.isdigit():
                                        is_valid = False
                                        err_msg = "❌ Row Count value must be an integer."

                                    if is_valid and r_type == "range" and ("-" not in r_val or len(r_val.split("-")) != 2):
                                        is_valid = False
                                        err_msg = "❌ Range must be 'min-max'."

                                    if not is_valid:
                                        st.error(err_msg)
                                        break
                                    else:
                                        new_rule = {"column": r_col, "type": r_type}
                                        if r_val:
                                            if r_type == "range":
                                                parts = r_val.split("-")
                                                new_rule["min"] = parts[0].strip()
                                                new_rule["max"] = parts[1].strip()
                                            elif r_type == "accepted_values":
                                                new_rule["values"] = [x.strip() for x in r_val.split(",")]
                                            elif r_type == "unique" or r_type == "not_null":
                                                pass
                                            else:
                                                new_rule["value"] = r_val

                                        if selected_asset_for_dq not in st.session_state['dq_rules']:
                                            st.session_state['dq_rules'][selected_asset_for_dq] = []
                                        st.session_state['dq_rules'][selected_asset_for_dq].append(new_rule)

                                st.session_state['expanded_task'] = task_key
                                st.rerun()

                        # Show existing rules
                        current_rules = st.session_state['dq_rules'].get(selected_asset_for_dq, [])
                        rules_count = len(current_rules)

                        st.markdown(f"📋 **Current Rules ({rules_count})**")
                        with st.container(height=200):
                            if current_rules:
                                for r_idx, rule in enumerate(current_rules):
                                    r_col_a, r_col_b = st.columns([5, 1])
                                    r_col_a.info(f"**{rule.get('column', '')}** : {rule.get('type')} | {rule.get('value', '')}")
                                    if r_col_b.button("🗑️", key=f"del_rule_{task_key}_{selected_asset_for_dq}_{r_idx}"):
                                        st.session_state['dq_rules'][selected_asset_for_dq].pop(r_idx)
                                        st.session_state['expanded_task'] = task_key
                                        st.rerun()
                            else:
                                st.caption("No rules configured for this asset yet.")
                """

                # Build manifest entry for this task (skip empty rows)
                sources_manifest = []
                targets_manifest = []

                for src in new_sources:
                    ident = src.get('identifier', '').strip()
                    if ident:
                        sources_manifest.append({
                            "name": ident,
                            "type": src.get('subtype', 'UNKNOWN') or 'UNKNOWN',
                            "load_type": src.get('load_type', 'FULL_REFRESH'),
                            "filter_column": src.get('filter_column', '')
                        })

                for tgt in new_targets:
                    ident = tgt.get('identifier', '').strip()
                    if ident:
                        targets_manifest.append({
                            "name": ident,
                            "type": tgt.get('subtype', 'UNKNOWN') or 'UNKNOWN',
                            "load_type": tgt.get('load_type', 'FULL_REFRESH'),
                            "filter_column": tgt.get('filter_column', '')
                        })

                # DQ rules for this task
                dq_section_manifest = {}
                for asset_name in all_assets:
                    if asset_name in st.session_state['dq_rules'] and st.session_state['dq_rules'][asset_name]:
                        dq_section_manifest[asset_name] = st.session_state['dq_rules'][asset_name]

                final_manifest[task_key] = {
                    "sources": sources_manifest,
                    "targets": targets_manifest,
                    "dq_rules": dq_section_manifest
                }

    # --- Step 3: Validate & Save ---
    st.markdown("---")
    st.markdown("## ✅ Validate & Save")

    # Execute validation at full width (outside columns)
    if st.session_state.get('trigger_validation'):
        st.session_state['trigger_validation'] = False

        validation_error_count = 0

        with st.status("Validating Assets...", expanded=True) as status:
            # Collect all unique assets to validate
            # Use composite key: identifier + subtype (since same table can be source AND target)
            assets_to_validate = {}

            for t_key, t_data in st.session_state['task_data'].items():
                for src in t_data.get('sources', []):
                    ident = src.get('identifier', '').strip()
                    subtype = src.get('subtype', 'UNKNOWN')
                    # Skip empty rows
                    if not ident:
                        continue

                    # Composite key: identifier|subtype (subtype affects validation logic)
                    composite_key = f"{ident}|{subtype}"
                    if composite_key not in assets_to_validate:
                        assets_to_validate[composite_key] = {
                            'identifier': ident,
                            'subtype': subtype
                        }

                for tgt in t_data.get('targets', []):
                    ident = tgt.get('identifier', '').strip()
                    subtype = tgt.get('subtype', 'UNKNOWN')
                    # Skip empty rows
                    if not ident:
                        continue

                    composite_key = f"{ident}|{subtype}"
                    if composite_key not in assets_to_validate:
                        assets_to_validate[composite_key] = {
                            'identifier': ident,
                            'subtype': subtype
                        }
            
            all_assets_to_validate = list(assets_to_validate.values())

            # Check if there are sources and targets separately
            has_valid_sources = False
            has_valid_targets = False
            for t_key, t_data in st.session_state['task_data'].items():
                for src in t_data.get('sources', []):
                    if src.get('identifier', '').strip():
                        has_valid_sources = True
                        break
                for tgt in t_data.get('targets', []):
                    if tgt.get('identifier', '').strip():
                        has_valid_targets = True
                        break

            # Check if there are any assets to validate
            if len(all_assets_to_validate) == 0:
                status.update(label="No assets to validate", state="error", expanded=False)
                st.session_state['validation_result'] = {
                    'type': 'error',
                    'message': "❌ No assets to validate. Please add at least one source and one target."
                }
                st.rerun()

            # Check if both sources and targets exist
            if not has_valid_sources or not has_valid_targets:
                missing = []
                if not has_valid_sources:
                    missing.append("source")
                if not has_valid_targets:
                    missing.append("target")

                status.update(label="Missing required assets", state="error", expanded=False)
                st.session_state['validation_result'] = {
                    'type': 'error',
                    'message': f"❌ At least one valid {' and '.join(missing)} is required."
                }
                st.rerun()

            # Filter to only validate assets that are not already cached
            assets_needing_validation = []
            cached_results = []
            for asset in all_assets_to_validate:
                composite_key = f"{asset['identifier']}|{asset['subtype']}"
                cached_status = sts.session_state.get('validation_cache', {}).get(composite_key)
                if cached_status is None:
                    assets_needing_validation.append(asset)
                else:
                    cached_results[composite_key] = cached_status

            st.write(f"Validating {len(assets_needing_validation)} new assets ({len(cached_results)} from cache)...")


            # Call backend only for uncached assets
            validation_results = dict(cached_results)  # Start with cached results
            if assets_needing_validation:
                db_service = DatabricksService()
                new_results = db_service.validate_assets(assets_needing_validation)
                validation_results.update(new_results)

                # Store new results in cache
                for cache_key, cache_status in new_results.items():
                    st.session_state['validation_cache'][cache_key] = cache_status

            # Update status counts
            valid_count = 0
            invalid_count = 0

            # Update each task's sources and targets using the validation results
            for t_key in list(st.session_state['task_data'].keys()):
                # Update sources
                for idx in range(len(st.session_state['task_data'][t_key].get('sources', []))):
                    src = st.session_state['task_data'][t_key]['sources'][idx]
                    identifier = src.get('identifier', '').strip()
                    subtype = src.get('subtype', 'UNKNOWN')
                    # Skip empty rows
                    if not identifier:
                        continue

                    # Use composite key to lookup result
                    composite_key = f"{identifier}|{subtype}"
                    status_msg = validation_results.get(composite_key, '❓ Unchecked')
                    st.session_state['task_data'][t_key]['sources'][idx]['validation_status'] = status_msg

                    if "✅" in status_msg:
                        valid_count += 1
                    elif "❌" in status_msg or "⚠️" in status_msg:
                        invalid_count += 1

                # Update targets
                for idx in range(len(st.session_state['task_data'][t_key].get('targets', []))):
                    tgt = st.session_state['task_data'][t_key]['targets'][idx]
                    identifier = tgt.get('identifier', '').strip()
                    subtype = tgt.get('subtype', 'UNKNOWN')
                    # Skip empty rows
                    if not identifier:
                        continue

                    # Use composite key to lookup result
                    composite_key = f"{identifier}|{subtype}"
                    status_msg = validation_results.get(composite_key, '❓ Unchecked')
                    st.session_state['task_data'][t_key]['targets'][idx]['validation_status'] = status_msg

                    if "✅" in status_msg:
                        valid_count += 1
                    elif "❌" in status_msg or "⚠️" in status_msg:
                        invalid_count += 1

            validation_error_count = invalid_count
            status.update(label=f"Done: {valid_count} Valid, {invalid_count} Issues.", state="complete", expanded=False)

        # Store result in session state
        if validation_error_count > 0:
            st.session_state['validation_result'] = {
                'type': 'error',
                'message': f"⚠️ Validation found **{validation_error_count} issues**. Please review before submitting."
            }
        else:
            st.session_state['validation_result'] = {
                'type': 'success',
                'message': "✅ All assets validated successfully!"
        }

        st.rerun()

    # Display persistent validation result BEFORE buttons
    if st.session_state.get('validation_result'):
        result = st.session_state['validation_result']
        if result['type'] == 'error':
            st.error(result['message'])
        else:
            st.success(result['message'])

    # Display save/submit result messages (full width, after validation result)
    if st.session_state.get('save_submit_message'):
        msg = st.session_state['save_submit_message']
        if msg['type'] == 'success':
            st.success(msg['message'])
        elif msg['type'] == 'error':
            st.error(msg['message'])
        elif msg['type'] == 'info':
            st.info(msg['message'])

    # Note: Don't clear here - it causes re-render flickering
    # Message will be cleared on next save/submit action

    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 3])

    with col_btn1:
        if st.button("🔍 Validate Assets", type="secondary"):
            # Clear any previous save/submit message
            st.session_state['save_submit_message'] = None
            st.session_state['trigger_validation'] = True
            st.rerun()

    with col_btn2:
        if st.button("💾 Save Draft"):
            # Clear any previous message
            st.session_state['save_submit_message'] = None

            # Add metadata to manifest
            final_manifest['metadata'] = {
                'excluded_tasks': st.session_state.get('excluded_tasks', []),
                'job_task_snapshot': st.session_state.get('job_task_snapshot', [])
            }

            output_path = "manifest.json"
            with open(output_path, "w") as f:
                json.dump(final_manifest, f, indent=2)

            # Save to Delta with DRAFT status
            if manifest_table:
                with st.spinner(f"Saving draft to '{manifest_table}'..."):
                    db_service = DatabricksService()
                    current_job_id = st.session_state.get('current_job_id', '') or 'unknown'
                    save_res = db_service.save_manifest_to_table(
                        table_name=manifest_table,
                        manifest=final_manifest,
                        job_id=current_job_id,
                        version="auto",
                        status="DRAFT"
                    )

                    if "✅" in save_res:
                        st.session_state['save_submit_message'] = {
                            'type': 'success',
                            'message': f"✅ Draft saved to '{output_path}' and '{manifest_table}'"
                        }
                    else:
                        st.session_state['save_submit_message'] = {
                            'type': 'error',
                            'message': save_res
                        }
            else:
                st.session_state['save_submit_message'] = {
                    'type': 'success',
                    'message': f"✅ Draft saved to '{output_path}'"
                }

            st.rerun()

    with col_btn3:
        # Check if there are validation errors (asset validation failed)
        validation_result = st.session_state.get('validation_result')
        has_validation_errors = validation_result is not None and validation_result.get('type') == 'error'

        if st.button("🚀 Submit", type="primary", disabled=has_validation_errors,
             help="Fix validation errors before submitting" if has_validation_errors else None):
            # Clear any previous message
            st.session_state['save_submit_message'] = None

            # Step 1: Validate field completeness
            validation_errors = validate_manifest_completeness()

            if validation_errors:
                # Show error summary
                error_count = sum(len(errors) for errors in validation_errors.values())
                st.error(f"❌ Cannot submit: {len(validation_errors)} task(s) have incomplete data ({error_count} issues)")

                # Display errors
                with st.expander("🧪 Validation Errors", expanded=True):
                    for task_key, errors in validation_errors.items():
                        st.markdown(f"**📌 {task_key}** ⚠️")
                        for error in errors:
                            st.markdown(f"- {error}")

                # Auto-expand tasks with errors
                first_error_task = list(validation_errors.keys())[0]
                st.session_state['expanded_task'] = first_error_task
                st.session_state['validation_errors'] = validation_errors
                st.rerun()

            else:
                # Step 2: Check if all assets are validated
                has_unvalidated = False
                for t_key, t_data in st.session_state['task_data'].items():
                    for asset in t_data.get('sources', []) + t_data.get('targets', []):
                        if asset.get('identifier', '') and "?" in asset.get('validation_status', ''):
                            has_unvalidated = True
                            break

                if has_unvalidated:
                    st.session_state['save_submit_message'] = {
                        'type': 'error',
                        'message': '❌ Please validate all assets before submitting.'
                    }
                    st.rerun()
                else:
                    # Add metadata to manifest
                    final_manifest["_metadata"] = {
                        'excluded_tasks': st.session_state.get('excluded_tasks', []),
                        'job_task_snapshot': st.session_state.get('job_task_snapshot', [])
                    }

                    # Step 3: Check for changes if manifest was loaded
                    has_changes = True
                    if st.session_state.get('loaded_manifest_version') and st.session_state.get('existing_manifest'):
                        loaded_manifest = st.session_state['existing_manifest']['manifest_data']

                        # Compare manifests (excluding _metadata for comparison)
                        current_manifest_copy = {k: v for k, v in final_manifest.items() if k != '_metadata'}
                        loaded_manifest_copy = {k: v for k, v in loaded_manifest.items() if k != '_metadata'}

                        if current_manifest_copy == loaded_manifest_copy:
                            has_changes = False

                    if not has_changes:
                        # No changes detected
                        st.session_state['save_submit_message'] = {
                            'type': 'info',
                            'message': f"ℹ️ **No changes detected** since v{st.session_state.get('loaded_manifest_version')}. Nothing to submit."
                        }

                        # Still save local JSON
                        output_path = "manifest.json"
                        with open(output_path, "w") as f:
                            json.dump(final_manifest, f, indent=2)
                        st.rerun()

                    else:
                        # Changes detected, proceed with submit
                        output_path = "manifest.json"
                        with open(output_path, "w") as f:
                            json.dump(final_manifest, f, indent=2)

                        # Save to Delta with SUBMITTED status
                        if manifest_table:
                            with st.spinner(f"Submitting to '{manifest_table}'..."):
                                db_service = DatabricksService()
                                current_job_id = st.session_state.get('current_job_id', '') or 'unknown'
                                save_res = db_service.save_manifest_to_table(
                                                table_name=manifest_table,
                                                manifest=final_manifest,
                                                job_id=current_job_id,
                                                version="auto",
                                                status="SUBMITTED"
                                )

                                if "✅" in save_res:
                                    st.session_state['save_submit_message'] = {'type': 'success', 'message': f"✅ Manifest submitted to '{manifest_table}'"}
                                else:
                                    st.session_state['save_submit_message'] = {'type': 'error', 'message': save_res}
                        else:
                            st.session_state['save_submit_message'] = {'type': 'success', 'message': f"✅ Manifest submitted and saved to '{output_path}'"}
                        st.rerun()
    # View JSON
    with st.expander("View Generated Manifest JSON"):
        st.json(final_manifest)
