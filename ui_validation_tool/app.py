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
job_params_input, manifest_table, manifest_version_input = render_sidebar(config)

# --- Main Content ---
st.title("🔍 Agentic RCA Validator")
st.markdown("Generate and validate Unity Catalog lineage mappings for your Databricks Jobs.")

# Initialize Session State
initialize_session_state()

# --- Step 1: Job Configuration ---
with st.expander("1️⃣ Job Configuration", expanded=True):
    job_id_input = st.text_input("Databricks Job ID", help="The numeric ID of your Databricks Job")

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
    st.markdown("### 📋 Existing Manifest Found")
    
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
            change_msg = f"✅ Loaded manifest v{manifest_info['version']}"
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
    
    # Track if analysis has been run
    if 'analysis_run_count' not in st.session_state:
        st.session_state['analysis_run_count'] = 0
    
    with col_analyze:
        # Show warning if analysis was already run
        if st.session_state['analysis_run_count'] > 0:
            st.warning(f"⚠️ Analysis already run {st.session_state['analysis_run_count']} time(s). Re-running will incur additional AI costs.")
            
            col_confirm, col_cancel = st.columns(2)
            with col_confirm:
                run_analysis = st.button("🔄 Re-run Analysis", type="secondary")
            with col_cancel:
                if st.button("❌ Cancel"):
                    st.info("Analysis cancelled. Using existing results.")
                    run_analysis = False
        else:
            run_analysis = st.button("🤖 Analyze All Tasks", type="primary")
        
        if run_analysis:
            st.session_state['analysis_run_count'] += 1

            with st.status("Running AI Analysis on All Tasks...", expanded=True) as status:
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
                
                st.write(f"📊 Analyzing **{unique_sources} unique code sources** for **{total_tasks} tasks**")
                
                # Step 2: Analyze each unique code source once
                progress_bar = st.progress(0)
                analyzed_count = 0
                
                for cache_key, task_keys in analysis_groups.items():
                    # Check if already cached
                    if cache_key in st.session_state['analysis_cache']:
                        st.write(f"✅ Using cached result for: {cache_key}")
                        analyzed_count += 1
                        progress_bar.progress(analyzed_count / unique_sources)
                        continue
                    
                    # Get representative task
                    representative_task = next(t for t in st.session_state['job_tasks'] if t['task_key'] == task_keys[0])
                    
                    st.write(f"🔍 Analyzing: {cache_key} (used by {len(task_keys)} tasks)")
                    
                    try:
                        code_context = db_service.get_task_code(representative_task)
                        
                        if code_context and isinstance(code_context, dict) and "error.txt" not in code_context:
                            # Add job parameters
                            if job_params_input:
                                current_meta = code_context.get("__metadata__", "")
                                code_context["__metadata__"] = f"{current_meta}\\nParameters: {job_params_input}"
                            
                            # Analyze
                            mapping = agent.analyze_code(code_context)
                            
                            # Cache result
                            st.session_state['analysis_cache'][cache_key] = mapping
                            st.write(f"  ✅ Found {len(mapping.assets)} assets")
                        else:
                            st.warning(f"  ⚠️ Could not fetch code for {cache_key}")
                            st.session_state['analysis_cache'][cache_key] = None
                    
                    except Exception as e:
                        st.error(f"  ❌ Analysis failed: {e}")
                        st.session_state['analysis_cache'][cache_key] = None
                    
                    analyzed_count += 1
                    progress_bar.progress(analyzed_count / unique_sources)
                
                # Step 3: Apply cached results to all tasks
                st.write("\\n📝 Applying results to tasks...")
                
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
                
                status.update(label=f"✅ Analysis Complete! Processed {unique_sources} unique sources for {total_tasks} tasks.", state="complete", expanded=False)
                st.rerun()
    
    with col_info:
        st.info("💡 **Tip**: AI will intelligently cache shared wheel files to avoid redundant analysis.")

# --- Step 2: Manual Lineage Entry ---
if st.session_state['job_tasks']:
    st.markdown("### 2️⃣ Manual Lineage Entry")
    st.info("Review AI-suggested lineage or add sources/targets manually. Use 🗑️ to remove irrelevant tasks.")
    
    final_manifest = {}
    
    for task in st.session_state['job_tasks']:
        task_key = task['task_key']
        initialize_task_data(task_key)
        
        # Collapsible view per task with delete button
        col_expander, col_delete = st.columns([20, 1])
        
        with col_delete:
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
                st.markdown("#### 📥 Sources")
                sources_df = pd.DataFrame(st.session_state['task_data'][task_key]['sources'])
                
                edited_sources = st.data_editor(
                    sources_df,
                    num_rows="dynamic",
                    use_container_width=True,
                    key=f"sources_{task_key}",
                    column_config={
                        "validation_status": st.column_config.TextColumn("Status", width="small", help="Validation result"),
                        "subtype": st.column_config.SelectboxColumn(
                            "Type",
                            options=[
                                "UNITY_CATALOG_TABLE", "HIVE_METASTORE_TABLE", "JDBC_DB", 
                                "ADLS", "S3", "GCS", "DBFS", "LOCAL_FILE", 
                                "PARQUET_FILE", "CSV_FILE", "DELTA_PATH", "UNKNOWN"
                            ],
                            required=True,
                            width="medium"
                        ),
                        "identifier": st.column_config.TextColumn(
                            "Source Name",
                            required=True,
                            width="large",
                            help="Full path or table name"
                        )
                    },
                    column_order=["validation_status", "subtype", "identifier"],
                    on_change=set_active_task,
                    args=(task_key,)
                )
                
                # Targets Section
                st.markdown("#### 📤 Targets")
                targets_df = pd.DataFrame(st.session_state['task_data'][task_key]['targets'])
                
                edited_targets = st.data_editor(
                    targets_df,
                    num_rows="dynamic",
                    use_container_width=True,
                    key=f"targets_{task_key}",
                    column_config={
                        "validation_status": st.column_config.TextColumn("Status", width="small", help="Validation result"),
                        "subtype": st.column_config.SelectboxColumn(
                            "Type",
                            options=[
                                "UNITY_CATALOG_TABLE", "HIVE_METASTORE_TABLE", "JDBC_DB", 
                                "ADLS", "S3", "GCS", "DBFS", "LOCAL_FILE", 
                                "PARQUET_FILE", "CSV_FILE", "DELTA_PATH", "UNKNOWN"
                            ],
                            required=True,
                            width="medium"
                        ),
                        "identifier": st.column_config.TextColumn(
                            "Target Name",
                            required=True,
                            width="large",
                            help="Full path or table name"
                        )
                    },
                    column_order=["validation_status", "subtype", "identifier"],
                    on_change=set_active_task,
                    args=(task_key,)
                )
                
                # Update session state with edited data
                st.session_state['task_data'][task_key]['sources'] = edited_sources.to_dict('records')
                st.session_state['task_data'][task_key]['targets'] = edited_targets.to_dict('records')
                
                # Collect all asset names for DQ rules
                all_assets = []
                for src in edited_sources.to_dict('records'):
                    if src.get('identifier'):
                        all_assets.append(src['identifier'])
                for tgt in edited_targets.to_dict('records'):
                    if tgt.get('identifier'):
                        all_assets.append(tgt['identifier'])
                
                # --- Data Quality Rules UI ---
                st.markdown("#### 🛡️ Data Quality Rules")
                
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
                        st.markdown("##### ➕ Add New Rule")
                        
                        # Fetch columns
                        asset_columns = ["*"]
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
                        r_cols = fr_c1.multiselect("Column Name(s)", options=asset_columns, help="Select one or more columns.", default=None, key=f"dqc_{task_key}_{selected_asset_for_dq}")
                        r_type = fr_c2.selectbox("Check Type", [
                            "not_null", "unique", "row_count", 
                            "range", "accepted_values", "regex"
                        ], key=f"dqt_{task_key}_{selected_asset_for_dq}")
                        
                        help_text = "Value/Param"
                        if r_type == "range": help_text = "Format: min-max (e.g. 0-100)"
                        elif r_type == "accepted_values": help_text = "Format: A,B,C"
                        elif r_type == "regex": help_text = "Regular Expression Pattern"
                        elif r_type == "row_count": help_text = "Minimum Row Count (Integer)"
                        elif r_type == "unique" or r_type == "not_null": help_text = "Leave empty (Not needed)"

                        r_val = fr_c3.text_input("Value/Param", help=help_text, placeholder=help_text, key=f"dqv_{task_key}_{selected_asset_for_dq}")
                        
                        if st.button("Save Rule", key=f"btn_save_{task_key}_{selected_asset_for_dq}"):
                            if not r_cols:
                                st.error("❌ Please select at least one column.")
                            else:
                                for r_col in r_cols:
                                    is_valid = True
                                    err_msg = ""
                                    if r_col.strip() == "*" and r_type != "row_count":
                                        is_valid = False
                                        err_msg = f"❌ '*' selector can only be used with 'row_count' (Invalid for {r_type})."
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
                                                parts = r_val.split('-')
                                                new_rule["min"] = parts[0].strip()
                                                new_rule["max"] = parts[1].strip()
                                            elif r_type == "accepted_values":
                                                new_rule["values"] = [x.strip() for x in r_val.split(',')]
                                            elif r_type == "unique" or r_type == "not_null": pass 
                                            else: new_rule["value"] = r_val
                                        
                                        if selected_asset_for_dq not in st.session_state['dq_rules']:
                                            st.session_state['dq_rules'][selected_asset_for_dq] = []
                                        st.session_state['dq_rules'][selected_asset_for_dq].append(new_rule)
                                
                                st.session_state['expanded_task'] = task_key
                                st.rerun()

                        # Show existing rules
                        current_rules = st.session_state['dq_rules'].get(selected_asset_for_dq, [])
                        rules_count = len(current_rules)
                        
                        st.markdown(f"**📜 Current Rules ({rules_count})**")
                        with st.container(height=200):
                            if current_rules:
                                for ridx, rule in enumerate(current_rules):
                                    r_col_a, r_col_b = st.columns([5, 1])
                                    r_col_a.info(f"**{rule.get('column','*')}** | `{rule['type']}` | {rule.get('value', '')}")
                                    if r_col_b.button("🗑️", key=f"del_rule_{task_key}_{selected_asset_for_dq}_{ridx}"):
                                        st.session_state['dq_rules'][selected_asset_for_dq].pop(ridx)
                                        st.session_state['expanded_task'] = task_key
                                        st.rerun()
                            else:
                                st.caption("No rules configured for this asset yet.")

                # Build manifest entry for this task
                sources_manifest = []
                targets_manifest = []
                
                for src in edited_sources.to_dict('records'):
                    if src.get('identifier'):
                        sources_manifest.append({
                            "name": src['identifier'],
                            "type": src.get('subtype', 'UNKNOWN')
                        })
                
                for tgt in edited_targets.to_dict('records'):
                    if tgt.get('identifier'):
                        targets_manifest.append({
                            "name": tgt['identifier'],
                            "type": tgt.get('subtype', 'UNKNOWN')
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
    st.markdown("### 3️⃣ Validate & Save")
    
    col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 3])
    
    with col_btn1:
        if st.button("🔎 Validate Assets", type="secondary"):
            validation_error_count = 0
            
            with st.status("Validating Assets...", expanded=True) as status:
                # Collect all assets
                all_assets_to_validate = []
                for t_key, t_data in st.session_state['task_data'].items():
                    for src in t_data.get('sources', []):
                        if src.get('identifier'):
                            all_assets_to_validate.append(src)
                    for tgt in t_data.get('targets', []):
                        if tgt.get('identifier'):
                            all_assets_to_validate.append(tgt)
                
                st.write(f"Validating {len(all_assets_to_validate)} assets...")
                
                # Call backend
                db_service = DatabricksService()
                validation_results = db_service.validate_assets(all_assets_to_validate)
                
                # Update status
                valid_count = 0
                invalid_count = 0
                
                for t_key, t_data in st.session_state['task_data'].items():
                    for asset in t_data.get('sources', []) + t_data.get('targets', []):
                        ident = asset.get('identifier')
                        if ident:
                            status_msg = validation_results.get(ident, "❔ Unchecked")
                            asset['validation_status'] = status_msg
                            
                            if "✅" in status_msg: 
                                valid_count += 1
                            else:
                                invalid_count += 1
                
                validation_error_count = invalid_count
                status.update(label=f"Done! {valid_count} Valid, {invalid_count} Issues.", state="complete", expanded=False)
            
            if validation_error_count > 0:
                st.error(f"⚠️ Validation found **{validation_error_count} issues**. Please review before submitting.")
            else:
                st.success("✅ All assets validated successfully!")
            
            st.rerun()
    
    with col_btn2:
        if st.button("💾 Save Draft"):
            # Add metadata to manifest
            final_manifest['_metadata'] = {
                'excluded_tasks': st.session_state.get('excluded_tasks', []),
                'job_task_snapshot': st.session_state.get('job_task_snapshot', [])
            }
            
            output_path = "manifest.json"
            with open(output_path, "w") as f:
                json.dump(final_manifest, f, indent=2)
            
            st.success(f"✅ Draft saved to `{output_path}`")
            
            # Save to Delta with DRAFT status
            if manifest_table:
                with st.spinner(f"Saving draft to `{manifest_table}`..."):
                    db_service = DatabricksService()
                    save_res = db_service.save_manifest_to_table(
                        table_name=manifest_table,
                        manifest=final_manifest,
                        job_id=job_id_input if job_id_input else "unknown",
                        version=manifest_version_input,
                        status="DRAFT"
                    )
                    if "✅" in save_res:
                        st.toast(save_res, icon="✅")
                    else:
                        st.error(save_res)
    
    with col_btn3:
        if st.button("✅ Submit", type="primary"):
            # Step 1: Validate field completeness
            validation_errors = validate_manifest_completeness()
            
            if validation_errors:
                # Show error summary
                error_count = sum(len(errors) for errors in validation_errors.values())
                st.error(f"❌ Cannot submit: {len(validation_errors)} task(s) have incomplete data ({error_count} issues)")
                
                # Display errors
                with st.expander("📋 Validation Errors", expanded=True):
                    for task_key, errors in validation_errors.items():
                        st.markdown(f"**📌 {task_key}** ⚠️")
                        for error in errors:
                            st.markdown(f"  - {error}")
                
                # Auto-expand tasks with errors
                if validation_errors:
                    first_error_task = list(validation_errors.keys())[0]
                    st.session_state['expanded_task'] = first_error_task
                    st.session_state['validation_errors'] = validation_errors
                    st.rerun()
            
            else:
                # Step 2: Check if all assets are validated
                has_unvalidated = False
                for t_key, t_data in st.session_state['task_data'].items():
                    for asset in t_data.get('sources', []) + t_data.get('targets', []):
                        if asset.get('identifier') and "❔" in asset.get('validation_status', ''):
                            has_unvalidated = True
                            break
                
                if has_unvalidated:
                    st.error("❌ Please validate all assets before submitting.")
                else:
                    # Add metadata to manifest
                    final_manifest['_metadata'] = {
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
                        st.info(f"""
ℹ️ **No changes detected** since v{st.session_state.get('loaded_manifest_version')}.

Nothing to submit. The manifest is identical to the loaded version.
                        """)
                        
                        # Still save local JSON
                        output_path = "manifest.json"
                        with open(output_path, "w") as f:
                            json.dump(final_manifest, f, indent=2)
                        st.caption(f"💾 Local copy saved to `{output_path}`")
                    
                    else:
                        # Changes detected, proceed with submit
                        output_path = "manifest.json"
                        with open(output_path, "w") as f:
                            json.dump(final_manifest, f, indent=2)
                        
                        st.success(f"✅ Manifest submitted and saved to `{output_path}`")
                        
                        # Save to Delta with SUBMITTED status
                        if manifest_table:
                            with st.spinner(f"Submitting to `{manifest_table}`..."):
                                db_service = DatabricksService()
                                save_res = db_service.save_manifest_to_table(
                                    table_name=manifest_table,
                                    manifest=final_manifest,
                                    job_id=job_id_input if job_id_input else "unknown",
                                    version=manifest_version_input,
                                    status="SUBMITTED"
                                )
                                if "✅" in save_res:
                                    st.toast(save_res, icon="✅")
                                else:
                                    st.error(save_res)

    # View JSON
    with st.expander("View Generated Manifest JSON"):
        st.json(final_manifest)
