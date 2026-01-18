import streamlit as st
import pandas as pd
import json
import logging
from backend.databricks_client import DatabricksService
from backend.config import get_config, setup_logging
from ai_agents.mapping_agent import MappingAgent

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
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png", width=150)
    st.title("Settings")
    
    st.markdown("### 🔑 API Config")
    st.info("Configuration loaded from Environment Variables.")
    
    st.markdown("### ⚙️ Job Parameters")
    job_params_input = st.text_area("JSON Parameters", value='{"env": "prod"}', height=100)

    st.markdown("### 💾 History Config")
    # Read from Config (Env Var)
    history_table = config.history_table
    st.text_input("History Table (Read-only)", value=history_table, disabled=True, help="Target Delta table (set via HISTORY_TABLE env var)")
    manifest_version_input = st.text_input("Manifest Version", value="1.0", help="Version tag for this run")
    
    st.markdown("---")
    st.markdown("### ℹ️ Help")
    st.markdown("1. Enter **Job ID**.\n2. **Select Tasks** to analyze.\n3. **Run AI Analysis** to map lineage.\n4. **Validate** & **Save** manifest.")

# --- Main Content ---
st.title("🔍 Agentic RCA Validator")
st.markdown("Generate and validate Unit Catalog lineage mappings for your Databricks Jobs.")

# Initialize Session State
if 'job_tasks' not in st.session_state: st.session_state['job_tasks'] = []
if 'analysis_results' not in st.session_state: st.session_state['analysis_results'] = {}

# --- Step 1: Input ---
with st.expander("1️⃣ Job Configuration", expanded=True):
    job_id_input = st.text_input("Databricks Job ID", help="The numeric ID of your Databricks Job")

    if st.button("Fetch Job"):
        if job_id_input:
            with st.spinner("Fetching Job Details..."):
                try:
                    # 1. Fetch Job
                    db_service = DatabricksService()
                    tasks = db_service.get_job_tasks(int(job_id_input))
                    st.session_state['job_tasks'] = tasks
                    
                    # Log filtering stats
                    valid_count = len([t for t in tasks if t.get('task_type') and t['task_type'].lower() != "unknown"])
                    st.success(f"✅ Found {valid_count} valid tasks (and {len(tasks)-valid_count} ignored unknown/empty types).")

                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("Please enter Job ID.")

# --- Step 2: Task Selection ---
if st.session_state['job_tasks']:
    st.markdown("### 2️⃣ Select Tasks to Analyze")
    
    # 1. Filter Logic
    valid_tasks = [
        t for t in st.session_state['job_tasks'] 
        if t.get('task_type') and t['task_type'].lower() != "unknown"
    ]
    
    if not valid_tasks:
        st.warning("No valid tasks found (filtered out unknown/empty types).")
    else:
        # Initialize selection state if not present
        if "task_selection" not in st.session_state:
            st.session_state.task_selection = {t['task_key']: True for t in valid_tasks}
            st.session_state.select_all_state = True
        
        # Ensure filtering consistency: if new valid tasks appeared, track them
        for t in valid_tasks:
             if t['task_key'] not in st.session_state.task_selection:
                 st.session_state.task_selection[t['task_key']] = True

        # Callbacks for Sync Logic --------------------------------
        def on_select_all_change():
            """Called when 'Select All' is toggled."""
            new_state = st.session_state.select_all_key
            for t in valid_tasks:
                st.session_state.task_selection[t['task_key']] = new_state
            
            # Update the flag to match
            st.session_state.select_all_state = new_state

        def on_individual_change():
            """Called when ANY individual task checkbox is toggled."""
            # We need to scan all checkboxes to see if all are selected
            # Streamlit 'key' writes directly to session state.
            
            current_selections = []
            for t in valid_tasks:
                # Read the session state key for this checkbox
                key = f"sel_{t['task_key']}"
                if key in st.session_state:
                     val = st.session_state[key]
                     st.session_state.task_selection[t['task_key']] = val
                     current_selections.append(val)
            
            all_selected = all(current_selections) if current_selections else False
            
            # Update 'Select All' state visually without triggering its callback loop
            st.session_state.select_all_state = all_selected
            # We must also update the key binding for Select All if we want it to check/uncheck
            # But changing 'select_all_key' might trigger its callback?
            # Streamlit trick: Just update the value associated with the key?
            # Actually, we can't easily update the specific 'select_all_key' widget state programmatically 
            # while inside another callback unless we rerun.
            # But the on_change triggers BEFORE this script reruns.
            # So updating st.session_state.select_all_state (which is bound to 'value') should work on next render.
        # ---------------------------------------------------------

        # "Select All" Checkbox
        # We bind 'value' to a state variable that we manually update.
        st.checkbox(
            "Select All Valid Tasks", 
            value=st.session_state.select_all_state,
            key="select_all_key",
            on_change=on_select_all_change
        )
        
        # 2. Compact Grid Layout (3 Columns)
        cols = st.columns(3)
        st.markdown("""<style>.stCheckbox { margin-bottom: -10px; }</style>""", unsafe_allow_html=True)
        
        selected_tasks_list = []
        
        for i, task in enumerate(valid_tasks):
            col = cols[i % 3]
            t_key = task['task_key']
            
            with col:
                # Bind value to our tracking dict.
                # Bind on_change to update the 'Select All' master checkbox.
                is_selected = st.checkbox(
                    f"**{t_key}** ({task['task_type']})",
                    value=st.session_state.task_selection[t_key],
                    key=f"sel_{t_key}",
                    on_change=on_individual_change
                )
            
            if is_selected:
                selected_tasks_list.append(task)
        
        # Assign to variable expected by Step 3
        selected_tasks = selected_tasks_list
    
    st.caption(f"Selected {len(selected_tasks)} / {len(valid_tasks)} tasks.")

    # --- Step 3: Analysis & Validation (Combined) ---
    if st.button("🚀 Run AI Lineage Analysis"):
        if not selected_tasks:
            st.warning("No tasks selected.")
        else:
            with st.status("Running AI Analysis...", expanded=True) as status:
                agent = MappingAgent()
                db_service = DatabricksService()
                
                results = {}
                progress_bar = st.progress(0)
                
                for i, task in enumerate(selected_tasks):
                    st.write(f"Analyzing `{task['task_key']}`...")
                    code_context = db_service.get_task_code(task)
                    
                    if code_context and isinstance(code_context, dict) and "error.txt" not in code_context:
                        # Append params
                        # Append params
                        if job_params_input:
                            current_meta = code_context.get("__metadata__", "")
                            code_context["__metadata__"] = f"{current_meta}\nParameters: {job_params_input}"
                        
                        # Analyze with Real-time Streaming
                        st.markdown(f"**Analyzing {task['task_key']}...**")
                        
                        log_container = st.container(height=300)
                        def stream_log(msg):
                            log_container.write(msg)
                            
                        mapping = agent.analyze_code(code_context, on_log=stream_log)
                        
                        # Store raw results (assets object list)
                        results[task['task_key']] = {
                            "assets": [a.model_dump() for a in mapping.assets],
                            "trace": mapping.resolution_trace,
                            "assets": [a.model_dump() for a in mapping.assets],
                            "trace": mapping.resolution_trace,
                            "token_stats": mapping.token_stats,
                            "source_files": mapping.source_files
                        }
                    else:
                        st.warning(f"Failed to get code for {task['task_key']}")
                        results[task['task_key']] = {"assets": [], "trace": [], "token_stats": {}}
                    
                    progress_bar.progress((i + 1) / len(selected_tasks))
                
                st.session_state['analysis_results'] = results
                status.update(label="Analysis Complete!", state="complete", expanded=False)

# --- Sidebar Stats ---
if st.session_state['analysis_results']:
    total_tokens = sum([t.get("token_stats", {}).get("total", 0) for t in st.session_state['analysis_results'].values()])
    st.sidebar.markdown("### 📊 Analysis Stats")
    st.sidebar.metric("Total Tokens (Est.)", total_tokens)


# --- Step 4: Collapsible Results View ---
if st.session_state['analysis_results']:
    st.markdown("### 3️⃣ Review & Edit Lineage")
    st.info("Expand each task to review and edit the discovered assets. You can Add/Delete rows directly.")
    
    final_manifest = {}
    
    # Iterate over tasks
    for task_key, data in st.session_state['analysis_results'].items():
        # Collapsible View
        with st.expander(f"📌 {task_key}", expanded=False):
            
            # Prepare Data for Editor
            current_assets = data.get("assets", [])
            df = pd.DataFrame(current_assets)
            
            if df.empty:
                df = pd.DataFrame(columns=["subtype", "identifier", "usage", "confidence", "asset_type", "validation_status"])
            
            # Ensure validation_status exists
            if "validation_status" not in df.columns:
                df["validation_status"] = "❔ Unchecked"

            # Editor Configuration
            edited_df = st.data_editor(
                df,
                num_rows="dynamic", # Allow Add/Delete
                use_container_width=True,
                key=f"editor_{task_key}",
                column_config={
                    "validation_status": st.column_config.TextColumn("Status", width="small", help="Result of validation check"),
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
                        "Source/Target Name",
                        required=True,
                        width="large",
                        help="Full path or table name"
                    ),
                    "usage": st.column_config.SelectboxColumn(
                        "Usage",
                        options=["SOURCE", "TARGET"],
                        required=True,
                        width="small"
                    ),
                    # Hide internal columns
                    "asset_type": None, 
                    "confidence": None,
                    "evidence": None
                },
                column_order=["validation_status", "usage", "subtype", "identifier"]
            )
            
            # Construct Manifest Chunk from *Edited* Data
            sources = []
            targets = []
            
            # Convert back to list of dicts for session state update (to persist edits)
            updated_assets_list = edited_df.to_dict("records")
            data["assets"] = updated_assets_list
            
            for _, row in edited_df.iterrows():
                asset_entry = {
                    "name": row["identifier"],
                    "type": row["subtype"]
                }
                if row["usage"] == "SOURCE":
                    sources.append(asset_entry)
                elif row["usage"] == "TARGET":
                    targets.append(asset_entry)
            
            final_manifest[task_key] = {
                "sources": sources,
                "targets": targets,
                "source_files": data.get("source_files", []),
                "code_content": data.get("source_code_snapshot", {}) # Persist Code!
            }

    # --- Validate & Save Button (Global) ---
    st.markdown("---")
    
    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        if st.button("🔎 Validate & Save", type="primary"):
            validation_error_count = 0
            
            with st.status("Validating Assets...", expanded=True) as status:
                # 1. Collect all assets to validate
                all_assets_to_validate = []
                count = 0
                for t_key, t_data in st.session_state['analysis_results'].items():
                    for a in t_data.get("assets", []):
                        all_assets_to_validate.append(a)
                        count += 1
                
                st.write(f"Validating {count} assets against Databricks...")
                
                # 2. Call Backend
                db_service = DatabricksService()
                validation_results = db_service.validate_assets(all_assets_to_validate)
                
                # 3. Update Status in Session State
                valid_count = 0
                invalid_count = 0
                
                for t_key, t_data in st.session_state['analysis_results'].items():
                    for a in t_data.get("assets", []):
                         ident = a.get("identifier")
                         status_msg = validation_results.get(ident, "❔ Unchecked")
                         a["validation_status"] = status_msg
                         
                         if "✅" in status_msg: 
                             valid_count += 1
                         else:
                             # Strict Mode: Any warning (⚠️) or error (❌) is an issue
                             invalid_count += 1
                
                validation_error_count = invalid_count
                status.update(label=f"Done! {valid_count} Valid, {invalid_count} Issues.", state="complete", expanded=False)
            
            # Persistent Alert
            if validation_error_count > 0:
                st.error(f"⚠️ Validation finished with **{validation_error_count} issues**. Please review the items marked with ❌ above before saving.")
                st.warning("You can fix the identifiers directly in the tables above and click 'Validate & Save' again.")
            else:
                st.success("✅ All assets validated successfully!")
                
            # 5. Save Manifest (Local)
            output_path = "manifest.json"
            if validation_error_count == 0:
                with open(output_path, "w") as f:
                    json.dump(final_manifest, f, indent=2)
                
                st.success(f"File saved to `{output_path}`")
                
                # 6. Save to Delta Table (if configured)
                if history_table:
                     with st.spinner(f"Saving to history table `{history_table}`..."):
                        save_res = db_service.save_manifest_to_table(
                             table_name=history_table,
                             manifest=final_manifest,
                             job_id=job_id_input if job_id_input else "unknown",
                             version=manifest_version_input
                        )
                        if "✅" in save_res:
                            st.toast(save_res, icon="✅")
                            # Append to persisted success message
                            st.session_state['last_table_msg'] = save_res
                        else:
                            st.error(save_res)
            
            # 5. Rerun to show updated statuses in the tables (using a brief pause or just rerun)
            # st.rerun() # Rerun clears the success/error messages! 
            # Solution: We rely on the button callback flow. 
            # To make the tables update, we NEED to rerun. 
            # To keep the message, we can use session_state for the message.
            
            st.session_state['last_validation_msg'] = {
                "type": "error" if validation_error_count > 0 else "success",
                "count": validation_error_count
            }
            st.rerun()

# Display Persistent Message if exists
if 'last_validation_msg' in st.session_state:
    msg = st.session_state['last_validation_msg']
    if msg['type'] == 'error':
        st.error(f"⚠️ Validation finished with **{msg['count']} issues**. Please review the items marked with ❌ above.")
    else:
        st.success("✅ All assets validated successfully! Manifest saved.")
        if 'last_table_msg' in st.session_state:
             st.info(st.session_state['last_table_msg'])
             del st.session_state['last_table_msg']
    
    # Clear it so it doesn't stay forever if they change something
    # But we want it to stay until next action? Let's leave it.
    del st.session_state['last_validation_msg']

    with st.expander("View Generated JSON"):
        st.json(final_manifest)


