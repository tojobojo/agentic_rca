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
                            "trace": mapping.resolution_trace
                        }
                    else:
                        st.warning(f"Failed to get code for {task['task_key']}")
                        results[task['task_key']] = {"assets": [], "trace": []}
                    
                    progress_bar.progress((i + 1) / len(selected_tasks))
                
                st.session_state['analysis_results'] = results
                status.update(label="Analysis Complete!", state="complete", expanded=False)

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
            # We want columns: [Subtype (Dropdown)] [Identifier (Text)] [Usage (Hidden/Fixed? Or Dropdown?)]
            # User design showed: [Catalog v] [Editable Text Field] [X]
            # So 'Subtype' is the dropdown. 'Identifier' is the text.
            
            current_assets = data.get("assets", [])
            df = pd.DataFrame(current_assets)
            
            if df.empty:
                # Initialize empty structure if no assets found
                df = pd.DataFrame(columns=["subtype", "identifier", "usage", "confidence", "asset_type"])
            
            # Simplified columns for the view
            # If usage is mixed (Source/Target), we probably need to show it or split tables?
            # View.png implies a single list. Let's show Usage too so user knows if it's Source or Target.
            
            # Editor Configuration
            edited_df = st.data_editor(
                df,
                num_rows="dynamic", # Allow Add/Delete
                use_container_width=True,
                key=f"editor_{task_key}",
                column_config={
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
                        width="large"
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
                column_order=["usage", "subtype", "identifier"]
            )
            
            # Update session state with edits (so they persist across reruns/expansions)
            # data["assets"] = edited_df.to_dict("records") 
            # Note: Streamlit data_editor updates session_state automatically if key is set, 
            # but we need to capture the *output* of the editor function to get the current state for saving.
            
            # We'll construct the manifest chunk right here
            sources = []
            targets = []
            
            for _, row in edited_df.iterrows():
                if row["usage"] == "SOURCE":
                    sources.append(row["identifier"])
                elif row["usage"] == "TARGET":
                    targets.append(row["identifier"])
            
            final_manifest[task_key] = {
                "sources": sources,
                "targets": targets
            }

            # Optional: Show Trace
            if st.checkbox("Show Trace", key=f"trace_{task_key}"):
                st.text("\n".join(data.get("trace", [])))

    # --- Save Button (Global) ---
    st.markdown("---")
    if st.button("💾 Save Manifest to JSON", type="primary"):
        output_path = "manifest.json"
        with open(output_path, "w") as f:
            json.dump(final_manifest, f, indent=2)
        
        st.success(f"✅ Manifest saved to `{output_path}`")
        with st.expander("View Generated JSON"):
            st.json(final_manifest)


