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
                    
                    st.success(f"✅ Found {len(tasks)} tasks!")
                except Exception as e:
                    st.error(f"Error: {e}")
        else:
            st.warning("Please enter Job ID.")

# --- Step 2: Task Selection ---
if st.session_state['job_tasks']:
    st.markdown("### 2️⃣ Select Tasks to Analyze")
    
    # "Select All" Logic
    all_keys = [t['task_key'] for t in st.session_state['job_tasks']]
    
    col_sel1, col_sel2 = st.columns([1, 4])
    with col_sel1:
        select_all = st.checkbox("Select All Tasks", value=True)
    
    selected_tasks = []
    
    # Display tasks in a grid or list
    with st.container():
        for task in st.session_state['job_tasks']:
            is_checked = select_all
            label = f"**{task['task_key']}** ({task['task_type']})"
            if task.get("package_name"):
                label += f" 📦 {task['package_name']}"
            if task.get("parameters"):
                label += f" 🔧 {task['parameters']}"
            
            if st.checkbox(label, value=is_checked, key=f"wk_{task['task_key']}"):
                selected_tasks.append(task)
    
    st.info(f"Selected {len(selected_tasks)} tasks for analysis.")

    # --- Step 3: Analysis ---
    if st.button("🚀 Run AI Lineage Analysis"):
        if not selected_tasks:
            st.warning("No tasks selected.")
        else:
            with st.status("Running AI Analysis...", expanded=True) as status:
                agent = MappingAgent()
                # db_service is already initialized in loop or we can re-init
                db_service = DatabricksService()
                
                results = {}
                progress_bar = st.progress(0)
                
                for i, task in enumerate(selected_tasks):
                    st.write(f"Analyzing `{task['task_key']}`...")
                    
                    # Use DatabricksService to get code
                    code_context = db_service.get_task_code(task)
                    
                    if code_context and isinstance(code_context, dict) and "error.txt" not in code_context:
                        # Success - we have a dictionary of files
                        file_count = len([k for k in code_context if k != "__metadata__"])
                        st.caption(f"Found {file_count} file(s).")
                        
                        # Analyze
                        mapping = agent.analyze_code(code_context)
                        
                        # Show Trace (Directly in status, no nested expander)
                        st.markdown("---")
                        st.caption("**Files Analyzed:**")
                        st.json([k for k in code_context if k != "__metadata__"])
                        
                        st.caption("**Resolution Steps:**")
                        if mapping.resolution_trace:
                            for step in mapping.resolution_trace:
                                st.text(step)
                        else:
                            st.info("No trace provided.")
                        st.markdown("---")

                        results[task['task_key']] = {
                            "sources": mapping.sources,
                            "targets": mapping.targets,
                            "logic": mapping.logic_summary,
                            "trace": mapping.resolution_trace,
                            "files": [k for k in code_context if k != "__metadata__"]
                        }
                    else:
                        error_msg = code_context.get("error.txt", "Unknown Error") if isinstance(code_context, dict) else str(code_context)
                        st.warning(f"Could not retrieve code for {task['task_key']}: {error_msg}")
                        results[task['task_key']] = {"sources": [], "targets": [], "logic": f"Retrieval failed: {error_msg}"}
                    
                    progress_bar.progress((i + 1) / len(selected_tasks))
                
                st.session_state['analysis_results'] = results
                status.update(label="Analysis Complete!", state="complete", expanded=False)

# --- Step 4: Validation & Save ---
if st.session_state['analysis_results']:
    st.markdown("### 3️⃣ Validate & Save Manifest")
    
    # Prepare Data for Editor
    # We want a format: Task Key | Logic | Sources | Targets
    # Sources/Targets should be comma-separated strings for editing, or list
    
    display_data = []
    for t_key, data in st.session_state['analysis_results'].items():
        display_data.append({
            "Task Key": t_key,
            "Logic Summary": data.get("logic", ""),
            "Source Tables": ", ".join(data.get("sources", [])),
            "Target Tables": ", ".join(data.get("targets", [])),
        })
    
    df = pd.DataFrame(display_data)
    
    edited_df = st.data_editor(
        df,
        num_rows="dynamic",
        use_container_width=True,
        key="editor",
        column_config={
            "Task Key": st.column_config.TextColumn(disabled=True),
            "Logic Summary": st.column_config.TextColumn(width="large"),
            "Source Tables": st.column_config.TextColumn("Source Tables (comma-separated)", required=True),
            "Target Tables": st.column_config.TextColumn("Target Tables (comma-separated)", required=True),
        }
    )

    st.markdown("#### 🕵️ Detailed Analysis Review")
    selected_task_key = st.selectbox("Select Task to View Details:", options=df["Task Key"].tolist())
    
    if selected_task_key:
        task_data = st.session_state['analysis_results'].get(selected_task_key, {})
        with st.container(border=True):
             col1, col2 = st.columns(2)
             with col1:
                 st.write("**Files Analyzed:**")
                 st.json(task_data.get("files", []))
             with col2:
                 st.write("**Resolution Trace:**")
                 trace = task_data.get("trace", [])
                 if trace:
                     for step in trace:
                         st.text(step)
                 else:
                     st.info("No trace available.")
    
    if st.button("💾 Save Manifest"):
        # Convert back to JSON format
        final_manifest = {}
        for index, row in edited_df.iterrows():
            task_key = row["Task Key"]
            sources = [s.strip() for s in row["Source Tables"].split(",") if s.strip()]
            targets = [t.strip() for t in row["Target Tables"].split(",") if t.strip()]
            
            final_manifest[task_key] = {
                "sources": sources,
                "targets": targets
            }
        
        # Save to file
        output_path = "manifest.json"
        with open(output_path, "w") as f:
            json.dump(final_manifest, f, indent=2)
            
        st.success(f"Manifest saved to `{output_path}`! You can now use this with the RCA system.")
        st.json(final_manifest, expanded=False)

