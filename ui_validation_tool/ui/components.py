"""Reusable UI components for the validation tool."""
import streamlit as st
from backend.config import UIConfig


def render_sidebar(config: UIConfig):
    """Render sidebar with configuration and help."""
    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/commons/6/63/Databricks_Logo.png", width=150)
        st.title("Settings")
        
        st.markdown("### 🔑 API Config")
        st.info("Configuration loaded from Environment Variables.")
        
        st.markdown("### ⚙️ Job Parameters")
        job_params_input = st.text_area("JSON Parameters", value='{"env": "prod"}', height=100)

        st.markdown("### 💾 Manifest Config")
        manifest_table = config.manifest_table
        st.text_input("Manifest Table (Read-only)", value=manifest_table, disabled=True, 
                     help="Target Delta table (set via RCA_MANIFEST_TABLE env var)")
        manifest_version_input = st.text_input("Manifest Version", value="1.0", 
                                               help="Version tag for this run")
        
        st.markdown("---")
        st.markdown("### ℹ️ Help")
        st.markdown("1. Enter **Job ID** and fetch tasks.\\n2. **Analyze All** or manually add sources/targets.\\n3. **Validate** & **Save/Submit** manifest.")
    
    return job_params_input, manifest_table, manifest_version_input
