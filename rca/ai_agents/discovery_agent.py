import os
import re
import pathlib
import sys
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task
from pydantic import BaseModel, Field

from config.config import get_config
import logging
import urllib.parse
import hashlib
import zipfile
import io
import tempfile
import shutil

logger = logging.getLogger(__name__)


class StepInfo(BaseModel):
    """Represents a single step in the pipeline."""
    task_key: str
    task_type: str = "notebook" # notebook, python, wheel, sql
    script_path: str # Generic path (notebook path, file path, or whl path)
    
    code_source_type: Optional[str] = None  # WORKSPACE or MANIFEST
    dependencies: List[str] = Field(default_factory=list)
    code_content: Optional[str] = None
    # Drift Detection Fields
    manifest_code_snapshot: Optional[str] = None
    is_drift_detected: bool = False


class DiscoveryAgent:
    """
    The Cartographer: Discovers pipeline structure and maps code.
    Replaces Git cloning with Manifest-based local file retrieval.
    """
    
    def __init__(self):
        self.config = get_config()
        self.workspace_client: Optional[WorkspaceClient] = None
    
    def _get_workspace_client(self) -> WorkspaceClient:
        """Initialize Databricks Workspace Client."""
        if self.workspace_client is None:
            self.workspace_client = WorkspaceClient(
                host=self.config.databricks_host,
                token=self.config.databricks_token
            )
        return self.workspace_client
    
    def fetch_job_definition(self, job_id: int) -> Dict:
        """
        Fetch the Job definition from Databricks API.
        Returns the full job JSON including all tasks.
        """
        client = self._get_workspace_client()
        job = client.jobs.get(job_id)
        return job.as_dict()
    
    def extract_steps_from_job(self, job_json: Dict) -> List[StepInfo]:
        """
        Parse the Job JSON and extract a list of StepInfo objects.
        Supports Notebooks, Python Scripts, and Wheels.
        """
        steps = []
        settings = job_json.get("settings", {})
        tasks = settings.get("tasks", [])
        
        for task in tasks:
            task_key = task.get("task_key", "unknown")
            task_type = "unknown"
            script_path = None
            dependencies = []
            
            # Determine Type & Path
            if "notebook_task" in task:
                task_type = "notebook"
                script_path = task["notebook_task"].get("notebook_path", "")
            elif "spark_python_task" in task:
                task_type = "python"
                script_path = task["spark_python_task"].get("python_file", "")
            elif "python_wheel_task" in task:
                task_type = "wheel"
                # Wheel path is usually in libraries, not the task body directly?
                # The UI tool logic looked in libraries.
                libraries = task.get("libraries", [])
                for lib in libraries:
                    if "whl" in lib:
                        script_path = lib["whl"]
                        break
                # Fallback if provided in parameters (rare)
            elif "sql_task" in task:
                 task_type = "sql"
                 if "file" in task["sql_task"]:
                     script_path = task["sql_task"]["file"].get("path", "")

            # Extract dependencies
            if "depends_on" in task:
                dependencies = [dep.get("task_key") for dep in task["depends_on"]]
            
            steps.append(StepInfo(
                task_key=task_key,
                task_type=task_type,
                script_path=script_path or "N/A",
                dependencies=dependencies
            ))
        
        return steps
    
    def _download_file_stream(self, path: str):
        """
        Returns a file-like object (stream) for the remote path.
        Handles DBFS, Volumes, and Workspace files.
        """
        client = self._get_workspace_client()
        # 1. Volumes or Workspace Files
        if path.startswith("/Volumes") or path.startswith("/Workspace"):
            return client.files.download(path).contents

        # 2. DBFS
        dbfs_path = path
        if path.startswith("/dbfs"):
             dbfs_path = f"dbfs:{path}"
        elif not path.startswith("dbfs:"):
             dbfs_path = f"dbfs:{path}"
             
        return client.dbfs.open(dbfs_path, read=True)

    def _process_wheel(self, whl_path: str) -> Optional[str]:
        """Downloads wheel, extracts source code from .py files."""
        try:
             # Download
             stream = self._download_file_stream(whl_path)
             whl_data = stream.read()
             
             # Extract in memory if possible, or use temp file
             # ZipFile works with BytesIO
             content_buffer = io.BytesIO()
             full_text = []
             
             with zipfile.ZipFile(io.BytesIO(whl_data)) as z:
                 for file in z.namelist():
                     if file.endswith(".py") and "site-packages" not in file:
                         with z.open(file) as f:
                             text = f.read().decode("utf-8", errors="ignore")
                             full_text.append(f"# File: {file}\n{text}")
             
             return "\n\n".join(full_text)
        except Exception as e:
            logger.warning(f"Failed to process wheel {whl_path}: {e}")
            return None

    def fetch_code_from_workspace(self, step: StepInfo) -> Optional[str]:
        """
        Fetch the authentic code content directly from the Databricks Workspace.
        Supports Notebooks, Scripts, and Wheels.
        """
        path = step.script_path
        if not path or path == "N/A":
            return None
            
        client = self._get_workspace_client()
        
        try:
            if step.task_type == "notebook":
                # Export as SOURCE
                from databricks.sdk.service.workspace import ExportFormat
                import base64
                response = client.workspace.export(path=path, format=ExportFormat.SOURCE)
                return base64.b64decode(response.content).decode('utf-8')
                
            elif step.task_type == "python" or step.task_type == "sql":
                 # Download file
                 stream = self._download_file_stream(path)
                 return stream.read().decode('utf-8')
                 
            elif step.task_type == "wheel":
                 return self._process_wheel(path)
                 
            else:
                return None

        except Exception as e:
            logger.warning(f"Could not fetch authentic code for {path} (Type: {step.task_type}): {e}")
            return None

    def resolve_code_from_manifest(self, steps: List[StepInfo], manifest_data: Optional[Dict]) -> List[StepInfo]:
        """
        Resolve code content strategies:
        1. Ground Truth: Fetch directly from Workspace (if possible).
        2. Comparison: Check against Manifest (Code Drift Detection).
        3. Fallback: Use Manifest content if Workspace unreadable.
        """
        if not manifest_data:
            logger.warning("No manifest provided. Cannot resolve code content.")
            return steps
            
        for step in steps:
            task_data = manifest_data.get(step.task_key)
            if not task_data:
                logger.warning(f"Task {step.task_key} not found in manifest.")
                continue
            
            # --- 1. Get Manifest Code ---
            manifest_code = None
            embedded_code = task_data.get("code_content", {})
            if embedded_code:
                # Combine all files? Usually typical task has 1 entry point + utils
                # We need to match what we downloaded from Workspace (Entry Point)
                # This logic is tricky if multiple files.
                # Assuming simple notebook task pattern for now.
                manifest_code = "\n".join([f"# File: {k}\n{v}" for k,v in embedded_code.items()])
            
            # --- 2. Get Authentic Code ---
            authentic_code = self.fetch_code_from_workspace(step)
            
            # --- 3. Drift Detection & Selection ---
            if authentic_code:
                step.code_content = authentic_code
                step.code_source_type = "WORKSPACE"
                
                if manifest_code:
                     def normalize(s): return re.sub(r'\s+', '', s)
                     
                     if normalize(authentic_code) != normalize(manifest_code):
                             logger.warning(f"[DRIFT DETECTED] Task {step.task_key}: Code Changed since Manifest generation.")
                             step.is_drift_detected = True
                             step.manifest_code_snapshot = manifest_code
                else:
                    logger.info(f"Task {step.task_key}: using Workspace code (No manifest code found).")
            
            elif manifest_code:
                # Fallback to Manifest
                step.code_content = manifest_code
                step.code_source_type = "MANIFEST"
                logger.info(f"Task {step.task_key}: using Manifest code (Workspace fetch failed).")
                
        return steps

    def discover(self, job_id: int, manifest_data: Optional[Dict] = None) -> List[StepInfo]:
        """
        Main entry point: Discover pipeline structure.
        Iterates Databricks Job tasks and maps them to code via Manifest.
        Args:
            job_id: Databricks Job ID
            manifest_data: Dictionary containing task->file mapping
        """
        logger.info("[Discovery] Fetching Job %s...", job_id)
        job_json = self.fetch_job_definition(job_id)

        logger.info("[Discovery] Extracting steps...")
        steps = self.extract_steps_from_job(job_json)
        
        if manifest_data:
            logger.info("[Discovery] Resolving code from Manifest...")
            steps = self.resolve_code_from_manifest(steps, manifest_data)
        else:
            logger.warning("[Discovery] No Manifest provided. Code content will be empty.")

        return steps
