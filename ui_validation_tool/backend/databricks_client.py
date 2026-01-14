from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Job
from databricks.sdk.service.workspace import ExportFormat
import logging
import base64
import os
import json
import zipfile
from typing import List, Dict, Any
from .config import get_config

logger = logging.getLogger(__name__)

class DatabricksService:
    def __init__(self):
        self.config = get_config()
        # We instantiate WorkspaceClient without arguments.
        # This allows the SDK to automatically resolve credentials using its standard chain:
        # 1. Environment Variables (loaded via dotenv in config.py)
        # 2. Databricks Configuration Profiles
        # 3. Native Authentication (when running on Databricks)
        # Explicit (host, token) arguments can conflict with Native Auth (OAuth) on Databricks.
        self.client = WorkspaceClient()

    def get_job(self, job_id: int) -> Dict[str, Any]:
        """Fetch full job definition."""
        try:
            job = self.client.jobs.get(job_id)
            return job.as_dict()
        except Exception as e:
            logger.error(f"Failed to fetch job {job_id}: {e}")
            raise

    def get_job_tasks(self, job_id: int) -> List[Dict[str, Any]]:
        """
        Get unique tasks from a job.
        Returns a list of dicts with task_key, task_type, dependencies, and metadata.
        """
        job_dict = self.get_job(job_id)
        settings = job_dict.get("settings", {})
        tasks = settings.get("tasks", [])
        
        simplified_tasks = []
        for t in tasks:
            task_key = t.get("task_key")
            task_type = "unknown"
            script_path = None
            parameters = []
            package_name = None
            
            if "notebook_task" in t:
                task_type = "notebook"
                nb_task = t["notebook_task"]
                script_path = nb_task.get("notebook_path")
                parameters = nb_task.get("base_parameters", {})
                
            elif "spark_python_task" in t:
                task_type = "python"
                py_task = t["spark_python_task"]
                script_path = py_task.get("python_file")
                parameters = py_task.get("parameters", [])
                
            elif "python_wheel_task" in t:
                task_type = "wheel"
                whl_task = t["python_wheel_task"]
                package_name = whl_task.get("package_name")
                parameters = whl_task.get("parameters", [])
                
                # Find the wheel file in libraries
                # We assume the first whl library is the one containing the code
                libraries = t.get("libraries", [])
                for lib in libraries:
                    if "whl" in lib:
                        script_path = lib["whl"]
                        break
            
            elif "sql_task" in t:
                task_type = "sql"
                # SQL tasks might not have a file path in the same way
            
            simplified_tasks.append({
                "task_key": task_key,
                "task_type": task_type,
                "script_path": script_path,
                "package_name": package_name,
                "parameters": parameters,
                "depends_on": [d.get("task_key") for d in t.get("depends_on", [])]
            })
            
        return simplified_tasks

    def get_task_code(self, task: Dict[str, Any]) -> str:
        """
        Retrieves the code for a given task from Databricks (Notebook, File, or Wheel).
        """
        task_type = task.get("task_type")
        script_path = task.get("script_path")

        try:
            if task_type == "notebook" and script_path:
                return self._get_notebook_content(script_path)
            
            elif task_type == "python" and script_path:
                # Script path could be Workspace (/Workspace/...) or DBFS (/dbfs/...)
                return self._get_file_content(script_path)

            elif task_type == "wheel":
                return self._process_wheel_task(task)
            
            return f"# No code retrieval supported for task type: {task_type}"

        except Exception as e:
            logger.error(f"Failed to retrieve code for {task.get('task_key')}: {e}")
            return f"# Error retrieving code: {str(e)}"

    def _get_notebook_content(self, path: str) -> str:
        """Exports notebook source from Workspace."""
        try:
            # SDK handles the export
            resp = self.client.workspace.export(path, format=ExportFormat.SOURCE)
            if resp.content:
                return base64.b64decode(resp.content).decode('utf-8')
            return ""
        except Exception as e:
             # Fallback or re-raise
             raise ValueError(f"Could not export notebook at {path}: {e}")

    def _get_file_content(self, path: str) -> str:
        """Reads file from Workspace or DBFS."""
        # Check prefix
        if path.startswith("dbfs:") or path.startswith("/dbfs"):
            # Use DBFS API
            dbfs_path = path if path.startswith("dbfs:") else f"dbfs:{path}"
            with self.client.dbfs.open(dbfs_path) as f:
                return f.read().decode('utf-8')
        else:
            # Assume Workspace file
            return self._get_notebook_content(path)

    def _process_wheel_task(self, task: Dict[str, Any]) -> str:
        """Downloads wheel, caches it, and extracts source code."""
        package_name = task.get("package_name", "unknown")
        whl_path = task.get("script_path") 
        
        if not whl_path:
            return "# No wheel path found in task definition."
            
        # Setup Cache
        cache_dir = os.path.join(self.config.temp_dir, "wheels")
        os.makedirs(cache_dir, exist_ok=True)
        
        whl_filename = os.path.basename(whl_path)
        local_whl_path = os.path.join(cache_dir, whl_filename)
        meta_path = os.path.join(cache_dir, f"{whl_filename}.json")
        extract_dir = os.path.join(cache_dir, f"{whl_filename}_extracted")

        # 1. Get DBFS File Info
        dbfs_path = whl_path if whl_path.startswith("dbfs:") else f"dbfs:{whl_path}"
        try:
            status = self.client.dbfs.get_status(dbfs_path)
        except Exception as e:
             return f"# Not found or no access to wheel: {dbfs_path}"

        # 2. Check Cache
        cached = False
        if os.path.exists(local_whl_path) and os.path.exists(meta_path) and os.path.exists(extract_dir):
            try:
                with open(meta_path, 'r') as f:
                    meta = json.load(f)
                if meta.get('modification_time') == status.modification_time and meta.get('file_size') == status.file_size:
                    cached = True
            except:
                pass # Corrupt meta, re-download
        
        # 3. Download if not cached
        if not cached:
            logger.info(f"Downloading wheel {whl_filename}...")
            # Cleanup
            if os.path.exists(extract_dir):
                import shutil
                shutil.rmtree(extract_dir)
            
            with self.client.dbfs.open(dbfs_path) as src, open(local_whl_path, 'wb') as dst:
                dst.write(src.read())
            
            # Update Meta
            with open(meta_path, 'w') as f:
                json.dump({
                    "modification_time": status.modification_time,
                    "file_size": status.file_size
                }, f)
                
            # Extract
            with zipfile.ZipFile(local_whl_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)

        # 4. Read Source Files
        source_code = []
        
        # Add Metadata Header
        source_code.append(f"# Task Metadata")
        source_code.append(f"# Package: {package_name}")
        source_code.append(f"# Wheel: {whl_path}")
        source_code.append(f"# Parameters: {task.get('parameters', [])}")
        source_code.append("-" * 40)

        for root, dirs, files in os.walk(extract_dir):
             for file in files:
                 if file.endswith(".py"):
                     full_path = os.path.join(root, file)
                     rel_path = os.path.relpath(full_path, extract_dir)
                     
                     # Skip common noise
                     if "site-packages" in rel_path or "egg-info" in rel_path:
                         continue

                     source_code.append(f"\n# File: {rel_path}")
                     try:
                        with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                            source_code.append(f.read())
                     except Exception as e:
                        source_code.append(f"# Error reading file: {e}")
        
        return "\n".join(source_code)
