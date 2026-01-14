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

    def _download_file_stream(self, path: str):
        """
        Returns a file-like object (stream) for the remote path.
        Handles DBFS, Volumes, and Workspace files.
        """
        # 1. ADLS / WASBS Guard
        if any(path.startswith(p) for p in ["abfss:", "wasbs:", "adls:"]):
            raise ValueError(
                f"Direct download from ADLS ({path}) is not supported.\n"
                "Please use a Unity Catalog Volume path (/Volumes/...) or DBFS Mount (dbfs:/mnt/...) instead."
            )

        # 2. Volumes or Workspace Files
        # Paths usually start with /Volumes or /Workspace
        if path.startswith("/Volumes") or path.startswith("/Workspace"):
            logger.info(f"Downloading from Files API: {path}")
            return self.client.files.download(path).contents

        # 3. DBFS
        # Normalize DBFS path
        dbfs_path = path
        if path.startswith("/dbfs"):
             dbfs_path = f"dbfs:{path}"
        elif not path.startswith("dbfs:"):
             # Fallback/Default to DBFS if no other prefix matched
             # But be careful, if it's a relative path in logic?
             # For now, assume dbfs: if not absolute /Volumes or /Workspace
             dbfs_path = f"dbfs:{path}"
             
        logger.info(f"Downloading from DBFS API: {dbfs_path}")
        return self.client.dbfs.open(dbfs_path, read=True)

    def _get_file_content(self, path: str) -> str:
        """Reads file from Workspace or DBFS or Volumes."""
        try:
            # Use unified downloader
            # Context manager is tricky because different returns might behave differently
            # but usually they support read().
            stream = self._download_file_stream(path)
            
            # If it's a context manager (dbfs.open), we should use 'with', 
            # but files.download().contents is likely just a stream.
            # Let's try to read safely.
            try:
                content = stream.read()
            finally:
                if hasattr(stream, 'close'):
                    stream.close()
                    
            return content.decode('utf-8')
        except Exception as e:
            # Fallback for old Workspace file logic if it was a Notebook path treated as file?
            # Or if _download_file_stream failed.
            logger.warning(f"File download failed for {path}: {e}. Retrying as Notebook Export.")
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

        # 1. Get File Info (Modification Time)
        # This is tricky across different APIs. 
        # DBFS has get_status. Files API has get_metadata.
        # For simplicity, we might skip strict cache invalidation for non-DBFS paths 
        # OR implement _get_file_metadata. 
        # Let's try to support it.
        
        file_size = 0
        mod_time = 0
        
        try:
            if whl_path.startswith("dbfs:") or whl_path.startswith("/dbfs") or (not whl_path.startswith("/")):
                 dbfs_p = whl_path if whl_path.startswith("dbfs:") else f"dbfs:{whl_path}"
                 status = self.client.dbfs.get_status(dbfs_p)
                 file_size = status.file_size
                 mod_time = status.modification_time
            elif whl_path.startswith("/Volumes") or whl_path.startswith("/Workspace"):
                 status = self.client.files.get_metadata(whl_path)
                 file_size = status.content_length
                 mod_time = status.last_modified # distinct format?
        except Exception as e:
             logger.warning(f"Could not get metadata for {whl_path}: {e}")
             # Proceed without strict caching if metadata fails (force download?)
             pass

        # 2. Check Cache
        cached = False
        if os.path.exists(local_whl_path) and os.path.exists(meta_path) and os.path.exists(extract_dir):
            try:
                with open(meta_path, 'r') as f:
                    meta = json.load(f)
                # Only check if we successfully got remote metadata
                if mod_time > 0:
                     if meta.get('modification_time') == mod_time and meta.get('file_size') == file_size:
                        cached = True
                else:
                    # If we couldn't get remote meta, maybe trust cache? 
                    # Or force refresh? Let's trust cache to avoid error loops if just metadata failed.
                    cached = True 
            except:
                pass 
        
        # 3. Download if not cached
        if not cached:
            logger.info(f"Downloading wheel {whl_filename}...")
            # Cleanup
            if os.path.exists(extract_dir):
                import shutil
                shutil.rmtree(extract_dir)
            
            try:
                stream = self._download_file_stream(whl_path)
                with open(local_whl_path, 'wb') as dst:
                    # Shutil copyfileobj is efficient
                    import shutil
                    shutil.copyfileobj(stream, dst)
                
                # Cleanup stream
                if hasattr(stream, 'close'): stream.close()
                
            except Exception as e:
                return f"# Error downloading wheel {whl_path}: {e}"
            
            # Update Meta
            if mod_time > 0:
                with open(meta_path, 'w') as f:
                    json.dump({
                        "modification_time": mod_time,
                        "file_size": file_size
                    }, f)
                
            # Extract
            try:
                with zipfile.ZipFile(local_whl_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
            except zipfile.BadZipFile:
                 return f"# Error: Downloaded file {whl_filename} is not a valid zip/wheel."

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
