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

    def get_task_code(self, task: Dict[str, Any]) -> Dict[str, str]:
        """
        Retrieves the code for a given task from Databricks (Notebook, File, or Wheel).
        Returns a dictionary: {filename: content}
        """
        task_type = task.get("task_type")
        script_path = task.get("script_path")
        
        # Metadata header as a string to be pre-pended or handled by agent 
        # (Actually implementation plan said to extract it, but here we can include it in the dict)
        metadata_content = f"Task: {task.get('task_key')}\nType: {task_type}\n"
        if task.get("package_name"): metadata_content += f"Package: {task.get('package_name')}\n"
        if task.get("parameters"): metadata_content += f"Parameters: {task.get('parameters')}\n"
        
        result = {"__metadata__": metadata_content}

        try:
            if task_type == "notebook" and script_path:
                content = self._get_notebook_content(script_path)
                result[script_path] = content
            
            elif task_type == "python" and script_path:
                content = self._get_file_content(script_path)
                result[script_path] = content

            elif task_type == "wheel":
                wheel_files = self._process_wheel_task(task)
                result.update(wheel_files)
            
            return result

        except Exception as e:
            logger.error(f"Failed to retrieve code for {task.get('task_key')}: {e}")
            return {"error.txt": f"Error retrieving code: {str(e)}"}

    def _get_notebook_content(self, path: str) -> str:
        """Exports notebook source from Workspace."""
        try:
            resp = self.client.workspace.export(path, format=ExportFormat.SOURCE)
            if resp.content:
                return base64.b64decode(resp.content).decode('utf-8')
            return ""
        except Exception as e:
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
        if path.startswith("/Volumes") or path.startswith("/Workspace"):
            logger.info(f"Downloading from Files API: {path}")
            return self.client.files.download(path).contents

        # 3. DBFS
        dbfs_path = path
        if path.startswith("/dbfs"):
             dbfs_path = f"dbfs:{path}"
        elif not path.startswith("dbfs:"):
             dbfs_path = f"dbfs:{path}"
             
        logger.info(f"Downloading from DBFS API: {dbfs_path}")
        return self.client.dbfs.open(dbfs_path, read=True)

    def _get_file_content(self, path: str) -> str:
        """Reads file from Workspace or DBFS or Volumes."""
        try:
            stream = self._download_file_stream(path)
            try:
                content = stream.read()
            finally:
                if hasattr(stream, 'close'): stream.close()     
            return content.decode('utf-8')
        except Exception as e:
            logger.warning(f"File download failed for {path}: {e}. Retrying as Notebook Export.")
            return self._get_notebook_content(path)

    def _process_wheel_task(self, task: Dict[str, Any]) -> Dict[str, str]:
        """Downloads wheel, caches it, and extracts source code. Returns Dict[filename, content]"""
        package_name = task.get("package_name", "unknown")
        whl_path = task.get("script_path")
        logger.info(f"Processing wheel task: {task}, {whl_path}")
        
        if not whl_path:
             return {"error.txt": "# No wheel path found in task definition."}
            
        # Setup Cache
        cache_dir = os.path.join(self.config.temp_dir, "wheels")
        os.makedirs(cache_dir, exist_ok=True)
        logger.info(f"Cache directory: {cache_dir}")
        
        whl_filename = os.path.basename(whl_path)
        local_whl_path = os.path.join(cache_dir, whl_filename)
        meta_path = os.path.join(cache_dir, f"{whl_filename}.json")
        extract_dir = os.path.join(cache_dir, f"{whl_filename}_extracted")
        logger.info(f"Extract directory: {extract_dir}")

        # 1. Get File Info
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
                 mod_time = status.last_modified 
        except Exception as e:
             logger.warning(f"Could not get metadata for {whl_path}: {e}")
             pass

        # 2. Check Cache
        cached = False
        if os.path.exists(local_whl_path) and os.path.exists(meta_path) and os.path.exists(extract_dir):
            try:
                with open(meta_path, 'r') as f:
                    meta = json.load(f)
                if mod_time > 0:
                     if meta.get('modification_time') == mod_time and meta.get('file_size') == file_size:
                        cached = True
                else:
                    cached = True 
            except:
                pass 
        
        # 3. Download if not cached
        if not cached:
            logger.info(f"Downloading wheel {whl_filename}...")
            if os.path.exists(extract_dir):
                import shutil
                shutil.rmtree(extract_dir)
            
            try:
                stream = self._download_file_stream(whl_path)
                with open(local_whl_path, 'wb') as dst:
                    import shutil
                    shutil.copyfileobj(stream, dst)
                if hasattr(stream, 'close'): stream.close()
            except Exception as e:
                return {"error.txt": f"# Error downloading wheel {whl_path}: {e}"}
            
            if mod_time > 0:
                with open(meta_path, 'w') as f:
                    json.dump({"modification_time": mod_time, "file_size": file_size}, f)
                
            try:
                with zipfile.ZipFile(local_whl_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
            except zipfile.BadZipFile:
                 return {"error.txt": f"# Error: Downloaded file {whl_filename} is not a valid zip/wheel."}

        # 4. Read Source Files
        files_dict = {}
        
        for root, dirs, files in os.walk(extract_dir):
             for file in files:
                 # Allowed extensions for analysis
                 allowed_exts = {".py", ".sql", ".scala", ".java", ".yaml", ".yml", ".json", ".properties", ".conf", ".ini", ".txt"}
                 if any(file.endswith(ext) for ext in allowed_exts):
                     full_path = os.path.join(root, file)
                     rel_path = os.path.relpath(full_path, extract_dir)
                     if "site-packages" in rel_path or "egg-info" in rel_path: continue

                     try:
                        with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                            files_dict[rel_path] = f.read()
                     except Exception as e:
                        files_dict[rel_path] = f"# Error reading file: {e}"
        
        return files_dict
    def validate_assets(self, assets: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Validates the existence of the provided assets.
        Returns a dict mapping Identifier -> Status Message (e.g. "✅ Exists", "❌ Not Found", "⚠️ External").
        """
        results = {}
        
        # Batching isn't really supported by these APIs, so we do sequential checks.
        # Ideally, this should be parallelized or batched if the SDK supports it.
        
        for asset in assets:
            ident = asset.get("identifier")
            subtype = asset.get("subtype", "UNKNOWN")
            
            if not ident:
                continue

            try:
                # 1. Unity Catalog / Hive Tables
                if subtype in ["UNITY_CATALOG_TABLE", "HIVE_METASTORE_TABLE", "GENERIC_TABLE"] or "TABLE" in subtype:
                    try:
                        self.client.tables.get(ident)
                        results[ident] = "✅ Exists"
                    except Exception as e:
                        if "NOT_FOUND" in str(e):
                            results[ident] = "❌ Not Found"
                        else:
                            # Could be permissions or other error
                            results[ident] = f"⚠️ Check Failed: {str(e)[:50]}..."

                # 2. Files / Paths
                elif subtype in ["ADLS", "S3", "GCS", "DBFS", "LOCAL_FILE", "PARQUET_FILE", "CSV_FILE", "DELTA_PATH"] or "FILE" in subtype:
                    # DBFS / Volumes
                    if ident.startswith("dbfs:") or ident.startswith("/dbfs"):
                         path = ident.replace("dbfs:", "/dbfs") if ident.startswith("dbfs:") else ident
                         # Use dbfs api
                         try:
                             # We use the dbfs path format for get_status
                             check_path = ident if ident.startswith("dbfs:") else f"dbfs:{ident}"
                             self.client.dbfs.get_status(check_path)
                             results[ident] = "✅ Exists"
                         except Exception as e:
                             if "RESOURCE_DOES_NOT_EXIST" in str(e):
                                 results[ident] = "❌ Not Found"
                             else:
                                 results[ident] = f"⚠️ Error: {str(e)[:30]}"

                    elif ident.startswith("/Volumes") or ident.startswith("/Workspace"):
                        try:
                            self.client.files.get_metadata(ident)
                            results[ident] = "✅ Exists"
                        except Exception as e:
                             if "NOT_FOUND" in str(e):
                                 results[ident] = "❌ Not Found"
                             else:
                                 results[ident] = f"⚠️ Error: {str(e)[:30]}"
                    
                    elif "abfss" in ident or "s3" in ident:
                        # Cannot validate cloud paths without compute
                        results[ident] = "⚠️ Skipped (External)"
                    
                    else:
                        results[ident] = "❔ Unchecked"
                
                else:
                    results[ident] = "❔ Unchecked Type"

            except Exception as e:
                results[ident] = f"⚠️ Error: {str(e)}"
        
        return results
