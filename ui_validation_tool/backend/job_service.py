"""Job operations service for Databricks."""
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class JobService:
    """Handles Databricks job operations."""
    
    def __init__(self, client: WorkspaceClient, config):
        self.client = client
        self.config = config
    
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
