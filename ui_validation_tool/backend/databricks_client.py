from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Job
import logging
from typing import List, Dict, Any
from .config import get_config

logger = logging.getLogger(__name__)

class DatabricksService:
    def __init__(self):
        self.config = get_config()
        self.client = WorkspaceClient(
            host=self.config.databricks_host,
            token=self.config.databricks_token
        )

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
        Returns a list of dicts with task_key, task_type, and dependencies.
        """
        job_dict = self.get_job(job_id)
        settings = job_dict.get("settings", {})
        tasks = settings.get("tasks", [])
        
        # Sort tasks might be complicated due to DAG, but list order usually respects top-down creation
        # We'll just return them as is, UI can display dependencies
        
        simplified_tasks = []
        for t in tasks:
            task_key = t.get("task_key")
            task_type = "unknown"
            script_path = None
            
            if "notebook_task" in t:
                task_type = "notebook"
                script_path = t["notebook_task"].get("notebook_path")
            elif "spark_python_task" in t:
                task_type = "python"
                script_path = t["spark_python_task"].get("python_file")
            elif "sql_task" in t:
                task_type = "sql"
                # SQL tasks might not have a file path in the same way
            
            simplified_tasks.append({
                "task_key": task_key,
                "task_type": task_type,
                "script_path": script_path,
                "depends_on": [d.get("task_key") for d in t.get("depends_on", [])]
            })
            
        return simplified_tasks
