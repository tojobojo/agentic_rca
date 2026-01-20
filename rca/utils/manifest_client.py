"""
Manifest Client Module.
Reads lineage information from the centralized Manifest Table.
"""
from typing import Dict, Optional, Any
import json
import logging
from config.config import get_config, _get_or_create_spark

logger = logging.getLogger(__name__)

class ManifestClient:
    """
    Client for reading the RCA Manifest Table.
    """
    def __init__(self):
        self.config = get_config()
        self.spark = _get_or_create_spark()

    def get_latest_manifest(self, job_id: int) -> Dict[str, Any]:
        """
        Fetch the latest manifest for a given Job ID.
        
        Args:
            job_id: The Databricks Job ID.
            
        Returns:
            Dict containing the parsed manifest JSON.
            {'task_key': {'sources': [], 'targets': []}}
        """
        if not self.spark:
            logger.warning("Spark session unavailable. Cannot read manifest table.")
            return {}

        table_name = self.config.manifest_table
        
        try:
            # 1. Check if table exists
            if not self.spark.catalog.tableExists(table_name):
                logger.warning(f"Manifest table {table_name} does not exist.")
                return {}

            # 2. Query for latest entry for this job
            # Schema: id, job_id, manifest, version, date, created_by
            query = f"""
                SELECT manifest 
                FROM {table_name}
                WHERE job_id = '{job_id}'
                ORDER BY date DESC
                LIMIT 1
            """
            
            rows = self.spark.sql(query).collect()
            
            if not rows:
                logger.warning(f"No manifest found in {table_name} for Job ID {job_id}")
                return {}
            
            # 3. Parse JSON
            manifest_json_str = rows[0].manifest
            manifest = json.loads(manifest_json_str)
            
            logger.info(f"Successfully loaded manifest for Job {job_id} from {table_name}")
            return manifest

        except Exception as e:
            logger.error(f"Error reading manifest table: {e}")
            return {}

    def get_task_tables(self, job_id: int, task_key: str) -> Dict[str, list]:
        """
        Get source/target tables for a specific task from the manifest.
        """
        manifest = self.get_latest_manifest(job_id)
        if not manifest:
            return {"sources": [], "targets": []}
            
        task_data = manifest.get(task_key, {})
        return {
            "sources": task_data.get("sources", []),
            "targets": task_data.get("targets", [])
        }

    def resolve_tables_for_steps(
        self,
        job_id: int,
        task_keys: list,
        manifest_data: Optional[Dict] = None
    ) -> Dict[str, dict]:
        """
        Get source/target tables for multiple pipeline steps.
        Uses provided manifest_data or fetches latest from table.
        """
        result = {}
        
        # Use provided manifest or fetch fresh
        data = manifest_data
        if not data:
            data = self.get_latest_manifest(job_id)
            
        if not data:
             # Return empty structure for all keys
             return {k: {"sources": [], "targets": []} for k in task_keys}

        for key in task_keys:
            # Manifest is the Source of Truth
            result[key] = data.get(key, {"sources": [], "targets": []})
            
        return result
