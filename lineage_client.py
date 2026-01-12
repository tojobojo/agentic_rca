"""
Lineage Client Module.
Integrates with Unity Catalog Lineage API to discover table dependencies.
"""
from typing import Dict, List, Optional
from dataclasses import dataclass

from config import get_config, _get_or_create_spark
import logging

logger = logging.getLogger(__name__)


@dataclass
class TableLineage:
    """Lineage information for a table."""
    table_name: str
    upstream_tables: List[str]  # Tables this table reads from
    downstream_tables: List[str]  # Tables that read from this table


class LineageClient:
    """
    Client for Unity Catalog Lineage API.
    Discovers source/target tables for pipeline steps.
    """
    
    def __init__(self):
        self.config = get_config()
        self._client = None
    
    def _get_client(self):
        """Initialize Databricks SDK client."""
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient(
                host=self.config.databricks_host,
                token=self.config.databricks_token
            )
        return self._client
    
    def get_table_lineage(self, table_name: str) -> Optional[TableLineage]:
        """
        Get lineage for a specific table.
        
        Args:
            table_name: Fully qualified table name (catalog.schema.table)
        
        Returns:
            TableLineage with upstream/downstream tables
        """
        try:
            client = self._get_client()
            
            # Call Unity Catalog Lineage API
            # Note: This uses the Table Lineage endpoint
            response = client.api_client.do(
                "GET",
                f"/api/2.0/lineage-tracking/table-lineage?table_name={table_name}"
            )
            
            upstream = []
            downstream = []
            
            # Parse upstream (tables this table reads from)
            for edge in response.get("upstreams", []):
                if edge.get("tableInfo"):
                    upstream.append(edge["tableInfo"].get("name", ""))
            
            # Parse downstream (tables that read from this table)
            for edge in response.get("downstreams", []):
                if edge.get("tableInfo"):
                    downstream.append(edge["tableInfo"].get("name", ""))
            
            return TableLineage(
                table_name=table_name,
                upstream_tables=upstream,
                downstream_tables=downstream
            )
        except Exception as e:
            logger.warning("Lineage API error for %s: %s", table_name, e)
            return None
    
    def get_job_lineage(self, job_id: int, run_id: Optional[str] = None) -> Dict[str, dict]:
        """
        Get lineage for all tables touched by a job run.
        
        Args:
            job_id: Databricks Job ID
            run_id: Optional specific run ID
        
        Returns:
            Dict mapping task_key -> {"sources": [...], "targets": [...]}
        """
        try:
            client = self._get_client()
            
            # Get job runs
            if run_id:
                runs = [client.jobs.get_run(run_id)]
            else:
                runs = list(client.jobs.list_runs(job_id=job_id, limit=1))
            
            if not runs:
                logger.info("No runs found for job %s", job_id)
                return {}
            
            latest_run = runs[0]
            table_mapping = {}
            
            # For each task in the run, query lineage
            for task in latest_run.tasks or []:
                task_key = task.task_key
                
                # Try to get lineage from run context
                # This would require the run to have been instrumented
                # or we query the Lineage API for tables modified during this run
                
                # Alternative: Query information_schema for tables modified during run window
                table_mapping[task_key] = {
                    "sources": [],
                    "targets": []
                }
                
                # If task has cluster info, we can query Spark lineage
                if task.cluster_instance:
                    cluster_id = task.cluster_instance.cluster_id
                    # Additional lineage query could go here
            
            return table_mapping
        except Exception as e:
            logger.warning("Could not fetch job lineage: %s", e)
            return {}
    
    def get_tables_from_information_schema(
        self, 
        catalog: str = "*",
        modified_after: Optional[str] = None
    ) -> Dict[str, List[str]]:
        """
        Query information_schema for recently modified tables.
        Fallback method when Lineage API is not available.
        
        Args:
            catalog: Catalog to search (* for all)
            modified_after: ISO timestamp to filter by modification time
        
        Returns:
            Dict of catalog.schema.table -> [columns]
        """
        try:
            spark = _get_or_create_spark()
            if not spark:
                logger.warning("Spark session not available")
                return {}
            
            query = """
                SELECT 
                    table_catalog || '.' || table_schema || '.' || table_name as full_name,
                    last_altered
                FROM system.information_schema.tables
                WHERE 1=1
            """
            
            if catalog != "*":
                query += f" AND table_catalog = '{catalog}'"
            
            if modified_after:
                query += f" AND last_altered > '{modified_after}'"
            
            query += " ORDER BY last_altered DESC LIMIT 100"
            
            rows = spark.sql(query).collect()
            return {row.full_name: [] for row in rows}
        except Exception as e:
            logger.warning("Could not query information_schema: %s", e)
            return {}


def get_step_tables(
    job_id: int,
    task_keys: List[str],
    fallback_to_manifest: Optional[Dict] = None
) -> Dict[str, dict]:
    """
    Get source/target tables for pipeline steps.
    
    Priority:
    1. Unity Catalog Lineage API
    2. Fallback manifest (if provided)
    3. Empty (tables unknown)
    
    Args:
        job_id: Databricks Job ID
        task_keys: List of task keys to resolve
        fallback_manifest: Optional explicit table mapping
    
    Returns:
        Dict mapping task_key -> {"sources": [...], "targets": [...]}
    """
    result = {}
    
    # Try Lineage API first
    try:
        client = LineageClient()
        lineage_result = client.get_job_lineage(job_id)
        
        for key in task_keys:
            if key in lineage_result:
                result[key] = lineage_result[key]
    except Exception as e:
        logger.warning("Lineage API unavailable: %s", e)
    
    # Fill missing with fallback manifest
    if fallback_to_manifest:
        for key in task_keys:
            if key not in result:
                result[key] = fallback_to_manifest.get(key, {"sources": [], "targets": []})
    
    # Fill remaining with empty
    for key in task_keys:
        if key not in result:
            result[key] = {"sources": [], "targets": []}
    
    return result
