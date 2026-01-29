"""Databricks client facade - delegates to specialized services."""
from databricks.sdk import WorkspaceClient
from typing import List, Dict, Any
import logging

from .config import get_config
from .job_service import JobService
from .code_retriever import CodeRetriever
from .asset_validator import AssetValidator
from .manifest_service import ManifestService

logger = logging.getLogger(__name__)


class DatabricksService:
    """
    Facade for Databricks operations.
    Delegates to specialized services while maintaining backward compatibility.
    """
    
    def __init__(self):
        self.config = get_config()
        self.client = WorkspaceClient()
        
        # Initialize services
        self.job_service = JobService(self.client, self.config)
        self.code_retriever = CodeRetriever(self.client, self.config)
        self.asset_validator = AssetValidator(self.client, self.config, self._get_spark)
        self.manifest_service = ManifestService(self.config, self._get_spark)
    
    def _get_spark(self):
        """
        Attempts to return a DatabricksSession if available (Serverless).
        Falls back to None if not in a Databricks environment.
        """
        try:
            from databricks.connect import DatabricksSession
            spark = DatabricksSession.builder.getOrCreate()
            return spark
        except ImportError:
            logger.warning("databricks-connect not available. Spark operations will be skipped.")
            return None
        except Exception as e:
            logger.warning(f"Could not create Spark session: {e}")
            return None
    
    # Job operations - delegate to JobService
    def get_job(self, job_id: int) -> Dict[str, Any]:
        """Fetch full job definition."""
        return self.job_service.get_job(job_id)
    
    def get_job_tasks(self, job_id: int) -> List[Dict[str, Any]]:
        """Get unique tasks from a job."""
        return self.job_service.get_job_tasks(job_id)
    
    # Code retrieval - delegate to CodeRetriever
    def get_task_code(self, task: Dict[str, Any]) -> Dict[str, str]:
        """Retrieves the code for a given task."""
        return self.code_retriever.get_task_code(task)
    
    # Asset validation - delegate to AssetValidator
    def validate_assets(self, assets: List[Dict[str, Any]]) -> Dict[str, str]:
        """Validates the existence of the provided assets."""
        return self.asset_validator.validate_assets(assets)
    
    def get_asset_columns(self, identifier: str) -> List[str]:
        """Fetches column names for a given table identifier."""
        return self.asset_validator.get_asset_columns(identifier)
    
    # Manifest operations - delegate to ManifestService
    def load_latest_manifest(self, table_name: str, job_id: str) -> Dict[str, Any]:
        """Loads the latest manifest for a given job_id."""
        return self.manifest_service.load_latest_manifest(table_name, job_id)
    
    def save_manifest_to_table(self, table_name: str, manifest: Dict[str, Any], 
                               job_id: str, version: str = "1.0", status: str = "DRAFT") -> str:
        """Saves the generated manifest to a Delta table."""
        # Get current user for manifest metadata
        try:
            current_user = self.client.current_user.me().user_name
        except:
            current_user = "unknown"
        
        return self.manifest_service.save_manifest_to_table(
            table_name=table_name,
            manifest=manifest,
            job_id=job_id,
            version=version,
            status=status,
            current_user=current_user
        )
