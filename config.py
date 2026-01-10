"""
Configuration module for the Agentic RCA System.
Manages secrets and environment-specific settings.
"""
import os
import tempfile
import logging
from dataclasses import dataclass
from typing import Optional, List

logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Configuration settings for the RCA system."""
    
    # Databricks Settings
    databricks_host: str = ""
    databricks_token: str = ""
    
    # GitLab Settings
    gitlab_url: str = ""
    gitlab_token: str = ""
    
    # OpenAI Settings
    openai_api_key: str = ""
    openai_model: str = "gpt-4o"
    
    # History Table
    metrics_table: str = "rca_catalog.default.metrics_history"
    
    # Temp Directory for Git Clone (cross-platform)
    temp_dir: str = os.path.join(tempfile.gettempdir(), "rca_git_cache")
    
    def validate(self) -> List[str]:
        """Validate required configuration."""
        errors = []
        if not self.databricks_host:
            errors.append("DATABRICKS_HOST is required")
        if not self.databricks_token:
            errors.append("DATABRICKS_TOKEN is required")
        if not self.openai_api_key:
            errors.append("OPENAI_API_KEY is required")
        if not self.gitlab_url:
            errors.append("GITLAB_URL is required for code discovery")
        return errors
    
    def __repr__(self):
        """Safe representation that masks secrets."""
        return f"Config(host={self.databricks_host}, model={self.openai_model})"
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            databricks_host=os.getenv("DATABRICKS_HOST", ""),
            databricks_token=os.getenv("DATABRICKS_TOKEN", ""),
            gitlab_url=os.getenv("GITLAB_URL", ""),
            gitlab_token=os.getenv("GITLAB_TOKEN", ""),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4o"),
            metrics_table=os.getenv("RCA_METRICS_TABLE", "rca_catalog.default.metrics_history"),
            temp_dir=os.getenv("RCA_TEMP_DIR", os.path.join(tempfile.gettempdir(), "rca_git_cache")),
        )

    @classmethod
    def from_databricks_secrets(cls, scope: str = "rca-secrets") -> "Config":
        """
        Load configuration from Databricks Secrets.
        This is the recommended method when running on Databricks.
        """
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            dbutils = None
            # Get dbutils in Databricks environment
            try:
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(spark)
            except ImportError:
                # Fallback for newer Databricks runtimes
                import IPython
                dbutils = IPython.get_ipython().user_ns.get("dbutils")
            
            if dbutils:
                return cls(
                    databricks_host=spark.conf.get("spark.databricks.workspaceUrl", ""),
                    databricks_token=dbutils.secrets.get(scope, "databricks_token"),
                    gitlab_url=dbutils.secrets.get(scope, "gitlab_url"),
                    gitlab_token=dbutils.secrets.get(scope, "gitlab_token"),
                    openai_api_key=dbutils.secrets.get(scope, "openai_api_key"),
                )
        except Exception as e:
            logger.warning(f"Could not load from Databricks secrets: {e}")
        
        # Fallback to environment
        return cls.from_env()


# Global config instance (lazy loaded)
_config: Optional[Config] = None

def get_config() -> Config:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        # Try Databricks secrets first, fallback to env
        try:
            _config = Config.from_databricks_secrets()
        except:
            _config = Config.from_env()
    return _config

