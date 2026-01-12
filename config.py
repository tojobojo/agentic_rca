"""
Configuration module for the Agentic RCA System.
Manages secrets and environment-specific settings.
"""
import os
import tempfile
import logging
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator, ConfigDict
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

logger = logging.getLogger(__name__)

class Config(BaseModel):
    """Configuration settings for the RCA system."""
    
    model_config = ConfigDict(validate_assignment=True)
    
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
    temp_dir: str = Field(default_factory=lambda: os.path.join(tempfile.gettempdir(), "rca_git_cache"))
    
    # Anomaly Detection Thresholds
    anomaly_z_score_threshold: float = Field(default=3.0, gt=0)
    anomaly_drop_rate_threshold: float = Field(default=0.1, ge=0, le=1)
    anomaly_rejection_rate_threshold: float = Field(default=0.05, ge=0, le=1)
    
    # File Processing Limits
    max_file_size_mb: int = Field(default=1, gt=0)
    
    # LLM Retry Settings
    llm_max_retries: int = Field(default=3, ge=1, le=10)
    llm_retry_delay_seconds: int = Field(default=2, ge=1)
    
    @field_validator('databricks_host', 'gitlab_url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format if provided."""
        if v and not v.startswith(('http://', 'https://')):
            raise ValueError(f"URL must start with http:// or https://, got: {v}")
        return v
    
    def validate(self) -> List[str]:
        """Validate required configuration."""
        errors = []
        if not self.databricks_host:
            errors.append(
                "DATABRICKS_HOST is required. Set via environment variable or Databricks secret. "
                "Example: https://adb-1234567890123456.7.azuredatabricks.net"
            )
        if not self.databricks_token:
            errors.append(
                "DATABRICKS_TOKEN is required. Generate at: <workspace_url>/settings/tokens"
            )
        if not self.openai_api_key:
            errors.append(
                "OPENAI_API_KEY is required. Get your API key from: https://platform.openai.com/api-keys"
            )
        if not self.gitlab_url:
            errors.append(
                "GITLAB_URL is required for code discovery. Example: https://gitlab.com/your-org/your-repo.git"
            )
        if not self.gitlab_token:
            errors.append(
                "GITLAB_TOKEN is required for code discovery. Example: https://gitlab.com/your-org/your-repo.git"
            )
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
            anomaly_z_score_threshold=float(os.getenv("RCA_ANOMALY_Z_SCORE", "3.0")),
            anomaly_drop_rate_threshold=float(os.getenv("RCA_ANOMALY_DROP_RATE", "0.1")),
            anomaly_rejection_rate_threshold=float(os.getenv("RCA_ANOMALY_REJECTION_RATE", "0.05")),
            max_file_size_mb=int(os.getenv("RCA_MAX_FILE_SIZE_MB", "1")),
            llm_max_retries=int(os.getenv("RCA_LLM_MAX_RETRIES", "3")),
            llm_retry_delay_seconds=int(os.getenv("RCA_LLM_RETRY_DELAY", "2")),
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


def _get_or_create_spark():
    """Helper to get existing spark session or create new one."""
    from pyspark.sql import SparkSession
    try:
        spark = SparkSession.builder.getOrCreate()
        return spark
    except Exception:
        # Fallback for local testing if needed
        return None

def get_latest_run_id(job_id: int) -> int:
    """Fetch the latest run ID for a given job."""
    from databricks.sdk import WorkspaceClient
    
    config = get_config()
    client = WorkspaceClient(
        host=config.databricks_host,
        token=config.databricks_token
    )
    
    # List runs for the job, defaulting to latest first
    runs = list(client.jobs.list_runs(job_id=job_id, limit=1, expand_tasks=False))
    
    if not runs:
        raise ValueError(f"No runs found for Job ID {job_id}")
    
    return runs[0].run_id
