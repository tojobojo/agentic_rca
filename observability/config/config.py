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
import sys
import argparse

# Load environment variables from .env file if present
load_dotenv()

logger = logging.getLogger(__name__)

def _get_or_create_spark():
    """Helper to get existing spark session or create new one."""
    from pyspark.sql import SparkSession
    try:
        spark = SparkSession.builder.getOrCreate()
        return spark
    except Exception:
        # Fallback for local testing if needed
        return None

def get_dbutils(spark):
    """Safely get dbutils."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        return None

def get_runtime_args():
    """
    Unified argument parser for Databricks Jobs (CLI) and Interactive (Widgets).
    Prioritizes CLI arguments > Widgets > Defaults.
    """
    # 1. Setup Argparse (Values are optional strings to allow widget fallback)
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", type=str, required=False)
    parser.add_argument("--run-id", type=str, required=False)
    parser.add_argument("--manifest", type=str, required=False)
    parser.add_argument("--collect", action="store_true", help="Flag to specific collection mode")
    
    # Parse known args to suppress errors from internal Databricks flags
    cli_args, _ = parser.parse_known_args()
    
    # 2. Setup Widgets (Interactive fallback)
    spark = _get_or_create_spark()
    dbutils = get_dbutils(spark)
    
    # In interactive mode (or standard Databricks jobs), dbutils is usually available.
    # We initialize widgets just in case we are in interactive mode.
    if dbutils:
        try:
            dbutils.widgets.text("job_id", "", "1. Job ID (Required)")
            dbutils.widgets.text("run_id", "", "2. Run ID (Optional)")
            dbutils.widgets.dropdown("collect", "false", ["true", "false"], "3. Collect Metrics?")
            dbutils.widgets.text("manifest", "", "4. Manifest Path (Optional)")
        except: pass
    
    class Args:
        job_id = None
        run_id = None
        manifest = None
        collect = False
        
    final_args = Args()
    
    # 3. Resolution Strategy (CLI > Widget)
    
    # Job ID
    if cli_args.job_id:
        final_args.job_id = int(cli_args.job_id)
    elif dbutils:
        val = dbutils.widgets.get("job_id")
        final_args.job_id = int(val) if val and val.strip() else None
        
    # Run ID
    if cli_args.run_id:
        final_args.run_id = int(cli_args.run_id)
    elif dbutils:
        val = dbutils.widgets.get("run_id")
        final_args.run_id = int(val) if val and val.strip() else None
        
    # Manifest
    if cli_args.manifest:
        final_args.manifest = cli_args.manifest
    elif dbutils:
        val = dbutils.widgets.get("manifest")
        final_args.manifest = val if val and val.strip() else None
        
    # Collect
    if cli_args.collect:
        final_args.collect = True
    elif dbutils:
        val = dbutils.widgets.get("collect")
        final_args.collect = (val.lower() == "true") if val else False
        
    if not final_args.job_id:
        logger.warning("No Job ID provided via CLI (--job-id) or Widgets.")

    return final_args

class Config(BaseModel):
    """Configuration settings for the RCA system."""
    
    model_config = ConfigDict(validate_assignment=True)
    
    # Databricks Settings
    databricks_host: str = ""
    databricks_token: str = ""
    
    # Manifest Table (Source of Truth for Lineage)
    manifest_table: str = "rca_manifest_log"
    
    # History Table
    metrics_table: str = "rca_metrics_history"
    
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
    
    @field_validator('databricks_host')
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


        return errors
    
    def __repr__(self):
        """Safe representation that masks secrets."""
        return f"Config(host={self.databricks_host})"
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        return cls(
            databricks_host=os.getenv("DATABRICKS_HOST", ""),
            databricks_token=os.getenv("DATABRICKS_TOKEN", ""),

            manifest_table=os.getenv("RCA_MANIFEST_TABLE", "rca_manifest_log"),

            metrics_table=os.getenv("RCA_METRICS_TABLE", "rca_metrics_history"),
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
            spark = _get_or_create_spark()
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

                    # gitlab removed

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
