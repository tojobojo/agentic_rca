"""
Observability Collector Module.
The "Sentry" of the Agentic RCA system.
Responsible for collecting execution and data metrics after pipeline runs.

Supports:
1. Databricks Jobs (Task-based)
2. Delta Live Tables (Pipeline-based)
3. Delta Table Transaction Logs (Data changes)
"""
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from config.config import get_config, _get_or_create_spark

logger = logging.getLogger(__name__)

class MetricRecord(BaseModel):
    """Schema for rca.metrics_history"""
    run_id: str
    job_or_pipeline_id: str
    step_id: str  # task_key or table_name
    step_type: str  # 'task' or 'dlt_table'
    source_tables: List[str]
    target_table: str
    rows_in: int = Field(ge=0)
    rows_out: int = Field(ge=0)
    rows_rejected: int = Field(ge=0)
    operation_type: str  # INSERT, MERGE, etc.
    execution_mode: str  # batch, streaming
    timestamp: str

class ObservabilityCollector:
    """
    Collects observability metrics from various Databricks execution contexts.
    """
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.config = get_config()
        self.spark = spark or _get_or_create_spark()
        self.client = WorkspaceClient(
            host=self.config.databricks_host,
            token=self.config.databricks_token
        )
        
    
    def collect_job_metrics(self, run_id: int, job_id: int) -> List[MetricRecord]:
        """
        Collect metrics for a standard Databricks Job run.
        """
        logger.info(f"Collecting metrics for Job Run: {run_id} (Job {job_id})")
        
        # 1. Fetch Run Details
        try:
            run = self.client.jobs.get_run(run_id)
            # job_id passed explicitly, no need to fetch from run
        except Exception as e:
            logger.error(f"Failed to fetch run {run_id}: {e}")
            return []
        
        # Define run window for Delta History correlation
        run_start = run.start_time
        run_end = run.end_time or int(datetime.now().timestamp() * 1000)
        
        metrics = []
        
        # 2. Get Lineage (Source/Target Tables)
        # We use the existing lineage_client logic which supports a fallback manifest
        from utils.lineage_client import get_step_tables
        task_keys = [t.task_key for t in run.tasks or []]
        table_map = get_step_tables(run.job_id, task_keys) # Returns {task_key: {sources: [], targets: []}}
        
        # 3. Iterate through tasks
        for task in run.tasks or []:
            task_key = task.task_key
            
            # Detect Task Type
            task_type = "Unknown"
            if task.notebook_task: task_type = "Notebook"
            elif task.spark_python_task: task_type = "Python Script"
            elif task.spark_jar_task: task_type = "JAR"
            elif task.sql_task: task_type = "SQL"
            elif task.dbt_task: task_type = "dbt"
            elif task.pipeline_task: task_type = "DLT Pipeline"
            elif task.condition_task: task_type = "Condition (If/Else)"
            
            # Skip non-data tasks explicitly
            # if task.condition_task:
            #     logger.info(f"Skipping Task '{task_key}' (Type: {task_type}) - Logical control flow only.")
            #     continue

            tables = table_map.get(task_key, {})
            sources = tables.get("sources", [])
            targets = tables.get("targets", [])
            
            logger.info(f"Processing Task '{task_key}' (Type: {task_type}): Sources={sources}, Targets={targets}")
            
            if not targets:
                # If no targets known, we can't collect meaningful output metrics
                # unless we scan *everything*. Skipping for now.
                continue

            for target in targets:
                # Collect metrics for this target table
                record = self._collect_table_metrics(
                    run_id=str(run_id),
                    job_id=str(job_id),
                    step_id=task_key,
                    sources=sources,
                    target=target,
                    run_start_ts=run_start,
                    run_end_ts=run_end
                )
                metrics.append(record)
            
        return metrics

    def _collect_table_metrics(
        self, 
        run_id: str, 
        job_id: str, 
        step_id: str, 
        sources: List[str], 
        target: str,
        run_start_ts: int,
        run_end_ts: int
    ) -> MetricRecord:
        """
        Collect metrics for a specific table update.
        Tries Delta History first, falls back to Count.
        """
        timestamp = datetime.now().isoformat()
        
        # Default values
        rows_in = 0
        rows_out = 0
        rows_rejected = 0
        op_type = "UNKNOWN"
        
        # Strategy 1: Check Delta History
        # We look for a commit on this table that happened during the run window
        try:
            if self.spark.catalog.tableExists(target):
                # Retrieve history (last 5 commits to be safe)
                history = self.spark.sql(f"DESCRIBE HISTORY {target} LIMIT 5").collect()
                
                matched_commit = None
                for commit in history:
                    # commit.timestamp is usually a datetime object or string depending on DBR version
                    # Ensure we compare properly. standardized to millis if possible.
                    commit_ts = commit["timestamp"]
                    if isinstance(commit_ts, datetime):
                        commit_millis = int(commit_ts.timestamp() * 1000)
                    else:
                        # Fallback parsing if string
                        commit_millis = run_end_ts # Hack if parsing fails
                    
                    # Check if commit happened within run window (with small buffer)
                    # buffer = 60 seconds
                    if (run_start_ts - 60000) <= commit_millis <= (run_end_ts + 60000):
                        matched_commit = commit
                        break
                
                if matched_commit:
                    op_type = matched_commit["operation"]
                    op_metrics = matched_commit["operationMetrics"] or {}
                    
                    # Extract rows out (written)
                    rows_out = int(op_metrics.get("numOutputRows", 
                                   op_metrics.get("numTargetRowsInserted", 0)))
                    
                    # Extract rows in (read) - often harder to get from write commit
                    # sometimes "numSourceRows" exists
                    rows_in = int(op_metrics.get("numSourceRows", 0))
                    
                    logger.info(f"Found match in Delta History for {target}: {op_type}, Out={rows_out}")
                else:
                    logger.info(f"No matching commit in Delta History for {target} in window.")
                    # Fallback to current count
                    rows_out = self.spark.table(target).count()
                    op_type = "SNAPSHOT_COUNT"
            else:
                logger.warning(f"Table {target} does not exist in catalog.")
        
        except Exception as e:
            logger.warning(f"Error collecting Delta metrics for {target}: {e}")
            # Final fallback
            try:
                rows_out = self.spark.table(target).count()
            except:
                rows_out = 0

        # Attempt to get Input Count from Source Table (Snapshot)
        # Only if we didn't get it from Delta metrics
        if rows_in == 0 and sources:
            try:
                # Just count the first source table as a proxy
                rows_in = self.spark.table(sources[0]).count()
            except:
                pass

        return MetricRecord(
            run_id=str(run_id),
            job_or_pipeline_id=job_id,
            step_id=step_id,
            step_type='task',
            source_tables=sources,
            target_table=target,
            rows_in=rows_in,
            rows_out=rows_out,
            rows_rejected=rows_rejected,
            operation_type=op_type,
            execution_mode="batch", # simplified
            timestamp=timestamp
        )

    def save_metrics(self, metrics: List[MetricRecord]):
        """
        Persist metrics to the history table.
        """
        if not metrics:
            logger.info("No metrics to save.")
            return

        # Convert dataclasses to Rows
        from pyspark.sql import Row
        rows = [Row(**m.__dict__) for m in metrics]
        df = self.spark.createDataFrame(rows)
        
        table_name = self.config.metrics_table
        
        logger.info(f"Saving {len(metrics)} records to {table_name}")
        
        # Schema evolution might be needed if new fields are added
        (df.write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true")
           .saveAsTable(table_name))

