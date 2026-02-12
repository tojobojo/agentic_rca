"""
Observability Collector Module.
The "Sentry" of the Agentic RCA system.
Responsible for collecting execution and data metrics after pipeline runs.

Supports:
1. Databricks Jobs (Task-based) (Primary)
2. Incremental Metric Collection (Sync)
3. Direct Data Quality Checks (Counts, Nulls)
"""
import logging
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config.config import get_config, _get_or_create_spark
from utils.manifest_client import ManifestClient
from utils.manifest_client import ManifestClient

logger = logging.getLogger(__name__)

class MetricRecord(BaseModel):
    """Schema for rca.metrics_history"""
    run_id: str
    job_id: str
    task_key: str
    target_table: str
    asset_type: str # TABLE, FILE, ADLS, etc.
    metric_type: str # SOURCE or TARGET
    
    # Ordering & Retry
    step_index: int = 0
    attempt_number: int = 0
    execution_status: str # SUCCESS, FAILED (of the task itself)
    duration_ms: int = Field(ge=0, default=0)
    
    # Core Counts
    rows_total: int = Field(ge=0, default=0)
    rows_null_vital: Dict[str, int] = Field(default_factory=dict)
    distinct_counts: Dict[str, int] = Field(default_factory=dict)
    dq_validation_results: Dict[str, Any] = Field(default_factory=dict) # {"rule_name": "PASS" | "FAIL (count)"}
    columns: List[str] = Field(default_factory=list) # Schema Snapshot
    
    # Metadata
    timestamp: str
    collection_status: str # "SUCCESS", "FAILED" (of the collection process)
    filter_value: str = "" # Value used for filtering (e.g. run_id, date)

class ObservabilityCollector:
    """
    Collects observability metrics by directly querying data sources.
    Uses 'Sync' logic to ensure history is contiguous.
    """
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.config = get_config()
        self.spark = spark or _get_or_create_spark()
        self.client = WorkspaceClient(
            host=self.config.databricks_host,
            token=self.config.databricks_token
        )
        self.manifest_client = ManifestClient()
        
    def sync_metrics(self, job_id: int):
        """
        Main Entry Point.
        Ensures metrics are up-to-date for the given job.
        1. Check max(run_id) in metrics history.
        2. If empty, backfill last 7 runs.
        3. Else, forward fill (all runs > max_run_id).
        """
        logger.info(f"Starting Metrics Sync for Job {job_id}")
        
        # 1. Get Watermark
        max_run_id = self._get_last_collected_run_id(job_id)
        
        runs_to_process = []
        
        if max_run_id is None:
            logger.info("No existing metrics found. Triggering Backfill (Last 7 Runs).")
            # List last 7 runs
            runs = list(self.client.jobs.list_runs(job_id=job_id, limit=7, expand_tasks=True))
            runs_to_process = sorted(runs, key=lambda r: r.start_time)
        else:
            logger.info(f"Found existing metrics up to Run {max_run_id}. Checking for new runs...")
            # List runs since (simulated by fetching recent and filtering)
            # Databricks API doesn't support "min_run_id" easily, so we fetch limit=20 and filter
            runs = list(self.client.jobs.list_runs(job_id=job_id, limit=20, expand_tasks=True))
            # Filter strictly > max_run_id
            runs_to_process = [r for r in runs if r.run_id > max_run_id]
            runs_to_process = sorted(runs_to_process, key=lambda r: r.start_time)
            
        if not runs_to_process:
            logger.info("Metrics are already up to date.")
            return

        logger.info(f"Found {len(runs_to_process)} runs to process.")
        
        # 2. Process Sync
        manifest_data = self.manifest_client.get_latest_manifest(job_id)
        if not manifest_data:
            logger.warning("No manifest available. Cannot process metrics.")
            return

        for run in runs_to_process:
            try:
                self._process_run(run, manifest_data)
            except Exception as e:
                logger.error(f"Failed to process Run {run.run_id}: {e}")
                # Continue to next run? Or stop? 
                # Ideally continue to try and fill gaps, but might imply systemic issue.
                continue

    def _get_last_collected_run_id(self, job_id: int) -> Optional[int]:
        """Query metrics table for max run_id."""
        table = self.config.metrics_table
        if not self.spark.catalog.tableExists(table):
            return None
            
        try:
            # We assume run_id is stored as BIGINT or String. max() works for both usually.
            row = self.spark.sql(f"SELECT MAX(run_id) as max_id FROM {table} WHERE job_id = '{job_id}'").collect()
            if row and row[0].max_id:
                return int(row[0].max_id)
        except Exception:
            return None
        return None

    def _process_run(self, run, manifest_data: Dict):
        """Collect metrics for a single run using the manifest."""
        logger.info(f"--> Processing Run {run.run_id} ({run.start_time})")
        
        metrics = []
        
        tasks_map = {}
        for t in (run.tasks or []):
            if t.task_key not in tasks_map:
                tasks_map[t.task_key] = []
            tasks_map[t.task_key].append(t)

        # Cache for stats calculated in this run to avoid double-counting 
        # Key: (target_table_name, run_id) -> val: (rows_total, null_map, distinct_map, status)
        stats_cache = {}

        # 1. Driver Loop: Manifest (Source of Truth & Order)
        # We iterate the manifest to strictly follow the defined pipeline steps.
        for index, (task_key, task_info) in enumerate(manifest_data.items()):
            if task_key == "source_files": continue

            # 2. Find matching executions in this run
            matching_tasks = tasks_map.get(task_key, [])
            
            if not matching_tasks:
                logger.warning(f"Task {task_key} defined in manifest but not found in Run {run.run_id}.")
                continue
            
            # 3. Process each attempt/occurance (e.g. Retries, repairs)
            for task_attempt in matching_tasks:
                # Attempt Extraction
                attempt_number = 0
                if hasattr(task_attempt, "attempt_number"):
                    attempt_number = task_attempt.attempt_number
                
                execution_status = "UNKNOWN"
                if task_attempt.state and task_attempt.state.result_state:
                     execution_status = task_attempt.state.result_state.value 
                
                duration_ms = 0
                if hasattr(task_attempt, "execution_duration"):
                    duration_ms = task_attempt.execution_duration

                # Assets to check
                targets = task_info.get("targets", [])
                sources = task_info.get("sources", [])
                metric_config = task_info.get("metric_config", {})
                dq_rules_map = task_info.get("dq_rules", {}) # { target_table: [rules] }
                
                assets_to_check = [
                    (t, "TARGET") for t in targets
                ] + [
                    (s, "SOURCE") for s in sources
                ]
                
                for asset_entry, asset_metric_type in assets_to_check:
                    if not isinstance(asset_entry, dict):
                         continue

                    target = asset_entry.get("name")
                    target_type = asset_entry.get("type", "UNKNOWN")

                    table_conf = metric_config.get(target)
                    
                    # Optimization: Check Cache
                    cache_key = (target, run.run_id)
                    cached_stats = stats_cache.get(cache_key)

                    if cached_stats:
                        logger.info(f"Using cached metrics for {target} (Run {run.run_id})")
                        row_count, null_map, distinct_map, status, cols, dq_res = cached_stats
                        
                        metric = MetricRecord(
                            run_id=str(run.run_id),
                            job_id=str(run.job_id),
                            task_key=task_key,
                            target_table=target,
                            asset_type=target_type,
                            metric_type=asset_metric_type,
                            step_index=index,
                            attempt_number=attempt_number,
                            execution_status=execution_status,
                            duration_ms=duration_ms,
                            rows_total=row_count,
                            rows_null_vital=null_map,
                            distinct_counts=distinct_map,
                            dq_validation_results=dq_res,
                            columns=cols,
                            timestamp=str(datetime.now()),
                            collection_status=status
                        )
                    else:
                        # Calculate New
                        metric = self._collect_table_metrics(
                            run_id=run.run_id,
                            job_id=run.job_id,
                            task_key=task_key,
                            target=target,
                            target_type=target_type,
                            asset_type=target_type,
                            metric_type=asset_metric_type,
                            step_index=index,
                            attempt_number=attempt_number,
                            execution_status=execution_status,
                            duration_ms=duration_ms,
                            run_date_col=table_conf.get("run_id_column") if table_conf else None,
                            date_col=table_conf.get("date_column") if table_conf else None,
                            table_conf=table_conf,
                            dq_rules=dq_rules_map.get(target, []),
                            load_type=asset_entry.get("load_type", "FULL_REFRESH"),
                            filter_column=asset_entry.get("filter_column", "")
                        )
                        # Cache the data part
                        stats_cache[cache_key] = (metric.rows_total, metric.rows_null_vital, metric.distinct_counts, metric.collection_status, metric.columns, metric.dq_validation_results)
                        
                    metrics.append(metric)
        
        # Save batch
        if metrics:
            self.save_metrics(metrics)

    def _collect_table_metrics(
        self,
        run_id: int,
        job_id: int,
        task_key: str,
        target: str,
        asset_type: str = "UNKNOWN",
        metric_type: str = "TARGET",
        step_index: int = 0,
        attempt_number: int = 0,
        execution_status: str = "UNKNOWN",
        duration_ms: int = 0,
        target_type: str = "UNKNOWN", # Redundant but kept for arg matching
        run_date_col: Optional[str] = None,
        date_col: Optional[str] = None,
        table_conf: Optional[Dict[str, Any]] = None,
        dq_rules: List[Dict] = None,
        load_type: str = "FULL_REFRESH",
        filter_column: str = ""
    ) -> MetricRecord:
        
        row_count = 0
        null_map = {}
        distinct_map = {}
        dq_results = {}
        status = "FAILED"
        filter_val_str = ""
        
        try:
            # 1. Load Data (Handle Table vs Path)
            df = None
            # 1. Load Data (Strict Type Mapping)
            df = None
            
            # Helper to normalize type check
            tt = target_type.upper()
            
            if "TABLE" in tt or tt == "JDBC_DB":
                 if self.spark.catalog.tableExists(target):
                    df = self.spark.table(target)
            elif "PARQUET" in tt:
                 df = self.spark.read.format("parquet").load(target)
            elif "CSV" in tt:
                 df = self.spark.read.format("csv").option("header", "true").load(target)
            elif "JSON" in tt:
                 df = self.spark.read.format("json").load(target)
            else:
                 # Default for ADLS, S3, DBFS, DELTA_PATH is strict Delta
                 # We assume if the user didn't say "PARQUET_FILE", they mean Delta Lake.
                 df = self.spark.read.format("delta").load(target)


            if df:
                # 2. Filter by Run ID or Date
                filtered_df = df
                
                # Apply explicit Load Type filtering
                if load_type == "APPEND" and filter_column:
                    try:
                        # Case 1: Filter by Run ID (if column implies run_id)
                        if "run" in filter_column.lower() and "id" in filter_column.lower():
                             logger.info(f"Filtering {target} by Run ID: {filter_column} == {run_id}")
                             filtered_df = df.filter(F.col(filter_column) == str(run_id))
                             filter_val_str = str(run_id)
                        
                        # Case 2: Filter by specific date (if explicitly provided - rare)
                        # Case 3: Filter by MAX value (latest slice)
                        else:
                             logger.info(f"Filtering {target} by MAX({filter_column})")
                             # Check if column exists
                             if filter_column in df.columns:
                                 max_val_row = df.agg(F.max(F.col(filter_column)).alias("max_val")).collect()
                                 if max_val_row and max_val_row[0]["max_val"]:
                                     max_val = max_val_row[0]["max_val"]
                                     logger.info(f"  -> Max Value: {max_val}")
                                     filtered_df = df.filter(F.col(filter_column) == max_val)
                                     filter_val_str = str(max_val)
                                 else:
                                     logger.warning(f"Could not determine max value for {filter_column}. Using full table.")
                             else:
                                 logger.warning(f"Filter column {filter_column} not found in {target}. Using full table.")

                    except Exception as filter_err:
                        logger.warning(f"Failed to apply APPEND filter on {target}: {filter_err}. Using full table.")
                        filtered_df = df

                # Fallback to old config-based filtering (legacy)
                elif run_date_col and run_date_col in df.columns:
                    filtered_df = df.filter(F.col(run_date_col) == str(run_id))
                    
                # 3. Metrics Config
                if not table_conf:
                    # No fallback - relying on Manifest config
                    logger.warning(f"Metrics config missing for {target}. Standard metrics only.")
                    table_conf = {}

                # 4. Deep Inspection (Monitor Everything)
                # We dynamically build aggregations for ALL columns.
                agg_exprs = [F.count("*").alias("total_rows")]
                
                all_cols = df.columns
                # Sort columns for consistent schema hashing if needed
                all_cols.sort()
                
                # Heuristic: Only check distincts for likely categorical columns (String/Integer) 
                # to avoid exploding performance on high-cardinality IDs or timestamps.
                # Actually, user said "Monitor all columns", but distinct on a unique ID is expensive and useless (== count).
                # We will check Nulls for EVERYTHING.
                # We will check Distincts for string cols not ending in 'id' (heuristic).
                
                for col_name in all_cols:
                    # 1. Null Checks (Everyone gets this)
                    agg_exprs.append(F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"null_{col_name}"))
                    
                    agg_exprs.append(F.countDistinct(col_name).alias(f"distinct_{col_name}"))

                # 3. DQ RULE CHECKS (Dynamic SQL Generation)
                dq_exprs = []
                # DISABLED: Data Quality Checks
                if False and dq_rules:
                    for i, rule in enumerate(dq_rules):
                        col = rule.get("column")
                        rtype = rule.get("type")
                        
                        # Label for the aggregation result
                        rule_id = f"dq_rule_{i}" 
                        
                        cond = None
                        if rtype == "not_null":
                            cond = F.col(col).isNull()
                        elif rtype == "range":
                            mn = float(rule.get("min", "-inf"))
                            mx = float(rule.get("max", "inf"))
                            cond = (F.col(col) < mn) | (F.col(col) > mx)
                        elif rtype == "accepted_values":
                            vals = rule.get("values", [])
                            cond = ~F.col(col).isin(vals)
                        elif rtype == "regex":
                            pat = rule.get("value") # 'value' or 'pattern'
                            if pat: cond = ~F.col(col).rlike(pat)
                        elif rtype == "row_count":
                             # Row Count is a table check, not a row check.
                             # We handle it post-aggregation
                             pass

                        if cond is not None:
                            # Count failing rows
                            dq_exprs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(rule_id))

                # Execute Single Pass Aggregation
                final_agg = agg_exprs + dq_exprs
                result_row = filtered_df.agg(*final_agg).collect()[0]
                
                # Parse Results
                row_count = result_row["total_rows"]
                
                # ... Standard Parsing ...
                for col_name in all_cols:
                    null_val = result_row[f"null_{col_name}"]
                    dist_val = result_row[f"distinct_{col_name}"]
                    
                    null_map[col_name] = null_val if null_val is not None else 0
                    distinct_map[col_name] = dist_val if dist_val is not None else 0

                # ... DQ Parsing ...
                if dq_rules:
                    for i, rule in enumerate(dq_rules):
                        rname = f"{rule.get('column')} {rule.get('type')}"
                        rtype = rule.get("type")
                        
                        if rtype == "row_count":
                             min_r = int(rule.get("min", 0))
                             if row_count < min_r:
                                 dq_results[rname] = f"FAIL: Found {row_count} < Min {min_r}"
                             else:
                                 dq_results[rname] = "PASS"
                        else:
                             rule_id = f"dq_rule_{i}"
                             fail_count = result_row[rule_id]
                             if fail_count and fail_count > 0:
                                 dq_results[rname] = f"FAIL: {fail_count} invalid rows"
                             else:
                                 dq_results[rname] = "PASS"
                                          
                status = "SUCCESS"
            else:
                 logger.warning(f"Table/Path {target} could not be loaded.")
                 status = "LOAD_FAILED"

        except Exception as e:
            logger.error(f"Error checking {target}: {e}")
            status = f"ERROR: {str(e)}"

        return MetricRecord(
            run_id=str(run_id),
            job_id=str(job_id),
            task_key=task_key,
            target_table=target,
            asset_type=asset_type,
            metric_type=metric_type,
            step_index=step_index,
            attempt_number=attempt_number,
            execution_status=execution_status,
            duration_ms=duration_ms,
            rows_total=row_count,
            rows_null_vital=null_map,
            distinct_counts=distinct_map,
            dq_validation_results=dq_results,
            columns=all_cols if status == "SUCCESS" else [],
            timestamp=str(datetime.now()),
            collection_status=status,
            filter_value=filter_val_str
        )

    def save_metrics(self, metrics: List[MetricRecord]):
        """Persist to Delta."""
        if not metrics: return
        
        from pyspark.sql import Row
        rows = [Row(**m.model_dump()) for m in metrics] # v2
        df = self.spark.createDataFrame(rows)
        
        (df.write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true")
           .saveAsTable(self.config.metrics_table))

from datetime import datetime
