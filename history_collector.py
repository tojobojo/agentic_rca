"""
History Collector Module.
A standalone process that collects row count metrics after each pipeline run.
Designed to run as a scheduled Databricks Job or triggered by workflow events.

IMPORTANT: Uses the same table discovery (Lineage API / manifest) as the main RCA system.
"""
import argparse
import json
import os
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass

from config import get_config
from discovery_agent import DiscoveryAgent
from pipeline_parser import PipelineParser
from lineage_client import get_step_tables


@dataclass
class StepMetrics:
    """Metrics for a single step in the pipeline."""
    job_id: int
    run_id: str
    task_key: str
    run_timestamp: str
    input_count: int
    output_count: int
    drop_count: int
    drop_rate: float


class HistoryCollector:
    """
    Collects and stores row count metrics for pipeline steps.
    Uses the same Lineage API / manifest workflow as the main RCA system.
    """
    
    def __init__(self, spark_session=None):
        self.config = get_config()
        self.spark = spark_session
        
        if self.spark is None:
            try:
                from pyspark.sql import SparkSession
                self.spark = SparkSession.builder.getOrCreate()
            except ImportError:
                raise RuntimeError("PySpark required for History Collector")
    
    def _count_table(self, table_name: str) -> int:
        """Get row count for a table."""
        if not table_name:
            return 0
        
        try:
            # Handle both table names and paths
            if table_name.endswith(".parquet") or "/" in table_name:
                df = self.spark.read.parquet(table_name)
            else:
                df = self.spark.table(table_name)
            return df.count()
        except Exception as e:
            print(f"  Warning: Could not count {table_name}: {e}")
            return 0
    
    def collect_step_metrics(
        self,
        job_id: int,
        run_id: str,
        task_key: str,
        source_tables: List[str],
        target_tables: List[str]
    ) -> StepMetrics:
        """
        Collect metrics for a single step.
        Counts primary source and target tables.
        """
        timestamp = datetime.now().isoformat()
        
        # Count primary source (first in list)
        input_count = 0
        if source_tables:
            input_count = self._count_table(source_tables[0])
        
        # Count primary target (first in list)
        output_count = 0
        if target_tables:
            output_count = self._count_table(target_tables[0])
        
        # Calculate drop
        drop_count = max(0, input_count - output_count)
        drop_rate = drop_count / input_count if input_count > 0 else 0.0
        
        return StepMetrics(
            job_id=job_id,
            run_id=run_id,
            task_key=task_key,
            run_timestamp=timestamp,
            input_count=input_count,
            output_count=output_count,
            drop_count=drop_count,
            drop_rate=drop_rate
        )
    
    def save_metrics(self, metrics: List[StepMetrics]):
        """Save metrics to the history Delta table."""
        if not metrics:
            return
        
        from pyspark.sql import Row
        
        rows = [
            Row(
                job_id=m.job_id,
                run_id=m.run_id,
                task_key=m.task_key,
                run_timestamp=m.run_timestamp,
                input_count=m.input_count,
                output_count=m.output_count,
                drop_count=m.drop_count,
                drop_rate=m.drop_rate
            )
            for m in metrics
        ]
        
        df = self.spark.createDataFrame(rows)
        
        # Create or append to table
        table = self.config.metrics_table
        try:
            self.spark.sql(f"DESCRIBE TABLE {table}")
            df.write.format("delta").mode("append").saveAsTable(table)
        except:
            print(f"[Collector] Creating metrics table: {table}")
            df.write.format("delta").mode("overwrite").saveAsTable(table)
        
        print(f"[Collector] Saved {len(metrics)} step metrics to {table}")
    
    def run(
        self,
        job_id: int,
        run_id: str,
        gitlab_url: Optional[str] = None,
        branch: str = "main",
        manifest_path: Optional[str] = None
    ):
        """
        Main entry point for the collector.
        
        Args:
            job_id: Databricks Job ID
            run_id: Specific run ID to collect metrics for
            gitlab_url: GitLab repo URL (for step discovery)
            branch: Git branch
            manifest_path: Path to JSON manifest with table mappings
        """
        print("=" * 60)
        print("       HISTORY COLLECTOR")
        print("=" * 60)
        print(f"Job ID: {job_id}")
        print(f"Run ID: {run_id}")
        print(f"Manifest: {manifest_path or 'Using Lineage API'}")
        print("=" * 60)
        
        # Step 1: Discover steps from Job
        print("\n[Phase 1] DISCOVERY")
        print("-" * 40)
        
        if gitlab_url:
            discovery = DiscoveryAgent()
            steps = discovery.discover(job_id, gitlab_url, branch)
            task_keys = [s.task_key for s in steps]
        else:
            # Fallback: get task keys from latest run
            from databricks.sdk import WorkspaceClient
            client = WorkspaceClient(
                host=self.config.databricks_host,
                token=self.config.databricks_token
            )
            run = client.jobs.get_run(int(run_id))
            task_keys = [t.task_key for t in run.tasks or []]
        
        print(f"  Found {len(task_keys)} steps")
        
        # Step 2: Get table mappings (same as main system)
        print("\n[Phase 2] LINEAGE")
        print("-" * 40)
        
        manifest_data = None
        if manifest_path and os.path.exists(manifest_path):
            with open(manifest_path, 'r') as f:
                manifest_data = json.load(f)
            print(f"  Loaded manifest with {len(manifest_data)} mappings")
        
        table_mapping = get_step_tables(job_id, task_keys, fallback_to_manifest=manifest_data)
        
        # Step 3: Collect metrics for each step
        print("\n[Phase 3] COLLECTION")
        print("-" * 40)
        
        all_metrics = []
        for task_key in task_keys:
            tables = table_mapping.get(task_key, {})
            sources = tables.get("sources", [])
            targets = tables.get("targets", [])
            
            print(f"[Collector] {task_key}: {sources} -> {targets}")
            
            metrics = self.collect_step_metrics(
                job_id=job_id,
                run_id=run_id,
                task_key=task_key,
                source_tables=sources,
                target_tables=targets
            )
            
            print(f"  -> Input: {metrics.input_count:,} | Output: {metrics.output_count:,} | Drop: {metrics.drop_rate:.1%}")
            all_metrics.append(metrics)
        
        # Step 4: Save to Delta
        print("\n[Phase 4] SAVE")
        print("-" * 40)
        self.save_metrics(all_metrics)
        
        print("\n" + "=" * 60)
        print("       COLLECTION COMPLETE")
        print("=" * 60)


def main():
    """CLI entry point for standalone execution."""
    parser = argparse.ArgumentParser(
        description="History Collector for RCA System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python history_collector.py --job-id 12345 --run-id abc123 --manifest tables.json
  python history_collector.py --job-id 12345 --run-id abc123 --gitlab-url https://...
        """
    )
    
    parser.add_argument(
        "--job-id",
        type=int,
        required=True,
        help="Databricks Job ID"
    )
    
    parser.add_argument(
        "--run-id",
        type=str,
        required=True,
        help="Specific run ID to collect metrics for"
    )
    
    parser.add_argument(
        "--gitlab-url",
        type=str,
        default=None,
        help="GitLab repository URL (for step discovery)"
    )
    
    parser.add_argument(
        "--branch",
        type=str,
        default="main",
        help="Git branch (default: main)"
    )
    
    parser.add_argument(
        "--manifest",
        type=str,
        default=None,
        help="Path to JSON manifest with table mappings per step"
    )
    
    args = parser.parse_args()
    
    collector = HistoryCollector()
    collector.run(
        job_id=args.job_id,
        run_id=args.run_id,
        gitlab_url=args.gitlab_url,
        branch=args.branch,
        manifest_path=args.manifest
    )


if __name__ == "__main__":
    main()
