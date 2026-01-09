"""
Validation Engine Module.
Responsible for:
1. READING pre-collected metrics from History Collector's Delta Table.
2. Detecting anomalies based on statistical deviation.

NOTE: This module does NOT collect metrics. Use HistoryCollector separately.
"""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
import statistics

from pipeline_parser import ParsedStep
from config import get_config

logger = logging.getLogger(__name__)


@dataclass
class StepMetrics:
    """Metrics for a single step execution (read from Delta)."""
    job_id: int
    task_key: str
    run_timestamp: str
    input_count: int
    output_count: int
    drop_count: int
    drop_rate: float


@dataclass
class Anomaly:
    """Represents a detected anomaly."""
    step: ParsedStep
    metrics: StepMetrics
    historical_avg: float
    deviation: float
    reason: str


class ValidationEngine:
    """
    The Watcher: Reads metrics from History Collector and detects anomalies.
    
    IMPORTANT: This engine READS from the metrics Delta table.
    Metrics must be pre-collected by running HistoryCollector after each pipeline run.
    """
    
    def __init__(self, spark_session=None):
        """
        Initialize the Validation Engine.
        
        Args:
            spark_session: Active Spark session. If None, will attempt to get from context.
        """
        self.config = get_config()
        self.spark = spark_session
        
        if self.spark is None:
            try:
                from pyspark.sql import SparkSession
                self.spark = SparkSession.builder.getOrCreate()
            except ImportError:
                logger.warning("PySpark not available. Running in mock mode.")
    
    def _get_latest_metrics(self, job_id: int, task_key: str) -> Optional[StepMetrics]:
        """
        Get the LATEST metrics for a step from the History Collector's Delta table.
        These metrics should have been collected AFTER the most recent run.
        """
        if self.spark is None:
            return None
        
        try:
            metrics_df = self.spark.table(self.config.metrics_table)
            latest = (
                metrics_df
                .filter((metrics_df.job_id == job_id) & (metrics_df.task_key == task_key))
                .orderBy("run_timestamp", ascending=False)
                .limit(1)
                .collect()
            )
            
            if not latest:
                return None
            
            row = latest[0].asDict()
            return StepMetrics(
                job_id=row.get("job_id", job_id),
                task_key=row.get("task_key", task_key),
                run_timestamp=row.get("run_timestamp", ""),
                input_count=row.get("input_count", 0),
                output_count=row.get("output_count", 0),
                drop_count=row.get("drop_count", 0),
                drop_rate=row.get("drop_rate", 0.0)
            )
        except Exception as e:
            logger.warning(f"Could not fetch latest metrics: {e}")
            return None
    
    def _get_historical_metrics(self, job_id: int, task_key: str, exclude_latest: bool = True) -> List[Dict]:
        """
        Retrieve historical metrics for a step from Delta table.
        
        Args:
            job_id: Databricks Job ID
            task_key: Step task key
            exclude_latest: If True, skip the most recent run (used for comparison)
        """
        if self.spark is None:
            return []
        
        try:
            history_df = self.spark.table(self.config.metrics_table)
            query = history_df.filter((history_df.job_id == job_id) & (history_df.task_key == task_key))
            query = query.orderBy("run_timestamp", ascending=False)
            
            if exclude_latest:
                # Skip the first row (latest) and take next 30
                query = query.limit(31)
                rows = query.collect()
                return [row.asDict() for row in rows[1:]]  # Skip first
            else:
                query = query.limit(30)
                return [row.asDict() for row in query.collect()]
        except Exception as e:
            logger.warning(f"Could not fetch history: {e}")
            return []
    
    def _detect_anomaly(
        self, 
        step: ParsedStep, 
        metrics: StepMetrics, 
        history: List[Dict]
    ) -> Optional[Anomaly]:
        """
        Detect if the current drop rate is anomalous compared to history.
        Uses Z-score based detection.
        """
        if not history or len(history) < 3:
            # Not enough history. Flag if drop rate > 20% for lossy steps
            if step.logic_type in ["join", "filter", "distinct"] and metrics.drop_rate > 0.2:
                return Anomaly(
                    step=step,
                    metrics=metrics,
                    historical_avg=0.0,
                    deviation=0.0,
                    reason=f"High drop rate ({metrics.drop_rate:.1%}) with insufficient history"
                )
            return None
        
        # Calculate historical statistics
        historical_rates = [h["drop_rate"] for h in history if h.get("drop_rate") is not None]
        if not historical_rates:
            return None
        
        avg_rate = statistics.mean(historical_rates)
        stdev_rate = statistics.stdev(historical_rates) if len(historical_rates) > 1 else 0.01
        
        # Calculate Z-score
        if stdev_rate > 0:
            z_score = (metrics.drop_rate - avg_rate) / stdev_rate
        else:
            z_score = 0 if metrics.drop_rate == avg_rate else 10
        
        # Flag if Z-score > 2.5 (outside 99% confidence) and drop is significant
        if z_score > 2.5 and metrics.drop_rate > avg_rate + 0.05:
            return Anomaly(
                step=step,
                metrics=metrics,
                historical_avg=avg_rate,
                deviation=z_score,
                reason=f"Drop rate {metrics.drop_rate:.1%} exceeds historical avg {avg_rate:.1%} (Z={z_score:.1f})"
            )
        
        return None
    
    def validate_step(self, job_id: int, step: ParsedStep) -> Optional[Anomaly]:
        """
        Validate a single step by reading pre-collected metrics and checking for anomalies.
        
        PREREQUISITE: HistoryCollector must have run after the pipeline completed.
        """
        # Read latest metrics from Delta (collected by HistoryCollector)
        metrics = self._get_latest_metrics(job_id, step.task_key)
        
        if metrics is None:
            logger.info("No metrics found. Run HistoryCollector first.")
            return None
        
        # Get historical metrics (excluding latest for comparison)
        history = self._get_historical_metrics(job_id, step.task_key, exclude_latest=True)
        
        # Detect anomaly
        anomaly = self._detect_anomaly(step, metrics, history)
        
        return anomaly
    
    def validate_all(self, job_id: int, steps: List[ParsedStep]) -> List[Anomaly]:
        """
        Validate all steps by reading pre-collected metrics and detecting anomalies.
        
        PREREQUISITE: HistoryCollector must have run after the pipeline completed.
        """
        anomalies = []
        
        logger.info("Reading metrics from History Collector...")
        
        for step in steps:
            logger.info(f"Checking step: {step.task_key}...")
            anomaly = self.validate_step(job_id, step)
            if anomaly:
                logger.warning(f"ANOMALY: {anomaly.reason}")
                anomalies.append(anomaly)
            else:
                logger.info("OK")
        
        return anomalies
