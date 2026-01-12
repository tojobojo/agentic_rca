"""
Anomaly Detection Engine Module.
The "Monitor" of the Agentic RCA system.
Reads metrics from existing history and detects anomalies using:
1. Statistical deviation (Z-score)
2. Absolute thresholds (e.g. massive drop)
3. Execution-aware rules (e.g. DELETE operations)
"""
import logging
from typing import List, Optional, Dict
import statistics
from pydantic import BaseModel, Field, field_validator

from config import get_config, _get_or_create_spark
from execution_context import ExecutionContext

logger = logging.getLogger(__name__)

class Anomaly(BaseModel):
    """Represents a detected anomaly."""
    run_id: str
    step_id: str
    metric_name: str  # 'drop_rate', 'rows_rejected', etc.
    current_value: float = Field(ge=0)
    historical_avg: float = Field(ge=0)
    deviation_z_score: float
    severity: str  # 'high', 'medium', 'low'
    reason: str
    
    @field_validator('severity')
    @classmethod
    def validate_severity(cls, v: str) -> str:
        """Validate severity is one of allowed values."""
        allowed = {'high', 'medium', 'low'}
        if v not in allowed:
            raise ValueError(f"Severity must be one of {allowed}, got: {v}")
        return v

class AnomalyDetectionEngine:
    """
    Detects anomalies in pipeline execution based on historical metrics.
    """
    
    def __init__(self, spark_session=None):
        self.config = get_config()
        self.spark = spark_session if spark_session is not None else _get_or_create_spark()
             
    def _fetch_history(self, job_id: str, step_id: str, limit: int = 30) -> List[Dict]:
        """Fetch historical metrics for the given step."""
        table = self.config.metrics_table
        try:
            df = self.spark.table(table)
            # Filter by job and step
            rows = (df.filter((df.job_or_pipeline_id == job_id) & (df.step_id == step_id))
                      .orderBy("timestamp", ascending=False)
                      .limit(limit)
                      .collect())
            return [row.asDict() for row in rows]
        except Exception as e:
            logger.warning(f"Could not fetch history for {step_id}: {e}")
            return []

    def detect_anomalies(self, context: ExecutionContext) -> List[Anomaly]:
        """
        Main entry point: Check for anomalies in the given context (step).
        """
        job_id = context.job_id
        step_id = context.step_id
        
        # 1. Get History (including latest run)
        history = self._fetch_history(job_id, step_id)
        if not history:
            logger.info(f"No history found for {step_id}. Skipping detection.")
            return []
            
        # Separate latest run (target) from baseline
        latest = history[0]
        baseline = history[1:]
        
        # If run_id doesn't match, maybe collectors haven't run yet?
        if latest.get("run_id") != context.run_id:
            logger.warning(f"Latest metric run_id ({latest.get('run_id')}) does not match current run ({context.run_id}). Metric collection might be lagging.")
            # We proceed assuming 'latest' is the one we want to check, strictly speaking we should filter by run_id
            # but if it's missing, we can't do much.
        
        anomalies = []
        
        # 2. Check for Data Drop (Drop Rate)
        drop_anomaly = self._check_drop_rate(latest, baseline)
        if drop_anomaly:
            anomalies.append(drop_anomaly)
            
        # 3. Check for Data Rejection (Rows Rejected - e.g. DLT or Constraint)
        reject_anomaly = self._check_rejections(latest)
        if reject_anomaly:
            anomalies.append(reject_anomaly)
            
        return anomalies

    def _calculate_drop_rate(self, row: Dict) -> float:
        """Calculate drop rate safely."""
        input_rows = row.get("rows_in", 0)
        output_rows = row.get("rows_out", 0)
        operation = row.get("operation_type", "UNKNOWN")
        
        # For DELETE operations, output_rows might be 'rows deleted'.
        # This reverses the logic. If we deleted 100% of rows, that's not a 'data loss' in the pipeline sense,
        # but functionality it might be catastrophic if unintentional.
        # For now, let's treat drop rate as (In - Out) / In for standard Insert/Merge.
        
        if input_rows <= 0:
            return 0.0
            
        return max(0, (input_rows - output_rows)) / input_rows

    def _check_drop_rate(self, latest: Dict, baseline: List[Dict]) -> Optional[Anomaly]:
        """Check for statistical deviation in drop rate."""
        current_rate = self._calculate_drop_rate(latest)
        
        # If baseline empty, just check absolute threshold
        if not baseline:
            if current_rate > 0.5: # 50% drop on first run is suspicious
                return Anomaly(
                    run_id=latest["run_id"],
                    step_id=latest["step_id"],
                    metric_name="drop_rate",
                    current_value=current_rate,
                    historical_avg=0.0,
                    deviation_z_score=0.0,
                    severity="medium",
                    reason=f"High initial drop rate: {current_rate:.1%}"
                )
            return None

        # Stats
        hist_rates = [self._calculate_drop_rate(r) for r in baseline]
        avg = statistics.mean(hist_rates)
        stdev = statistics.stdev(hist_rates) if len(hist_rates) > 1 else 0.01
        
        # Check for insufficient variation
        if stdev < 0.001:
            logger.info(f"Insufficient variation in historical data for step (stdev={stdev:.4f})")
            return None
        
        z_score = (current_rate - avg) / stdev
        
        # Logic: Flag if drop rate is significantly higher than normal
        z_threshold = self.config.anomaly_z_score_threshold
        drop_threshold = self.config.anomaly_drop_rate_threshold
        
        if z_score > z_threshold and current_rate > (avg + drop_threshold):
             return Anomaly(
                    run_id=latest["run_id"],
                    step_id=latest["step_id"],
                    metric_name="drop_rate",
                    current_value=current_rate,
                    historical_avg=avg,
                    deviation_z_score=z_score,
                    severity="high",
                    reason=f"Drop rate {current_rate:.1%} is anomalous (Avg: {avg:.1%}, Z-Score: {z_score:.1f})"
                )
        
        return None

    def _check_rejections(self, latest: Dict) -> Optional[Anomaly]:
        """Check for high number of rejected rows (DLT expectations)."""
        rejected = latest.get("rows_rejected", 0)
        rows_in = latest.get("rows_in", 0)
        
        if rejected > 0:
            rate = rejected / rows_in if rows_in > 0 else 1.0
            rejection_threshold = self.config.anomaly_rejection_rate_threshold
            if rate > rejection_threshold:
                 return Anomaly(
                    run_id=latest["run_id"],
                    step_id=latest["step_id"],
                    metric_name="rows_rejected",
                    current_value=float(rejected),
                    historical_avg=0.0,
                    deviation_z_score=0.0,
                    severity="high",
                    reason=f"High data quality rejection: {rejected} rows ({rate:.1%})"
                )
        return None
