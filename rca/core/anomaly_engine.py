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

from config.config import get_config, _get_or_create_spark
from core.execution_context import ExecutionContext

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
            # Note: We query by task_key (step_id) if provided, else just job_id
            query = df.filter(df.job_id == job_id)
            if step_id:
                query = query.filter(df.task_key == step_id)
            
            rows = (query.orderBy("timestamp", ascending=False)
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
            
        # 2. Consolidate Metrics (Adapter for New Schema)
        # Group raw rows by run_id to form "Step Executions"
        consolidated_history = self._consolidate_run_metrics(history)
        if not consolidated_history:
             return []

        latest = consolidated_history[0]
        baseline = consolidated_history[1:]
        
        # Populate context with rich metrics for RCA Agent
        context.metrics_snapshot = latest

        # If run_id doesn't match, maybe collectors haven't run yet?
        if latest.get("run_id") != context.run_id:
            logger.warning(f"Latest history run_id {latest.get('run_id')} does not match context run_id {context.run_id}. This might indicate a delay in metric collection or an issue.")
            # Decide if we should proceed or return early. For now, proceed but warn.

        anomalies = [] # Initialize anomalies list
        
        # 3. Check for Data Quality Violations (Critical)
        dq_anomaly = self._check_dq_failures(latest)
        if dq_anomaly:
            anomalies.append(dq_anomaly)

        # 4. Check for Data Drop (Drop Rate)
        drop_anomaly = self._check_drop_rate(latest, baseline)
        if drop_anomaly:
            anomalies.append(drop_anomaly)
            
        # 4. Check for Null Spikes
        null_anomalies = self._check_null_spikes(latest, baseline)
        if null_anomalies:
            anomalies.extend(null_anomalies)

        # 5. Check for Duration Spikes
        dur_anomaly = self._check_duration_spike(latest, baseline)
        if dur_anomaly:
            anomalies.append(dur_anomaly)
            
        # 6. Check for Volume Anomaly
        vol_anomaly = self._check_volume_anomaly(latest, baseline)
        if vol_anomaly:
            anomalies.append(vol_anomaly)
            
        # 7. Check for Freshness/SLA Anomaly
        fresh_anomaly = self._check_freshness_anomaly(latest, baseline)
        if fresh_anomaly:
            anomalies.append(fresh_anomaly)
            
        # 8. Check for Schema Drift
        schema_anomaly = self._check_schema_drift(latest, baseline)
        if schema_anomaly:
            anomalies.append(schema_anomaly)
            
        return anomalies

    def _check_dq_failures(self, latest: Dict) -> Optional[Anomaly]:
        """Check for explicit Data Quality rule failures."""
        failures = latest.get("dq_failures", [])
        if not failures:
            return None
            
        return Anomaly(
            run_id=latest["run_id"],
            step_id=latest["step_id"],
            metric_name="dq_rule_failure",
            current_value=float(len(failures)),
            historical_avg=0.0,
            deviation_z_score=0.0,
            severity="high",
            reason=f"Data Quality Rules Failed ({len(failures)}): {'; '.join(failures[:3])}..."
        )

    def _consolidate_run_metrics(self, history: List[Dict]) -> List[Dict]:
        """
        Convert raw rows (Source, Target, Attempts) into logical Step Executions.
        Returns list of dicts with:
        {
          "run_id": ...,
          "step_id": ...,
          "rows_in": Sum(SOURCE rows),
          "rows_out": Sum(TARGET rows),
          "duration_ms": Sum(duration_ms) or Max(end-start),
          "null_details": {col_name: {source_nulls: X, target_nulls: Y}},
          "dq_failures": [List of failure strings],
          "raw_metrics": [original_rows] 
        }
        """
        grouped = {}
        for row in history:
            rid = row.get("run_id")
            if rid not in grouped:
                grouped[rid] = {
                    "run_id": rid,
                    "step_id": row.get("task_key"),
                    "rows_in": 0,
                    "rows_out": 0,
                    "duration_ms": 0,
                    "metrics_by_type": {"SOURCE": [], "TARGET": []},
                    "dq_failures": [],
                    "timestamp": row.get("timestamp"),
                    "columns": row.get("columns", [])
                }
            
            m_type = row.get("metric_type", "TARGET") # Default to Target if missing (old logs)
            grouped[rid]["metrics_by_type"][m_type].append(row)
            
            # Aggregate DQ Failures
            dq_res = row.get("dq_validation_results", {})
            if dq_res:
                for rule, res in dq_res.items():
                    if "FAIL" in str(res): # Simple string check
                        grouped[rid]["dq_failures"].append(f"{row.get('target_table')}: {rule} -> {res}")
            
            # Aggregate Duration (Max of tasks or Sum?)
            # If attempts are sequential, Sum. If parallel (unlikely for 1 task), Max.
            # We treat attempts as sequential usually.
            grouped[rid]["duration_ms"] = max(grouped[rid]["duration_ms"], row.get("duration_ms", 0))

        results = []
        for rid, data in grouped.items():
            # Sum Rows
            # If multiple sources, we sum them as "Total Input"
            # If multiple targets, we sum them as "Total Output"
            # Note: Deduplication might be needed if cached?
            # Our collector sends specific row for task X target Y. So sum is correct.
            
            for s in data["metrics_by_type"].get("SOURCE", []):
                data["rows_in"] += s.get("rows_total", 0)
            
            for t in data["metrics_by_type"].get("TARGET", []):
                data["rows_out"] += t.get("rows_total", 0)
                
            # If no sources (e.g. Ingestion), rows_in might be 0.
            # In that case, drop rate calculation is tricky.
            # But the collector now collects 'sources' for valid manifests.
            
            results.append(data)
            
        # Sort by timestamp desc
        results.sort(key=lambda x: x["timestamp"], reverse=True)
        return results

    def _check_null_spikes(self, latest: Dict, baseline: List[Dict]) -> List[Anomaly]:
        """Check if any vital column has a spike in Nulls."""
        anomalies = []
        
        # We look at TARGET metrics for quality
        targets = latest["metrics_by_type"].get("TARGET", [])
        if not targets: return []
        
        # Combine null counts from all targets (if multiple outputs)
        current_nulls = {} # col -> count
        total_rows = latest["rows_out"]
        if total_rows == 0: return []

        for t in targets:
            null_map = t.get("rows_null_vital", {}) or {} # Handle None
            for col, count in null_map.items():
                current_nulls[col] = current_nulls.get(col, 0) + count
        
        # Compare to baseline avg %
        for col, count in current_nulls.items():
            curr_pct = count / total_rows
            
            # Calculate hist avg stats
            hist_pcts = []
            for run_data in baseline:
                h_targets = run_data["metrics_by_type"].get("TARGET", [])
                h_nulls = 0
                h_total = run_data["rows_out"]
                if h_total == 0: continue
                
                for ht in h_targets:
                     h_null_map = ht.get("rows_null_vital", {}) or {}
                     h_nulls += h_null_map.get(col, 0)
                
                hist_pcts.append(h_nulls / h_total)
            
            if not hist_pcts: continue
            
            avg = statistics.mean(hist_pcts)
            
            # Threshold: > 10% increase or absolute > 50%?
            # Let's say: if curr > avg + 0.1 (10% absolute jump)
            if curr_pct > (avg + 0.1) and curr_pct > 0.05:
                 anomalies.append(Anomaly(
                    run_id=latest["run_id"],
                    step_id=latest["step_id"],
                    metric_name=f"null_rate_{col}",
                    current_value=curr_pct,
                    historical_avg=avg,
                    deviation_z_score=0.0, # Lazy
                    severity="high",
                    reason=f"Null Rate Spike in '{col}': {curr_pct:.1%} (Avg: {avg:.1%})"
                ))
        return anomalies

    def _check_duration_spike(self, latest: Dict, baseline: List[Dict]) -> Optional[Anomaly]:
        """Check for significant latency increase."""
        curr = latest.get("duration_ms", 0)
        if curr < 10000: return None # Ignore sub-10s noise
        
        hist_vals = [r.get("duration_ms", 0) for r in baseline]
        if not hist_vals: return None
        
        avg = statistics.mean(hist_vals)
        if avg == 0: return None
        
        # If 2x slower
        if curr > (avg * 2):
            return Anomaly(
                run_id=latest["run_id"],
                step_id=latest["step_id"],
                metric_name="duration_ms",
                current_value=curr,
                historical_avg=avg,
                deviation_z_score=0.0,
                severity="medium",
                reason=f"Duration Spike: {curr}ms (Avg: {avg:.0f}ms)"
            )
        return None

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

    def _check_volume_anomaly(self, latest: Dict, baseline: List[Dict]) -> Optional[Anomaly]:
        """Check if rows_in dropped significantly compared to history."""
        curr_in = latest.get("rows_in", 0)
        
        hist_volumes = [r.get("rows_in", 0) for r in baseline]
        if not hist_volumes: return None
        
        avg = statistics.mean(hist_volumes)
        if avg == 0: return None
        
        # Calculate drop pct: (Avg - Curr) / Avg
        # Positive means drop. Negative max be a spike (which we might ignore or flag separately)
        drop_pct = (avg - curr_in) / avg
        
        threshold = self.config.anomaly_volume_drop_threshold
        
        if drop_pct > threshold:
             return Anomaly(
                run_id=latest["run_id"],
                step_id=latest["step_id"],
                metric_name="volume_drop",
                current_value=float(curr_in),
                historical_avg=avg,
                deviation_z_score=0.0, # Could calc Z-score if needed
                severity="high",
                reason=f"Significant Volume Drop: {curr_in} rows (Avg: {avg:.0f}, -{drop_pct:.1%})"
            )
        return None

    def _check_freshness_anomaly(self, latest: Dict, baseline: List[Dict]) -> Optional[Anomaly]:
        """Check if start time is significantly delayed."""
        from datetime import datetime
        
        # Helper to parse timestamp
        def get_hour_of_day(ts_str):
            try:
                # Format: 2026-01-20 00:39:12.123456
                dt = datetime.strptime(str(ts_str).split('.')[0], "%Y-%m-%d %H:%M:%S")
                return dt.hour + (dt.minute / 60.0)
            except:
                return None

        curr_ts = latest.get("timestamp")
        curr_hour = get_hour_of_day(curr_ts)
        if curr_hour is None: return None
        
        hist_hours = []
        for r in baseline:
            h = get_hour_of_day(r.get("timestamp"))
            if h is not None: hist_hours.append(h)
            
        if not hist_hours: return None
        
        avg_hour = statistics.mean(hist_hours)
        
        # Handle day wrap-around edge case simply for now (ignoring it, assuming daily regular runs)
        # If avg is 23 (11pm) and curr is 1 (1am), diff is -22. Should be +2.
        # Simple Logic: If delay > threshold hours
        
        diff = curr_hour - avg_hour
        
        threshold = self.config.anomaly_freshness_delay_hours
        
        if diff > threshold:
             return Anomaly(
                run_id=latest["run_id"],
                step_id=latest["step_id"],
                metric_name="start_delay_hours",
                current_value=curr_hour,
                historical_avg=avg_hour,
                deviation_z_score=0.0,
                severity="medium",
                reason=f"Start Time Delayed by {diff:.1f} hours (Avg Hour: {avg_hour:.1f}, Curr: {curr_hour:.1f})"
            )
        return None

    def _check_schema_drift(self, latest: Dict, baseline: List[Dict]) -> Optional[Anomaly]:
        """Check if columns changed compared to last run."""
        # We only check vs the VERY LAST run (baseline[0]), not average
        if not baseline: return None
        
        last_run = baseline[0]
        
        # Columns might be in 'columns' field (if we updated collector) or we infer.
        # Collector update ensured 'columns' is in MetricRecord.
        
        # We need to extract columns from the TARGET metrics inside the run
        # Implementation Detail: _consolidate_run_metrics puts 'columns' at run level? 
        # No, we added it to run dict from 'row.get("columns")'.
        # Since a run has multiple metric rows (Source/Target), which one?
        # Usually TARGET has the interesting schema.
        
        def get_target_cols(run_data):
            targets = run_data["metrics_by_type"].get("TARGET", [])
            if not targets: return set()
            # If multiple targets, union them? Or just take first? 
            # Let's union all target schemas
            cols = set()
            for t in targets:
                c_list = t.get("columns", [])
                if c_list: cols.update(c_list)
            return cols

        curr_cols = get_target_cols(latest)
        last_cols = get_target_cols(last_run)
        
        if not curr_cols or not last_cols: return None
        
        if curr_cols != last_cols:
            missing = last_cols - curr_cols
            new = curr_cols - last_cols
            
            msg = "Schema Drift Detected."
            if missing: msg += f" Missing: {list(missing)}."
            if new: msg += f" New: {list(new)}."
            
            return Anomaly(
                run_id=latest["run_id"],
                step_id=latest["step_id"],
                metric_name="schema_drift",
                current_value=len(curr_cols),
                historical_avg=len(last_cols),
                deviation_z_score=0.0,
                severity="high",
                reason=msg
            )
        return None
