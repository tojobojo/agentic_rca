"""
Main Entrypoint for the Agentic RCA System.
Orchestrates the RCA workflow using the new Databricks-Native Architecture.

Workflow:
1. Observability (Optional): Collect latest metrics if needed.
2. Context: Build execution context (Code + Lineage + Semantics) for each step.
3. Detection: Check for anomalies in metrics history.
4. Investigation: AI Agent diagnosis for flagged steps.
5. Reporting: Generate Markdown report.
"""
import argparse
import os
import sys
import logging
from datetime import datetime
from typing import List, Tuple

# Ensure package imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import get_config
from observability_collector import ObservabilityCollector
from execution_context import ExecutionContextBuilder, ExecutionContext
from anomaly_engine import AnomalyDetectionEngine, Anomaly
from rca_agent import RCAAgent

logger = logging.getLogger(__name__)


import json

def run_rca_orchestrator(
    job_id: int,
    run_id: int,
    collect_metrics: bool = False,
    output_path: str = "rca_report.md",
    manifest_path: str = None
) -> str:
    """
    Execute the RCA Orchestration flow.
    """
    logger.info("=" * 60)
    logger.info("       AGENTIC RCA ORCHESTRATOR")
    logger.info("=" * 60)
    logger.info(f"Job ID: {job_id}")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Collect Metrics: {collect_metrics}")
    logger.info(f"Manifest: {manifest_path}")
    
    # Validate configuration
    config = get_config()
    errors = config.validate()
    if errors:
        raise ValueError(f"Configuration errors: {', '.join(errors)}")
    
    # Load manifest if provided
    manifest_data = None
    if manifest_path and os.path.exists(manifest_path):
        try:
            with open(manifest_path, 'r') as f:
                manifest_data = json.load(f)
            logger.info(f"Loaded manifest with {len(manifest_data)} entries.")
        except Exception as e:
            logger.warning(f"Failed to load manifest: {e}")

    # 1. Observability Collection (Phase 1)
    if collect_metrics:
        logger.info("\n[Phase 1] OBSERVABILITY COLLECTION")
        collector = ObservabilityCollector()
        collector.collect_job_metrics(run_id)
        # We don't save return value, as it writes to Delta table which Engine reads
    
    # 2. Initialization
    ctx_builder = ExecutionContextBuilder()
    engine = AnomalyDetectionEngine()
    agent = RCAAgent()
    
    # 3. Context & Detection Loop
    logger.info("\n[Phase 2] CONTEXT & DETECTION")
    
    # Get tasks from Databricks API via Discovery or SDK
    # We use discovery agent logic inside builder, but we need the list of tasks to iterate
    # For simplicity, we ask the builder to discover steps first? 
    # Actually, builder takes a step_id. We need to enlist steps first.
    # Let's use the discovery agent inside the builder (accessed via private member or we instantiate one here)
    steps = ctx_builder.discovery.discover(job_id, get_config().gitlab_url)
    task_keys = [s.task_key for s in steps]
    
    if not task_keys:
        # Fallback to fetching run tasks if discovery failed (no git connection)
        try:
            client = ctx_builder.discovery._get_workspace_client()
            run = client.jobs.get_run(run_id)
            task_keys = [t.task_key for t in run.tasks or []]
        except Exception:
            logger.warning("Could not list tasks. Aborting.")
            return "Error: Could not list tasks."

    anomalies_found: List[Tuple[Anomaly, ExecutionContext]] = []
    validated_steps = []

    for task_key in task_keys:
        logger.info(f"Analyzing Step: {task_key}...")
        
        try:
            # Build Context (with manifest)
            context = ctx_builder.build_context(job_id, run_id, task_key, manifest_data=manifest_data)
            validated_steps.append(context)
            
            # Detect Anomalies
            step_anomalies = engine.detect_anomalies(context)
            
            if step_anomalies:
                logger.warning(f"  -> FOUND {len(step_anomalies)} ANOMALIES")
                for a in step_anomalies:
                    anomalies_found.append((a, context))
            else:
                logger.info("  -> Status: OK")
                
        except Exception as e:
            logger.error(f"Error processing step {task_key}: {e}")

    # 4. AI Investigation (Phase 3)
    rca_outputs = []
    if anomalies_found:
        logger.info(f"\n[Phase 3] AI INVESTIGATION ({len(anomalies_found)} items)")
        rca_outputs = agent.analyze_all(anomalies_found)
    else:
        logger.info("\n[Phase 3] No anomalies to investigate.")

    # 5. Report Generation
    report = generate_report(job_id, run_id, validated_steps, anomalies_found, rca_outputs)
    
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        logger.info(f"\nReport saved to: {output_path}")

    return report

def generate_report(job_id, run_id, steps, anomalies, rca_outputs) -> str:
    """Generate Markdown report."""
    report = f"""# RCA Report
**Job ID**: {job_id}
**Run ID**: {run_id}
**Generated**: {datetime.now().isoformat()}

## Executive Summary
- **Steps Analyzed**: {len(steps)}
- **Anomalies Detected**: {len(anomalies)}

---
"""
    if rca_outputs:
        report += "## Root Cause Analysis\n\n"
        for output in rca_outputs:
            report += output + "\n\n---\n\n"
            
    report += "## Execution Log\n\n"
    report += "| Step | Logic | Status |\n|---|---|---|\n"
    
    # Create a set of anomalous step_ids for quick lookup
    bad_steps = {a[1].step_id for a in anomalies}
    
    for ctx in steps:
        status = "🔴 Anomaly" if ctx.step_id in bad_steps else "🟢 OK"
        report += f"| {ctx.step_id} | {ctx.logic_type} | {status} |\n"
        
    return report

def main():
    parser = argparse.ArgumentParser(description="Agentic RCA Orchestrator")
    parser.add_argument("--job-id", type=int, required=True, help="Databricks Job ID")
    parser.add_argument("--run-id", type=int, required=True, help="Databricks Run ID")
    parser.add_argument("--collect", action="store_true", help="Run observability collection first")
    parser.add_argument("--output", default="rca_report.md", help="Output file path")
    parser.add_argument("--manifest", default=None, help="Path to JSON manifest with table mappings")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    run_rca_orchestrator(
        job_id=args.job_id,
        run_id=args.run_id,
        collect_metrics=args.collect,
        output_path=args.output,
        manifest_path=args.manifest
    )

if __name__ == "__main__":
    main()
