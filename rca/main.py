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
import subprocess
from datetime import datetime
from typing import List, Tuple

def install_packages():
    try:
        print("Importing...")
        import agents
        return
    except ImportError:
        print("Import error")
        pass
    
    # Check if packages are installed
    packages = ["python-dotenv>=1.0.0", "pydantic>=2.5.2", "openai-agents>=0.6.5", "httpx>=0.27.0", "databricks-sdk>=0.1.0", "litellm>=1.0.0", "tqdm>=4.66.1"]
        
    # Just force install for now to be safe as per original logic, but suppress output a bit
    for package in packages:
        try:
             subprocess.check_call([sys.executable, "-m", "pip", "install", package, "--quiet", "--disable-pip-version-check"])
             print(f"Installed {package}")
        except Exception as e:
             print(f"Warning: Failed to install {package}: {e}")

install_packages()

from config.config import get_config
from core.execution_context import ExecutionContextBuilder, ExecutionContext
from core.anomaly_engine import AnomalyDetectionEngine, Anomaly
from ai_agents.rca_agent import RCAAgent
from utils.telemetry import PerformanceMetrics, PhaseTimer
import time
import json

logger = logging.getLogger(__name__)

from config.config import get_config

from core.execution_context import ExecutionContextBuilder, ExecutionContext
from core.anomaly_engine import AnomalyDetectionEngine, Anomaly
from ai_agents.rca_agent import RCAAgent
from utils.telemetry import PerformanceMetrics, PhaseTimer
import time

logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not available
    def tqdm(iterable, **kwargs):
        return iterable


import json

def run_rca_orchestrator(
    job_id: int,
    run_id: int = None,
    output_path: str = "rca_report.md",
    manifest_path: str = None
) -> str:
    """
    Execute the RCA Orchestration flow.
    If run_id is not provided, it finds the latest run for the job.
    """
    from config.config import get_latest_run_id
    
    # Auto-resolve run_id if missing
    if not run_id:
        logger.info(f"No Run ID provided. Fetching latest run for Job {job_id}...")
        run_id = get_latest_run_id(job_id)
        logger.info(f"Resolved Run ID: {run_id}")
    # Initialize telemetry
    metrics = PerformanceMetrics()
    workflow_start = time.time()
    
    logger.info("=" * 60)
    logger.info("       AGENTIC RCA ORCHESTRATOR")
    logger.info("=" * 60)
    logger.info(f"Job ID: {job_id}")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Run ID: {run_id}")
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


    
    # 2. Initialization
    ctx_builder = ExecutionContextBuilder()
    engine = AnomalyDetectionEngine()
    agent = RCAAgent()
    
    # 3. Context & Detection Loop
    logger.info("\n[Phase 2] CONTEXT & DETECTION")
    
    # Get tasks from Databricks API via Discovery or SDK
    with PhaseTimer("discovery", metrics):
        # Pass manifest_data for code resolution
        steps = ctx_builder.discovery.discover(job_id, manifest_data=manifest_data)
        task_keys = [s.task_key for s in steps]
    
    if not task_keys:
        # Fallback to fetching run tasks from Databricks API if discovery via Manifest skipped
        try:
            client = ctx_builder.discovery._get_workspace_client()
            run = client.jobs.get_run(run_id)
            task_keys = [t.task_key for t in run.tasks or []]
        except Exception:
            logger.warning("Could not list tasks. Aborting.")
            return "Error: Could not list tasks."

    anomalies_found: List[Tuple[Anomaly, ExecutionContext]] = []
    validated_steps = []
    
    context_start = time.time()
    for task_key in tqdm(task_keys, desc="Analyzing steps", unit="step"):
        logger.info(f"Analyzing Step: {task_key}...")
        
        try:
            # Build Context (with manifest)
            step_start = time.time()
            context = ctx_builder.build_context(job_id, run_id, task_key, manifest_data=manifest_data)
            validated_steps.append(context)
            
            # Detect Anomalies
            detection_start = time.time()
            step_anomalies = engine.detect_anomalies(context)
            metrics.detection_time += (time.time() - detection_start)
            
            if step_anomalies:
                logger.warning(f"  -> FOUND {len(step_anomalies)} ANOMALIES")
                for a in step_anomalies:
                    anomalies_found.append((a, context))
            else:
                logger.info("  -> Status: OK")
                
        except Exception as e:
            logger.error(f"Error processing step {task_key}: {e}")
    
    # Track context building time
    metrics.context_build_time = time.time() - context_start
    metrics.steps_analyzed = len(validated_steps)

    # 4. AI Investigation (Phase 3)
    rca_outputs = []
    if anomalies_found:
        logger.info(f"\n[Phase 3] AI INVESTIGATION ({len(anomalies_found)} items)")
        with PhaseTimer("investigation", metrics):
            rca_outputs = agent.analyze_all(anomalies_found)
        metrics.anomalies_found = len(anomalies_found)
    else:
        logger.info("\n[Phase 3] No anomalies to investigate.")
    
    # Calculate total time
    metrics.total_time = time.time() - workflow_start

    # 5. Report Generation
    report = generate_report(job_id, run_id, validated_steps, anomalies_found, rca_outputs, metrics)
    
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        logger.info(f"\nReport saved to: {output_path}")

    return report

def generate_report(job_id, run_id, steps, anomalies, rca_outputs, metrics: PerformanceMetrics) -> str:
    """Generate Markdown report."""
    report = f"""# RCA Report
**Job ID**: {job_id}
**Run ID**: {run_id}
**Generated**: {datetime.now().isoformat()}

## Executive Summary
- **Steps Analyzed**: {len(steps)}
- **Anomalies Detected**: {len(anomalies)}

{metrics.report()}

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
    from config.config import get_runtime_args
    
    # Use Hybrid Argument Parser (CLI or Widgets)
    args = get_runtime_args()
    
    if not args.job_id:
        logger.error("Job ID is required! Pass --job-id (CLI) or set 'job_id' widget.")
        return
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    try:
        run_rca_orchestrator(
            job_id=args.job_id,
            run_id=args.run_id,
            manifest_path=args.manifest
        )
    except ValueError as e:
        logger.error(f"Execution Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Critical Failure: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
