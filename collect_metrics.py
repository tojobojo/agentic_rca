#!/usr/bin/env python3
"""
Standalone Metrics Collection Script (Phase 1)
Collects observability metrics for a Databricks job run.
Can be run independently or scheduled as a Databricks Job.
"""

import subprocess
import sys

def install_packages(packages: List[str]):
    try:
        import git
        return
    except ImportError:
        pass
    for package in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"Installed {package}")

install_packages(["gitpython>=3.1.40", "python-dotenv>=1.0.0", "pydantic>=2.5.2", "openai-agents>=0.6.5", "httpx>=0.27.0"])


import argparse
import logging
from observability_collector import ObservabilityCollector
from config import get_config, get_latest_run_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    from config import get_runtime_args
    
    args = get_runtime_args()
    
    if not args.job_id:
        logger.error("Job ID is required! Pass --job-id (CLI) or set 'job_id' widget.")
        return
    
    run_id = args.run_id
    if not run_id:
        logger.info(f"No Run ID provided. Fetching latest run for Job {args.job_id}...")
        try:
            run_id = get_latest_run_id(args.job_id)
            logger.info(f"Resolved Run ID: {run_id}")
        except Exception as e:
            logger.error(f"Failed to find latest run: {e}")
            return
    
    logger.info(f"Starting Metrics Collection for Run ID: {run_id}")
    
    try:
        collector = ObservabilityCollector()
        metrics = collector.collect_job_metrics(run_id, args.job_id)
        
        if metrics:
            print(f"✓ Successfully collected {len(metrics)} metric records")
            print(f"✓ Metrics written to: {collector.config.metrics_table}")
        else:
            print("⚠ No metrics collected (or run failed to fetch tasks)")
            
    except Exception as e:
        logger.error(f"Fatal error during collection: {e}")
        raise

if __name__ == "__main__":
    main()
