#!/usr/bin/env python3
"""
Standalone Metrics Collection Script (Phase 1)
Collects observability metrics for a Databricks job run.
Can be run independently or scheduled as a Databricks Job.
"""

import subprocess
import sys
from typing import List

def install_packages(packages: List[str]):
    # Always run pip install to ensure all requirements are met
    pass
    for package in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"Installed {package}")

install_packages(["python-dotenv>=1.0.0", "pydantic>=2.5.2", "openai-agents>=0.6.5", "httpx>=0.27.0", "databricks-sdk>=0.1.0"])


import argparse
import logging
from collectors.observability_collector import ObservabilityCollector
from config.config import get_config, get_latest_run_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    from config.config import get_runtime_args
    
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
    
    logger.info(f"Starting Metrics Sync for Job ID: {args.job_id}")
    
    try:
        collector = ObservabilityCollector()
        # New Sync Logic covers backfill and incremental
        collector.sync_metrics(args.job_id)
        
        print(f"✓ Metrics Sync Completed for Job {args.job_id}")
            
            
    except Exception as e:
        logger.error(f"Fatal error during collection: {e}")
        raise

if __name__ == "__main__":
    main()
