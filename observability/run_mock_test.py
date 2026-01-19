import logging
from unittest.mock import MagicMock
from datetime import datetime
from collectors.observability_collector import ObservabilityCollector, MetricRecord

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_mock_test():
    print("🚀 Running Observability Collector with Mocked Databricks API...")

    # 1. Initialize Collector
    # We rely on the local Spark session (via Databricks Connect or local-mode if available)
    # Ensure setup_test_data.py has been run first!
    try:
        collector = ObservabilityCollector()
    except Exception as e:
        print(f"Failed to init collector: {e}")
        return

    # 2. Mock the Databricks Client
    # We need to mock: collector.client.jobs.list_runs(job_id=..., ...)
    # The collector iterates over runs, so we return a list containing our mock run.
    
    mock_run = MagicMock()
    mock_run.run_id = 1001
    mock_run.job_id = 999999
    mock_run.start_time = 1700000000000 # Fake Timestamp
    
    # Define tasks for this run (Must match keys in your Manifest!)
    # In setup_test_data.py, we created "merchant_retention"
    mock_task = MagicMock()
    mock_task.task_key = "merchant_retention"
    
    # State mocking (SUCCESS result)
    state = MagicMock()
    state.result_state.value = "SUCCESS"
    mock_task.state = state
    
    # Other metadata
    mock_task.execution_duration = 5000 # 5 seconds
    mock_task.attempt_number = 1
    
    # Attach tasks to run
    mock_run.tasks = [mock_task]

    # Apply the mock to the collector's client
    # The collector calls: client.jobs.list_runs(job_id=..., limit=..., expand_tasks=...)
    collector.client.jobs.list_runs = MagicMock(return_value=[mock_run])
    
    # Also mock _get_last_collected_run_id to return None (force backfill/process)
    # or we can rely on it returning None if the table is empty.
    # Let's mock it to be safe so we always process run 1001.
    collector._get_last_collected_run_id = MagicMock(return_value=None)

    # 3. Run Sync
    # This will:
    #   a. Call list_runs (Mocked -> Returns run 1001)
    #   b. Fetch Manifest (Real -> Reads from 'dev_dcs_catalog.dev_peergroup_benchmark.rca_manifest_log')
    #   c. Process 'test_step_1' -> Reads 'dev_dcs_catalog.dev_peergroup_benchmark.rca_merchant_source' ...
    #   d. Save Metrics -> Writes to 'dev_dcs_catalog.dev_peergroup_benchmark.rca_metrics_history'
    
    print("--> Triggering sync_metrics(999999)...")
    try:
        collector.sync_metrics(job_id=999999)
        print("\n✅ Test Complete!")
        print("Check table 'dev_dcs_catalog.dev_peergroup_benchmark.rca_metrics_history' to see the collected metrics.")
        
    except Exception as e:
        print(f"\n❌ Test Failed: {e}")

if __name__ == "__main__":
    run_mock_test()
