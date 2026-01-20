import logging
import sys
import os
import json
import subprocess
from unittest.mock import patch


def install_packages():
    try:
        print("Importing...")
        import agents
        return
    except ImportError:
        print("Import error")
        pass
    packages = ["python-dotenv>=1.0.0", "pydantic>=2.5.2", "openai-agents>=0.6.5", "httpx>=0.27.0", "databricks-sdk>=0.1.0", "litellm>=1.0.0"]
    for package in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"Installed {package}")

install_packages()

from dotenv import load_dotenv

# CRITICAL: Load environment variables BEFORE any imports that depend on config
# This ensures DATABRICKS_HOST, DATABRICKS_TOKEN, etc. are available during module initialization
load_dotenv()

from main import run_rca_orchestrator
from ai_agents.discovery_agent import DiscoveryAgent, StepInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_mock_rca():
    print("--> Running RCA Agent with Mocked Discovery and Spark tools...")
    
    JOB_ID = 999999
    RUN_ID = 1001
    MANIFEST_PATH = "sample_manifest.json"
    
    # Mock Spark tools to prevent agent from hitting max turns
    def mock_get_table_schema(table_name: str):
        return f"Schema for {table_name}: id (bigint), merchant_id (string), transaction_date (date), amount (decimal), status (string)"
    
    def mock_query_spark_sql(query: str):
        return "Query executed successfully. Sample result: 1000 rows affected."
    
    def mock_count_nulls(table_name: str, column_name: str):
        return f"Null count in {table_name}.{column_name}: 42 (0.5% of total rows)"
    
    def mock_delta_history(table_name: str):
        return f"Delta history for {table_name}: Last 3 operations - INSERT (2024-01-20), UPDATE (2024-01-19), DELETE (2024-01-18)"
    
    # Mock Discovery Agent methods
    mock_job_def = {
        "job_id": JOB_ID,
        "settings": {
            "name": "Merchant Retention Pipeline",
            "tasks": [
                {"task_key": "identify_smb", "notebook_task": {"notebook_path": "nb1"}},
                {"task_key": "dynamic_sample_size", "notebook_task": {"notebook_path": "nb2"}, "depends_on": [{"task_key": "identify_smb"}]},
                {"task_key": "proximity_calculation", "notebook_task": {"notebook_path": "nb3"}, "depends_on": [{"task_key": "dynamic_sample_size"}]},
                {"task_key": "ats_calculation", "notebook_task": {"notebook_path": "nb4"}, "depends_on": [{"task_key": "proximity_calculation"}]},
                {"task_key": "merchant_retention", "notebook_task": {"notebook_path": "nb5"}, "depends_on": [{"task_key": "ats_calculation"}]},
                {"task_key": "all_mcc_segmentation", "notebook_task": {"notebook_path": "nb6"}, "depends_on": [{"task_key": "merchant_retention"}]}
            ]
        }
    }
    
    with patch("ai_agents.rca_agent.get_table_schema", side_effect=mock_get_table_schema):
        with patch("ai_agents.rca_agent.query_spark_sql", side_effect=mock_query_spark_sql):
            with patch("ai_agents.rca_agent.count_nulls_in_column", side_effect=mock_count_nulls):
                with patch("ai_agents.rca_agent.get_delta_history", side_effect=mock_delta_history):
                    with patch("ai_agents.discovery_agent.DiscoveryAgent.fetch_job_definition", return_value=mock_job_def):
                        with patch("ai_agents.discovery_agent.DiscoveryAgent.fetch_code_from_workspace", return_value=None):
                            with patch("databricks.sdk.WorkspaceClient"):
             
                    # We also need to ensure 'get_latest_run_id' isn't called if we pass run_id.
                    # run_rca_orchestrator calls it if run_id is None. We are passing 1001.
                    
                    print(f"--> Invoking RCA Orchestrator for Job {JOB_ID}, Run {RUN_ID}...")
                    
                    try:
                        report = run_rca_orchestrator(
                            job_id=JOB_ID,
                            run_id=RUN_ID,
                            manifest_path=MANIFEST_PATH,
                            output_path="test_report.md"
                        )
                        
                        print("\n✅ RCA Execution Complete!")
                        print(f"Report generated at: test_report.md")
                        
                        # Print snippet of report
                        print("\n--- Report Snippet ---\n")
                        lines = report.split('\n')
                        print("\n".join(lines[:20]))
                        print("...")
                        
                    except Exception as e:
                        print(f"❌ RCA Failed: {e}")
                        import traceback
                        traceback.print_exc()

if __name__ == "__main__":
    run_mock_rca()
