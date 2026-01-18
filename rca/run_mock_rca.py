import logging
import sys
import os
import json
from unittest.mock import MagicMock, patch

# Ensure '.' is in path 
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main import run_rca_orchestrator
from ai_agents.discovery_agent import DiscoveryAgent, StepInfo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_mock_rca():
    print("🚀 Running RCA Agent with Mocked Discovery...")
    
    JOB_ID = 999999
    RUN_ID = 1001
    MANIFEST_PATH = "rca/sample_manifest.json" # Use the one we updated
    
    # 1. Mock Discovery Agent methods
    # We want to intercept 'fetch_job_definition' to return a fake job structure
    # matching our test data (Task: "test_step_1").
    
    mock_job_def = {
        "job_id": JOB_ID,
        "settings": {
            "name": "Test Job",
            "tasks": [
                {
                    "task_key": "test_step_1",
                    "notebook_task": {
                        "notebook_path": "/Workspace/Users/test/test_notebook"
                    },
                    "description": "A test step that drops rows."
                }
            ]
        }
    }
    
    # We also need to mock `fetch_code_from_workspace` because we don't have access to the workspace.
    # It should return some dummy code or None (fallback to Manifest).
    
    with patch("ai_agents.discovery_agent.DiscoveryAgent.fetch_job_definition", return_value=mock_job_def) as mock_fetch_job:
        with patch("ai_agents.discovery_agent.DiscoveryAgent.fetch_code_from_workspace", return_value=None) as mock_fetch_code:
             with patch("databricks.sdk.WorkspaceClient") as mock_ws_client: # Mock generic client creation
                
                # We also need to ensure 'get_latest_run_id' isn't called if we pass run_id.
                # run_rca_orchestrator calls it if run_id is None. We are passing 1001.
                
                print(f"--> Invoking RCA Orchestrator for Job {JOB_ID}, Run {RUN_ID}...")
                
                try:
                    report = run_rca_orchestrator(
                        job_id=JOB_ID,
                        run_id=RUN_ID,
                        manifest_path=MANIFEST_PATH,
                        output_path="rca/test_report.md"
                    )
                    
                    print("\n✅ RCA Execution Complete!")
                    print(f"Report generated at: rca/test_report.md")
                    
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
