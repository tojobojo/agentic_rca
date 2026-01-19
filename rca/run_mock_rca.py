import logging
import sys
import os
import json
from unittest.mock import patch

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
    base_dir = os.path.dirname(os.path.abspath(__file__))
    MANIFEST_PATH = os.path.join(base_dir, "sample_manifest.json")
    
    # 1. Mock Discovery Agent methods
    # We want to intercept 'fetch_job_definition' to return a fake job structure
    # matching our test data (Task: "test_step_1").
    
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
                        output_path=os.path.join(base_dir, "test_report.md")
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
