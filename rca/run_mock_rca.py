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

# Load environment variables (optional in Databricks, but harmless)
load_dotenv()

from main import run_rca_orchestrator
from ai_agents.discovery_agent import DiscoveryAgent, StepInfo
from unittest.mock import patch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_mock_rca():
    print("--> Running RCA Agent with Mocked Discovery...")
    
    JOB_ID = 999999
    RUN_ID = 1001
    MANIFEST_PATH = "sample_manifest.json"
    
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
    
    with patch("ai_agents.discovery_agent.DiscoveryAgent.fetch_job_definition", return_value=mock_job_def):
        with patch("ai_agents.discovery_agent.DiscoveryAgent.fetch_code_from_workspace", return_value=None):
            with patch("databricks.sdk.WorkspaceClient"):
                print(f"--> Invoking RCA Orchestrator for Job {JOB_ID}, Run {RUN_ID}...")
                
                try:
                    report = run_rca_orchestrator(
                        job_id=JOB_ID,
                        run_id=RUN_ID,
                        manifest_path=MANIFEST_PATH,
                        output_path="test_report.md"
                    )
                    
                    print("\n[SUCCESS] RCA Execution Complete!")
                    print(f"Report generated at: test_report.md")
                    
                    # Print snippet of report
                    print("\n--- Report Snippet ---\n")
                    lines = report.split('\n')
                    print("\n".join(lines[:20]))
                    print("...")
                    
                except Exception as e:
                    print(f"[ERROR] RCA Failed: {e}")
                    import traceback
                    traceback.print_exc()

if __name__ == "__main__":
    run_mock_rca()
