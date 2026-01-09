"""
Discovery Agent Module.
Responsible for:
1. Fetching Databricks Job definition via API.
2. Cloning GitLab repository.
3. Mapping Databricks task paths to Git files.
"""
import os
import re
import shutil
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pathlib import Path

import git
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task

from config import get_config


@dataclass
class StepInfo:
    """Represents a single step in the pipeline."""
    task_key: str
    notebook_path: str  # Databricks path
    git_file_path: Optional[str] = None  # Resolved local path
    dependencies: List[str] = field(default_factory=list)
    code_content: Optional[str] = None


class DiscoveryAgent:
    """
    The Cartographer: Discovers pipeline structure and maps code.
    """
    
    def __init__(self):
        self.config = get_config()
        self.workspace_client: Optional[WorkspaceClient] = None
        self.repo_path: Optional[Path] = None
    
    def _get_workspace_client(self) -> WorkspaceClient:
        """Initialize Databricks Workspace Client."""
        if self.workspace_client is None:
            self.workspace_client = WorkspaceClient(
                host=self.config.databricks_host,
                token=self.config.databricks_token
            )
        return self.workspace_client
    
    def fetch_job_definition(self, job_id: int) -> Dict:
        """
        Fetch the Job definition from Databricks API.
        Returns the full job JSON including all tasks.
        """
        client = self._get_workspace_client()
        job = client.jobs.get(job_id)
        return job.as_dict()
    
    def extract_steps_from_job(self, job_json: Dict) -> List[StepInfo]:
        """
        Parse the Job JSON and extract a list of StepInfo objects.
        Handles notebook_task, spark_python_task, etc.
        """
        steps = []
        settings = job_json.get("settings", {})
        tasks = settings.get("tasks", [])
        
        for task in tasks:
            task_key = task.get("task_key", "unknown")
            notebook_path = None
            dependencies = []
            
            # Extract notebook path
            if "notebook_task" in task:
                notebook_path = task["notebook_task"].get("notebook_path", "")
            elif "spark_python_task" in task:
                notebook_path = task["spark_python_task"].get("python_file", "")
            elif "sql_task" in task:
                # SQL tasks might reference a file or query
                sql_task = task["sql_task"]
                if "file" in sql_task:
                    notebook_path = sql_task["file"].get("path", "")
            
            # Extract dependencies
            if "depends_on" in task:
                dependencies = [dep.get("task_key") for dep in task["depends_on"]]
            
            if notebook_path:
                steps.append(StepInfo(
                    task_key=task_key,
                    notebook_path=notebook_path,
                    dependencies=dependencies
                ))
        
        return steps
    
    def clone_gitlab_repo(self, gitlab_url: str, branch: str = "main") -> Path:
        """
        Clone the GitLab repository to a temporary directory.
        Returns the path to the cloned repo.
        """
        # Clean up existing clone if present
        repo_dir = Path(self.config.temp_dir) / "repo"
        if repo_dir.exists():
            shutil.rmtree(repo_dir)
        
        repo_dir.mkdir(parents=True, exist_ok=True)
        
        # Add token to URL for authentication
        # Format: https://oauth2:TOKEN@gitlab.com/user/repo.git
        auth_url = gitlab_url
        if self.config.gitlab_token:
            if "https://" in gitlab_url:
                auth_url = gitlab_url.replace(
                    "https://", 
                    f"https://oauth2:{self.config.gitlab_token}@"
                )
        
        # Clone
        git.Repo.clone_from(auth_url, repo_dir, branch=branch, depth=1)
        self.repo_path = repo_dir
        return repo_dir
    
    def map_databricks_path_to_git(self, databricks_path: str) -> Optional[str]:
        """
        Intelligently map a Databricks workspace path to a file in the cloned Git repo.
        
        Example mappings:
        - /Workspace/Repos/user/project/src/etl/step1 -> src/etl/step1.py
        - /Repos/user/project/notebooks/clean -> notebooks/clean.py
        """
        if self.repo_path is None:
            raise ValueError("Repository not cloned. Call clone_gitlab_repo first.")
        
        # Normalize the Databricks path
        # Remove common prefixes
        clean_path = databricks_path
        prefixes_to_remove = [
            r"/Workspace/Repos/[^/]+/[^/]+/",  # /Workspace/Repos/user/project/
            r"/Repos/[^/]+/[^/]+/",             # /Repos/user/project/
            r"/Workspace/",                      # /Workspace/
        ]
        
        for prefix in prefixes_to_remove:
            clean_path = re.sub(prefix, "", clean_path)
        
        # Search for matching files
        possible_extensions = [".py", ".sql", "", ".scala"]
        
        for ext in possible_extensions:
            candidate = self.repo_path / f"{clean_path}{ext}"
            if candidate.exists():
                return str(candidate)
        
        # Fuzzy search: find files with similar names
        target_name = Path(clean_path).name
        for file_path in self.repo_path.rglob("*"):
            if file_path.is_file() and target_name in file_path.stem:
                return str(file_path)
        
        return None
    
    def resolve_steps(self, steps: List[StepInfo]) -> List[StepInfo]:
        """
        Resolve all steps by mapping Databricks paths to Git files
        and loading the code content.
        """
        for step in steps:
            git_path = self.map_databricks_path_to_git(step.notebook_path)
            if git_path:
                step.git_file_path = git_path
                with open(git_path, 'r', encoding='utf-8') as f:
                    step.code_content = f.read()
            else:
                print(f"Warning: Could not resolve Git path for {step.notebook_path}")
        
        return steps
    
    def discover(self, job_id: int, gitlab_url: str, branch: str = "main") -> List[StepInfo]:
        """
        Main entry point: Discover the full pipeline structure.
        
        Args:
            job_id: Databricks Job ID
            gitlab_url: GitLab repository URL
            branch: Git branch to clone
            
        Returns:
            List of StepInfo with resolved code content
        """
        print(f"[Discovery] Fetching Job {job_id}...")
        job_json = self.fetch_job_definition(job_id)
        
        print(f"[Discovery] Extracting steps...")
        steps = self.extract_steps_from_job(job_json)
        print(f"[Discovery] Found {len(steps)} steps.")
        
        print(f"[Discovery] Cloning GitLab repo...")
        self.clone_gitlab_repo(gitlab_url, branch)
        
        print(f"[Discovery] Mapping paths to Git files...")
        resolved_steps = self.resolve_steps(steps)
        
        resolved_count = sum(1 for s in resolved_steps if s.git_file_path)
        print(f"[Discovery] Resolved {resolved_count}/{len(steps)} steps to Git files.")
        
        return resolved_steps
