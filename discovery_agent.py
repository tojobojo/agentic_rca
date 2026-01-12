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
import subprocess
import stat
import tempfile
import pathlib
import sys
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pathlib import Path

import git
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task
from pydantic import BaseModel, Field

from config import get_config
import logging
import urllib.parse
import hashlib

logger = logging.getLogger(__name__)


class StepInfo(BaseModel):
    """Represents a single step in the pipeline."""
    task_key: str
    notebook_path: str  # Databricks path
    git_file_path: Optional[str] = None  # Resolved local path
    dependencies: List[str] = Field(default_factory=list)
    code_content: Optional[str] = None


class DiscoveryAgent:
    """
    The Cartographer: Discovers pipeline structure and maps code.
    """
    
    def __init__(self):
        self.config = get_config()
        self.workspace_client: Optional[WorkspaceClient] = None
        self.repo_path: Optional[Path] = None
    
    def _validate_gitlab_url(self, url: str) -> bool:
        """Validate GitLab URL format for security."""
        try:
            parsed = urllib.parse.urlparse(url)
            # Only allow https/http schemes
            if parsed.scheme not in ['https', 'http']:
                logger.error(f"Invalid URL scheme: {parsed.scheme}. Only https/http allowed.")
                return False
            # Must have a network location
            if not parsed.netloc:
                logger.error("Invalid URL: missing hostname")
                return False
            return True
        except Exception as e:
            logger.error(f"URL validation failed: {e}")
            return False
    
    def _get_repo_cache_dir(self, gitlab_url: str) -> Path:
        """Get cache directory for a specific repository."""
        # Use hash of URL to create unique cache directory
        url_hash = hashlib.md5(gitlab_url.encode()).hexdigest()
        return Path(self.config.temp_dir) / url_hash
    
    def _is_repo_cached(self, cache_dir: Path) -> bool:
        """Check if repository is already cached."""
        return cache_dir.exists() and (cache_dir / ".git").exists()
    
    def _update_cached_repo(self, cache_dir: Path, branch: str) -> bool:
        """Update an existing cached repository."""
        try:
            repo = git.Repo(cache_dir)
            repo.remotes.origin.fetch()
            repo.git.checkout(branch)
            repo.remotes.origin.pull()
            logger.info(f"Updated cached repo at {cache_dir}")
            return True
        except Exception as e:
            logger.warning(f"Cache update failed, will re-clone: {e}")
            return False
    
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
        Uses caching to avoid re-cloning on subsequent runs.
        Returns the path to the cloned repo.
        """
        # Validate URL for security
        if not self._validate_gitlab_url(gitlab_url):
            raise ValueError(f"Invalid or insecure GitLab URL: {gitlab_url}")
        
        # Check cache first
        repo_dir = self._get_repo_cache_dir(gitlab_url)
        
        if self._is_repo_cached(repo_dir):
            logger.info(f"Found cached repository at {repo_dir}")
            if self._update_cached_repo(repo_dir, branch):
                self.repo_path = repo_dir
                return repo_dir
            # If update failed, clean up and re-clone
            shutil.rmtree(repo_dir, ignore_errors=True)
        
        # Clean up existing clone if present
        if repo_dir.exists():
            shutil.rmtree(repo_dir)
        
        repo_dir.mkdir(parents=True, exist_ok=True)
        
        # Two-step clone strategy:
        # 1) Try GitPython clone (may work with auth URL)
        # 2) Fallback to subprocess `git clone` using a temporary GIT_ASKPASS script

        auth_url = gitlab_url
        if self.config.gitlab_token and gitlab_url.startswith("https://"):
            # Create auth URL for GitPython attempt (avoid logging this value)
            auth_url = gitlab_url.replace("https://", f"https://oauth2:{self.config.gitlab_token}@")

        # Attempt 1: GitPython clone using auth URL
        try:
            git.Repo.clone_from(auth_url, repo_dir, branch=branch, depth=1)
            self.repo_path = repo_dir
            return repo_dir
        except Exception:
            logger.info("GitPython clone failed; attempting subprocess fallback")

        # Attempt 2: subprocess `git clone` with GIT_ASKPASS script to supply token
        askpass_script = None
        try:
            if self.config.gitlab_token:
                askpass_fd, askpass_path = tempfile.mkstemp(prefix="git_askpass_", suffix=".sh")
                askpass_script = Path(askpass_path)
                with os.fdopen(askpass_fd, "w") as fh:
                    # Script prints the token to stdout for git to use
                    fh.write("#!/bin/sh\n")
                    fh.write(f"echo '{self.config.gitlab_token}'\n")
                # Make executable
                os.chmod(askpass_path, os.stat(askpass_path).st_mode | stat.S_IEXEC)

                env = os.environ.copy()
                env["GIT_ASKPASS"] = askpass_path
                env["GIT_TERMINAL_PROMPT"] = "0"

                cmd = [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "--branch",
                    branch,
                    gitlab_url,
                    str(repo_dir)
                ]

                subprocess.run(cmd, check=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            else:
                # No token present; try normal subprocess clone
                cmd = ["git", "clone", "--depth", "1", "--branch", branch, gitlab_url, str(repo_dir)]
                subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            self.repo_path = repo_dir
            return repo_dir
        except subprocess.CalledProcessError as e:
            logger.error("Subprocess git clone failed: %s", e)
            if repo_dir.exists():
                shutil.rmtree(repo_dir, ignore_errors=True)
            raise RuntimeError(f"Failed to clone {gitlab_url}: {e}")
        finally:
            # Cleanup askpass script if created
            try:
                if askpass_script and askpass_script.exists():
                    askpass_script.unlink()
            except Exception:
                pass
    
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
        max_file_size = self.config.max_file_size_mb * 1024 * 1024  # Convert to bytes
        
        for step in steps:
            git_path = self.map_databricks_path_to_git(step.notebook_path)
            if git_path:
                step.git_file_path = git_path
                try:
                    file_size = os.path.getsize(git_path)
                    if file_size > max_file_size:
                        logger.warning(
                            f"File {git_path} ({file_size / 1024 / 1024:.2f}MB) exceeds "
                            f"size limit ({self.config.max_file_size_mb}MB), truncating"
                        )
                        with open(git_path, 'r', encoding='utf-8') as f:
                            step.code_content = f.read(max_file_size)
                    else:
                        with open(git_path, 'r', encoding='utf-8') as f:
                            step.code_content = f.read()
                except Exception as e:
                    logger.warning("Could not read file %s: %s", git_path, e)
            else:
                logger.warning("Could not resolve Git path for %s", step.notebook_path)
        
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
        logger.info("[Discovery] Fetching Job %s...", job_id)
        job_json = self.fetch_job_definition(job_id)

        logger.info("[Discovery] Extracting steps...")
        steps = self.extract_steps_from_job(job_json)
        logger.info("[Discovery] Found %d steps.", len(steps))

        logger.info("[Discovery] Cloning GitLab repo...")
        self.clone_gitlab_repo(gitlab_url, branch)

        logger.info("[Discovery] Mapping paths to Git files...")
        resolved_steps = self.resolve_steps(steps)

        resolved_count = sum(1 for s in resolved_steps if s.git_file_path)
        logger.info("[Discovery] Resolved %d/%d steps to Git files.", resolved_count, len(steps))
        
        return resolved_steps
