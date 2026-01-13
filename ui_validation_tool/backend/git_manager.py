import os
import shutil
import git
import logging
from pathlib import Path
import tempfile
import stat
import subprocess
from typing import Optional
from .config import get_config

logger = logging.getLogger(__name__)

class GitManager:
    def __init__(self):
        self.config = get_config()
        self.repo_dir = None

    def clone_repo(self, gitlab_url: str, branch: str = "main") -> str:
        """Clones repo to temp dir and returns path."""
        # Create temp dir
        temp_base = self.config.temp_dir
        os.makedirs(temp_base, exist_ok=True)
        
        repo_name = gitlab_url.split("/")[-1].replace(".git", "")
        self.repo_dir = os.path.join(temp_base, repo_name)
        
        # Cleanup if exists
        if os.path.exists(self.repo_dir):
            shutil.rmtree(self.repo_dir, ignore_errors=True)
            
        os.makedirs(self.repo_dir, exist_ok=True)

        token = self.config.gitlab_token
        auth_url = gitlab_url
        if token and gitlab_url.startswith("https://"):
             auth_url = gitlab_url.replace("https://", f"https://oauth2:{token}@")

        try:
            logger.info(f"Cloning {gitlab_url} to {self.repo_dir}")
            git.Repo.clone_from(auth_url, self.repo_dir, branch=branch, depth=1)
            return self.repo_dir
        except Exception as e:
            # Fallback for subprocess if needed (simplified from original agent)
            logger.error(f"Git clone failed: {e}")
            raise
    
    def get_file_content(self, relative_path: str) -> Optional[str]:
        """Read content of a file in the repo."""
        if not self.repo_dir:
            raise ValueError("Repo not cloned")
            
        # Normalize path (remove /Workspace/Repos/ etc)
        clean_path = relative_path
        prefixes = [r"/Workspace/Repos/[^/]+/[^/]+/", r"/Repos/[^/]+/[^/]+/", r"/Workspace/"]
        import re
        for p in prefixes:
            clean_path = re.sub(p, "", clean_path)
            
        # Try finding file
        search_path = Path(self.repo_dir) / clean_path
        
        # Try exact match first
        if search_path.exists():
            return search_path.read_text(encoding='utf-8', errors='ignore')
            
        # Try extensions
        for ext in [".py", ".sql", ".scala"]:
            p = search_path.with_suffix(ext)
            if p.exists():
                return p.read_text(encoding='utf-8', errors='ignore')
        
        # Fuzzy search
        target_name = Path(clean_path).name
        for f in Path(self.repo_dir).rglob("*"):
             if f.is_file() and target_name in f.name:
                 return f.read_text(encoding='utf-8', errors='ignore')
                 
        return None
