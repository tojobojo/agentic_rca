import yaml
import json
import os
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    Layer 1: Config & Parameter Resolution.
    Loads and merges configurations to establish 'Ground Truth' for variables.
    """
    def __init__(self, config_dir: str = "conf"):
        self.config_dir = config_dir
        self.config: Dict[str, Any] = {}
        self.flat_config: Dict[str, Any] = {}

    def load_configs(self, environment: str = None, job_params: Dict[str, Any] = None, config_files: Dict[str, str] = None) -> Dict[str, Any]:
        """
        Loads keys in this order (last wins):
        1. defaults.yaml
        2. {environment}.yaml (defaults to ['prod', 'production'] if None)
        3. job_params (JSON)
        """
        self.config_files = config_files or {}

        # 1. Defaults
        # Check for defaults.yaml in config_files (any path ending with defaults.yaml) or on disk
        defaults_content = self._find_and_load("defaults.yaml")
        if defaults_content:
            self.config = self._merge(self.config, defaults_content)

        # 2. Environment Overlay
        # If no environment provider, default to Production
        envs_to_try = [environment] if environment else ["prod", "production"]
        
        for env in envs_to_try:
             # 1. Try exact file "env.yaml"
             env_file = f"{env}.yaml"
             env_content = self._find_and_load(env_file)
             if env_content:
                 logger.info(f"Loading environment config: {env_file}")
                 self.config = self._merge(self.config, env_content)
                 break
             
             # 2. Try path matching (e.g. conf/prod/config.yaml or conf/production/settings.yml)
             # We look for a file that contains /{env}/ in its path and ends with .yaml/.yml
             env_content = self._find_and_load(env, try_path_match=True)
             if env_content:
                 logger.info(f"Loading environment config (from path): {env}")
                 self.config = self._merge(self.config, env_content)
                 break
        
        # 3. Job Parameters
        if job_params:
            logger.info("Merging job parameters...")
            # Normalize job params (Databricks passes them as strings usually)
            self.config = self._merge(self.config, job_params)

        # Flatten for easier lookup (e.g. "tables.source" -> "db.tbl")
        self.flat_config = self._flatten(self.config)
        return self.config

    def resolve(self, key: str) -> Optional[Any]:
        """Resolve a key from the loaded config."""
        return self.flat_config.get(key)

    def _find_and_load(self, token: str, try_path_match: bool = False) -> Dict[str, Any]:
        """
        Finds a file.
        If try_path_match=False: finds file ending with 'token' (e.g. 'defaults.yaml').
        If try_path_match=True: finds file containing '/token/' in path and ending with .yaml/.yml.
        """
        # 1. Try InMemory
        for path, content in self.config_files.items():
            if try_path_match:
                # Path Match logic: must look like .../prod/... or .../production/...
                # And must be a YAML file
                normalized_path = path.replace("\\", "/")
                if f"/{token}/" in normalized_path and (path.endswith(".yaml") or path.endswith(".yml")):
                     try:
                        return yaml.safe_load(content) or {}
                     except Exception as e:
                        logger.error(f"Failed to parse in-memory YAML {path}: {e}")
                        return {}
            else:
                # Suffix match logic
                if path.endswith(token):
                    try:
                        return yaml.safe_load(content) or {}
                    except Exception as e:
                        logger.error(f"Failed to parse in-memory YAML {path}: {e}")
                        return {}
        
        # 2. Try Disk (Only supported for direct suffix match for now, path match on disk is complex/ambiguous without walk)
        if not try_path_match:
            disk_path = os.path.join(self.config_dir, token)
            if os.path.exists(disk_path):
                 return self._load_from_disk(disk_path)
        
        return {}

    def _load_from_disk(self, path: str) -> Dict[str, Any]:
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Failed to load YAML {path}: {e}")
            return {}

    def _merge(self, base: Dict, overlay: Dict) -> Dict:
        """Recursive merge."""
        for k, v in overlay.items():
            if k in base and isinstance(base[k], dict) and isinstance(v, dict):
                base[k] = self._merge(base[k], v)
            else:
                base[k] = v
        return base

    def _flatten(self, d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flattens nested dict: {'a': {'b': 1}} -> {'a.b': 1}"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
