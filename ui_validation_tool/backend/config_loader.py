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

    def load_configs(self, environment: str = None, job_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Loads keys in this order (last wins):
        1. defaults.yaml (if exists)
        2. {environment}.yaml (if exists)
        3. job_params (JSON)
        """
        # 1. Defaults
        default_path = os.path.join(self.config_dir, "defaults.yaml")
        if os.path.exists(default_path):
            self.config = self._merge(self.config, self._load_yaml(default_path))

        # 2. Environment Overlay
        if environment:
            env_path = os.path.join(self.config_dir, f"{environment}.yaml")
            if os.path.exists(env_path):
                logger.info(f"Loading environment config: {env_path}")
                self.config = self._merge(self.config, self._load_yaml(env_path))
        
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

    def _load_yaml(self, path: str) -> Dict[str, Any]:
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
