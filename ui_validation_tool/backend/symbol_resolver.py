import ast
from typing import Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class SymbolResolver:
    """
    Layer 3: Hybrid Resolution.
    Safely evaluates AST nodes using a provided configuration context.
    Does NOT use eval(). Manually traverses AST for strings/f-strings/lookups.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config # The "Ground Truth" context (flattened or nested)

    def resolve(self, node: ast.AST, local_vars: Dict[str, Any] = None) -> Tuple[Optional[str], str]:
        """
        Resolves an AST node to a string value.
        Returns: (value, confidence)
        Confidence: 'HIGH', 'MEDIUM', 'LOW'
        """
        if local_vars is None: local_vars = {}

        # 1. Literals (Strings)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                return node.value, 'HIGH'
            return str(node.value), 'HIGH'

        # 2. Variables (Name)
        elif isinstance(node, ast.Name):
            var_name = node.id
            # Check locals first (e.g. assignments tracked in AST)
            if var_name in local_vars:
                 # If local var is an AST node, recurse
                 if isinstance(local_vars[var_name], ast.AST):
                      return self.resolve(local_vars[var_name], local_vars)
                 return str(local_vars[var_name]), 'MEDIUM' # It was a resolved value
            
            # Check config (Ground Truth)
            if var_name in self.config:
                return str(self.config[var_name]), 'HIGH'
                
            return None, 'LOW'

        # 3. f-strings (JoinedStr)
        elif isinstance(node, ast.JoinedStr):
            full_str = ""
            min_confidence = 'HIGH'
            
            for part in node.values:
                val, conf = self.resolve(part, local_vars)
                if val is None: 
                    return None, 'LOW'
                full_str += val
                if conf == 'LOW': min_confidence = 'LOW'
                elif conf == 'MEDIUM' and min_confidence == 'HIGH': min_confidence = 'MEDIUM'
                
            return full_str, min_confidence

        # 4. Formatted values inside f-strings ({x})
        elif isinstance(node, ast.FormattedValue):
            return self.resolve(node.value, local_vars)

        # 5. Dict/Attribute Lookups (cfg['env'] or cfg.env)
        elif isinstance(node, ast.Subscript):
            # value[slice] -> cfg['env']
            target, t_conf = self.resolve(node.value, local_vars)
            # This is hard because resolve returns a string, but here we need the Object to look up into.
            # Simplified approach: Look up the Full Path in Flat Config
            
            # Reconstruct the source code of the lookup to see if it matches a config key
            # e.g. "config['tables']['source']"
            try:
                # We can try to reconstruct the key path
                # This is a heuristic.
                key_path = self._reconstruct_attribute_path(node)
                if key_path:
                     # Check flat config for this exact key
                     # We support dot notation for config keys even if code uses dicts
                     flat_key = key_path.replace("['", ".").replace("']", "").replace('["', '.').replace('"]', "")
                     if flat_key in self.config:
                         return str(self.config[flat_key]), 'HIGH'
            except:
                pass
                
            return None, 'LOW'

        elif isinstance(node, ast.Attribute):
            # cfg.env
            try:
                key_path = self._reconstruct_attribute_path(node)
                if key_path and key_path in self.config:
                     return str(self.config[key_path]), 'HIGH'
            except:
                pass
            return None, 'LOW'

        return None, 'LOW'

    def _reconstruct_attribute_path(self, node: ast.AST) -> Optional[str]:
        """Recursively builds 'cfg.env' or 'cfg['env']' string from AST."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            parent = self._reconstruct_attribute_path(node.value)
            if parent: return f"{parent}.{node.attr}"
        elif isinstance(node, ast.Subscript):
             parent = self._reconstruct_attribute_path(node.value)
             if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                 if parent: return f"{parent}.{node.slice.value}" # Flattened: cfg.env
        return None
