import ast
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ASTParser(ast.NodeVisitor):
    """
    Layer 2: Deterministic AST Extraction.
    Extracts variable assignments and Spark I/O calls without execution.
    """
    def __init__(self):
        self.assignments: Dict[str, Any] = {} # var_name -> expression_node
        self.io_calls: List[Dict[str, Any]] = [] # list of {type: read/write, target: node}
        self.imports: Dict[str, str] = {} # alias -> full_name

    def parse(self, code: str):
        try:
            tree = ast.parse(code)
            self.visit(tree)
        except SyntaxError as e:
            logger.error(f"AST Parse Error: {e}")

    def visit_Import(self, node):
        for alias in node.names:
            name = alias.name
            asname = alias.asname or name
            self.imports[asname] = name
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        module = node.module or ""
        for alias in node.names:
            name = alias.name
            asname = alias.asname or name
            self.imports[asname] = f"{module}.{name}"
        self.generic_visit(node)

    def visit_Assign(self, node):
        """Track variable assignments."""
        # Handle simple assignments: target = value
        value = node.value
        for target in node.targets:
            if isinstance(target, ast.Name):
                # We store the raw AST node of the value for later 'safe eval'
                self.assignments[target.id] = value
        self.generic_visit(node)

    def visit_Call(self, node):
        """Find Spark I/O calls."""
        # Heuristic for Spark calls
        # We look for methods: table(), load(), save(), saveAsTable(), parquet(), csv()
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            
            # SOURCES
            if method_name in ["table", "load", "parquet", "csv", "json", "text"]:
                # Check if it looks like a read operation
                # Typically: spark.read.table() or spark.table() or spark.read.load()
                # We catch broadly here, Filter/Refine later
                if self._is_spark_read(node.func):
                    self._add_io("READ", node)

            # TARGETS
            elif method_name in ["save", "saveAsTable", "insertInto", "parquet", "csv", "json"]:
                 if self._is_spark_write(node.func):
                     self._add_io("WRITE", node)
                     
        self.generic_visit(node)

    def _add_io(self, operation: str, node: ast.Call):
        """Extract the argument (table/path) from the call."""
        if not node.args:
            return # No argument?
            
        arg = node.args[0] # First arg is usually the path/table
        
        io_record = {
            "operation": operation,
            "arg_node": arg, # The AST node of the argument (Str, Name, JoinedStr, etc.)
            "line": node.lineno
        }
        self.io_calls.append(io_record)

    def _is_spark_read(self, attr_node: ast.Attribute) -> bool:
        """Crude check if the call chain suggests a Spark Read."""
        # traverse up: spark.read.format(...).load(...)
        # We assume if it's one of the keywords, it's relevant. 
        # Resolving 'spark' variable is hard in pure AST without flow analysis, 
        # so we rely on method names + context in the Hybrid phase.
        return True 

    def _is_spark_write(self, attr_node: ast.Attribute) -> bool:
        """Crude check for Spark Write."""
        # createOrReplaceTempView is NOT a persistent write
        if attr_node.attr == "createOrReplaceTempView": return False
        return True
