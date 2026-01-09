"""
Pipeline Parser Module.
Responsible for extracting LOGIC TYPE from code files.

NOTE: Source/Target tables should be provided via:
1. Input Manifest (explicit)
2. Unity Catalog Lineage API (automatic)
This parser does NOT attempt to extract table names from code.
"""
import re
from dataclasses import dataclass, field
from typing import List, Optional

from discovery_agent import StepInfo


@dataclass
class ParsedStep:
    """Enriched step with extracted metadata."""
    task_key: str
    code_content: str
    source_tables: List[str] = field(default_factory=list)  # Provided externally
    target_tables: List[str] = field(default_factory=list)  # Provided externally
    logic_type: str = "unknown"  # filter, join, aggregation, etc.
    logic_summary: str = ""


class PipelineParser:
    """
    Parses code files to extract LOGIC TYPE only.
    Table references are expected to be provided via Input or Lineage API.
    """
    
    # Logic detection patterns
    LOGIC_PATTERNS = {
        "filter": [r'\.filter\(', r'\.where\(', r'\bWHERE\b'],
        "join": [r'\.join\(', r'\bJOIN\b', r'\bLEFT\s+JOIN\b', r'\bINNER\s+JOIN\b'],
        "aggregation": [r'\.groupBy\(', r'\.agg\(', r'\bGROUP\s+BY\b', r'\bSUM\(', r'\bCOUNT\('],
        "distinct": [r'\.distinct\(', r'\.dropDuplicates\(', r'\bDISTINCT\b'],
        "union": [r'\.union\(', r'\bUNION\b'],
    }
    
    def _detect_logic_type(self, code: str) -> str:
        """Detect the primary logic type in the code."""
        detected = []
        for logic_type, patterns in self.LOGIC_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, code, re.IGNORECASE):
                    detected.append(logic_type)
                    break
        
        if not detected:
            return "transform"
        
        # Prioritize: join > aggregation > filter > distinct > union
        priority = ["join", "aggregation", "filter", "distinct", "union"]
        for p in priority:
            if p in detected:
                return p
        
        return detected[0]
    
    def _generate_logic_summary(self, code: str, logic_type: str) -> str:
        """Generate a brief summary of the logic for RCA context."""
        summary_parts = []
        
        if logic_type == "join":
            # Extract join key if possible
            join_match = re.search(r'\.join\([^,]+,\s*["\']?([^"\')\]]+)', code)
            if join_match:
                summary_parts.append(f"Joins on: {join_match.group(1)}")
            
            # Detect join type
            if "inner" in code.lower():
                summary_parts.append("Type: INNER (drops unmatched)")
            elif "left_anti" in code.lower() or "leftanti" in code.lower():
                summary_parts.append("Type: LEFT ANTI (keeps unmatched)")
            elif "left" in code.lower():
                summary_parts.append("Type: LEFT")
        
        elif logic_type == "filter":
            # Extract filter condition
            filter_match = re.search(r'\.filter\(([^)]+)\)', code)
            if filter_match:
                summary_parts.append(f"Condition: {filter_match.group(1)[:100]}")
        
        elif logic_type == "aggregation":
            # Extract group by columns
            gb_match = re.search(r'\.groupBy\(["\']?([^"\')\]]+)', code)
            if gb_match:
                summary_parts.append(f"Groups by: {gb_match.group(1)}")
        
        return "; ".join(summary_parts) if summary_parts else f"Logic type: {logic_type}"
    
    def parse_step(
        self, 
        step_info: StepInfo,
        source_tables: Optional[List[str]] = None,
        target_tables: Optional[List[str]] = None
    ) -> ParsedStep:
        """
        Parse a single step and extract logic metadata.
        
        Args:
            step_info: Step with code content
            source_tables: Externally provided source tables (from Input/Lineage)
            target_tables: Externally provided target tables (from Input/Lineage)
        """
        code = step_info.code_content or ""
        
        # Detect logic
        logic_type = self._detect_logic_type(code)
        logic_summary = self._generate_logic_summary(code, logic_type)
        
        return ParsedStep(
            task_key=step_info.task_key,
            code_content=code,
            source_tables=source_tables or [],
            target_tables=target_tables or [],
            logic_type=logic_type,
            logic_summary=logic_summary
        )
    
    def parse_all(
        self, 
        steps: List[StepInfo],
        table_mapping: Optional[dict] = None
    ) -> List[ParsedStep]:
        """
        Parse all steps and return enriched metadata.
        
        Args:
            steps: List of steps with code content
            table_mapping: Optional dict {task_key: {"sources": [...], "targets": [...]}}
        """
        parsed = []
        table_mapping = table_mapping or {}
        
        for step in steps:
            if step.code_content:
                tables = table_mapping.get(step.task_key, {})
                parsed.append(self.parse_step(
                    step,
                    source_tables=tables.get("sources"),
                    target_tables=tables.get("targets")
                ))
            else:
                print(f"Warning: No code content for step {step.task_key}")
        
        return parsed
