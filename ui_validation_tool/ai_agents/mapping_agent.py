from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from agents import Agent
from .runner import Runner
from backend.config import get_config
from backend.config_loader import ConfigLoader
from backend.ast_parser import ASTParser
from backend.symbol_resolver import SymbolResolver
import logging
import json
import re
import asyncio
import nest_asyncio

logger = logging.getLogger(__name__)

class DataAsset(BaseModel):
    """Represents a resolved data asset (Table or File)."""
    asset_type: str = Field(description="TABLE or FILE")
    usage: str = Field(description="SOURCE or TARGET")
    identifier: str = Field(description="Full table name or file path")
    confidence: str = Field(description="HIGH, MEDIUM, or LOW")
    evidence: str = Field(description="How this was resolved (e.g. 'Config key x', 'AST extraction')")

class HybridResult(BaseModel):
    """Output of the Hybrid Analysis."""
    assets: List[DataAsset] = Field(description="List of all identified sources and targets")
    logic_summary: str = Field(description="Summary of the transformation logic")
    resolution_trace: List[str] = Field(description="Step-by-step resolution log")

class MappingAgent:
    def __init__(self):
        self.config = get_config()
        # We reuse the agent for "Logic Summary" or as a fallback for complex extraction
        self.agent = Agent(
             name="HybridAgent",
             model=self.config.model,
             model_settings=self.config.model_settings,
             instructions="You are a Data Logic Summarizer. Given code and resolved tables, summarize the logic.",
             output_type=HybridResult # Reusing structure, though we heavily rely on deterministic tools
        )

    async def analyze_code_async(self, code_context: dict) -> HybridResult:
        """
        Executes the 3-Layer Resolution Pipeline:
        1. Config Check (Ground Truth)
        2. AST Extraction (Deterministic)
        3. Symbol Resolution (Safe Eval)
        + LLM (Logic Summary)
        """
        if not code_context:
             return HybridResult(assets=[], logic_summary="Empty context", resolution_trace=[])

        # --- Layer 1: Config Loading ---
        # Parse 'Metadata' from context to get Job Params
        job_params = {}
        if "__metadata__" in code_context:
             meta = code_context.pop("__metadata__")
             # Heuristic to parse job params if available in metadata string
             # Metadata format: "Package: ... Parameters: {'env': 'prod', ...}"
             m = re.search(r"Parameters: (\{.*?\})", meta)
             if m:
                 try: 
                    # simplistic fix for python dict string to json
                    import ast
                    job_params = ast.literal_eval(m.group(1))
                 except Exception as e: 
                    logger.warning(f"Failed to parse job params: {e}")

        config_loader = ConfigLoader()
        # In a real app config_dir would be configurable. Defaulting to 'conf' relative to CWD.
        # Pass code_context as config_files so it can find 'defaults.yaml' etc.
        config = config_loader.load_configs(job_params=job_params, config_files=code_context)
        
        # --- Layer 2 & 3: AST & Resolve ---
        resolver = SymbolResolver(config)
        resolved_trace = []
        found_assets = []
        
        # Sort files to process utils/config files first? 
        # For now, just process all.
        
        for filename, content in code_context.items():
            if filename == "__metadata__": continue
            
            resolved_trace.append(f"Analyzing {filename}...")
            
            # --- Handler: Python ---
            if filename.endswith(".py"):
                # AST Parse
                parser = ASTParser()
                parser.parse(content)
                
                # Resolve Assignments to build local context
                local_vars = {}
                for var, val_node in parser.assignments.items():
                    val, conf = resolver.resolve(val_node, local_vars)
                    if val:
                        local_vars[var] = val
                        resolved_trace.append(f"  Defined {var} = {val} ({conf})")

                # Resolve I/O Calls from AST
                for io in parser.io_calls:
                    op = io['operation'] # READ / WRITE
                    arg_node = io['arg_node']
                    
                    val, conf = resolver.resolve(arg_node, local_vars)
                    
                    evidence = f"AST extraction line {io['line']}"
                    if conf == 'HIGH': evidence += " + Config/Literal Resolution"
                    elif conf == 'MEDIUM': evidence += " + Variable Tracing"
                    
                    if val:
                        self._add_asset(found_assets, resolved_trace, val, op, conf, evidence)
                    else:
                        resolved_trace.append(f"  Unresolved {op} at line {io['line']}")

            # --- Handler: SQL ---
            elif filename.endswith(".sql"):
                # Simple Regex for now
                # Sources: FROM x, JOIN y
                # Targets: INSERT INTO z, MERGE INTO w
                
                # Sources
                sources = re.findall(r"(?:FROM|JOIN)\s+([a-zA-Z0-9_.]+)", content, re.IGNORECASE)
                for s in sources:
                    self._add_asset(found_assets, resolved_trace, s, "READ", "HIGH", "SQL Regex Extraction")
                    
                # Targets
                targets = re.findall(r"(?:INSERT\s+INTO|MERGE\s+INTO|UPDATE)\s+([a-zA-Z0-9_.]+)", content, re.IGNORECASE)
                for t in targets:
                    self._add_asset(found_assets, resolved_trace, t, "WRITE", "HIGH", "SQL Regex Extraction")

        # --- Layer 4: LLM Logic Summary (Optional) ---
        # For efficiency, we just return the deterministic results + simple summary
        logic_summary = f"Identified {len(found_assets)} assets using Config-Driven Resolution."
        
        return HybridResult(
            assets=found_assets,
            logic_summary=logic_summary,
            resolution_trace=resolved_trace
        )

    def _add_asset(self, assets_list, trace, val, op, conf, evidence):
        """Helper to add unique assets."""
        # Heuristic for Asset Type
        asset_type = "FILE" if ("/" in val or "abfss:" in val or "dbfs:" in val) else "TABLE"
        usage_type = "SOURCE" if op == "READ" else "TARGET"
        
        # Deduplicate based on identifier
        if not any(a.identifier == val for a in assets_list):
            asset = DataAsset(
                asset_type=asset_type,
                usage=usage_type,
                identifier=val,
                confidence=conf,
                evidence=evidence
            )
            assets_list.append(asset)
            trace.append(f"  Found {op} {asset_type}: {val} (Constraint: {conf})")

    def analyze_code(self, code_context: dict) -> HybridResult:
        """Sync wrapper."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            nest_asyncio.apply()
            return loop.run_until_complete(self.analyze_code_async(code_context))
        return loop.run_until_complete(self.analyze_code_async(code_context))
