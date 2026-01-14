from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from agents import Agent, Runner
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
    ignored_files: List[str] = Field(default=[], description="List of files ignored by Context Pruner")

class MappingAgent:
    def __init__(self):
        self.config = get_config()
        # We reuse the agent for "Logic Summary" or as a fallback for complex extraction
        self.agent = Agent(
            name="HybridAgent",
            model=self.config.model,
            model_settings=self.config.model_settings,
            instructions="You are a Data Logic Summarizer. Given code and resolved tables, summarize the logic.",
            output_type=HybridResult 
        )
        
        # New: Context Pruner Agent
        self.pruner = Agent(
             name="ContextPruner",
             model=self.config.model,
             model_settings=self.config.model_settings,
             instructions="""
You are an Intelligent Code Context Pruner.
Your Goal: Given a Task Name, Job Parameters, and a list of File Names, identify ONLY the relevant files for analysis.

Rules:
1. ALWAYS keep 'conf/' or 'config/' files.
2. ALWAYS keep 'utils/', 'common/', or 'shared/' files.
3. Identify the main script based on the Task Name (e.g. task='process_sales' -> keep 'sales_etl.py').
4. Keep any other files that likely contain logic for this specific task.
5. DISCARD unrelated scripts (e.g. 'marketing_etl.py' if task is 'sales').
6. Look for ENVIRONMENT hints in file paths (e.g. 'conf/prod/...' or 'deploy/production/...'). PRIORITIZE 'prod'/'production' paths if multiple environments are present.
7. Return the list of relevant filenames as a simple JSON list of strings.
""",
             output_type=List[str]
        )

    async def analyze_code_async(self, code_context: dict) -> HybridResult:
        """
        Executes the 3-Layer Resolution Pipeline:
        1. Context Pruning (LLM)
        2. Config Check (Ground Truth)
        3. AST Extraction (Deterministic)
        4. Symbol Resolution (Safe Eval)
        + LLM (Logic Summary)
        """
        if not code_context:
             return HybridResult(assets=[], logic_summary="Empty context", resolution_trace=[])

        # --- Layer 0: Context Pruning ---
        # Identify task key from metadata if possible, or just pass all context keys
        task_info = ""
        job_params = {}
        if "__metadata__" in code_context:
             task_info = code_context["__metadata__"]
             # Extract params for config loader
             m = re.search(r"Parameters: (\{.*?\})", task_info)
             if m:
                 try:
                    import ast
                    job_params = ast.literal_eval(m.group(1))
                 except: pass

        all_files = [f for f in code_context.keys() if f != "__metadata__"]
        
        # LLM Pruning
        resolution_trace = []
        ignored_files = []
        relevant_files = all_files

        # Only prune if we have enough files to warrant it (e.g. > 3)
        if len(all_files) > 3:
            resolution_trace.append(f"Pruning context from {len(all_files)} files...")
                prompt = f"Task Info: {task_info}\nFiles: {json.dumps(all_files)}"
                result = await Runner.run(self.pruner, prompt)
                
                # Extract list from RunResult
                if hasattr(result, "final_output_as"):
                    relevant_files = result.final_output_as(list)
                else:
                    # Fallback if it returns raw data or something else (unlikely given the error)
                    relevant_files = result

                # Safety net: Ensure we didn't lose everything or config files
                # (The Agent instructions say always keep conf/, but let's double check code_context keys)
                # Actually, let's just trust the LLM but fallback if empty
                if not relevant_files:
                    relevant_files = all_files
                    resolution_trace.append("  Pruner returned empty, keeping all.")
                else:
                    # diff
                    ignored_files = list(set(all_files) - set(relevant_files))
                    resolution_trace.append(f"  Kept {len(relevant_files)} files, ignored {len(ignored_files)}.")
            except Exception as e:
                logger.error(f"Pruning failed: {e}")
                resolution_trace.append(f"  Pruning failed ({e}), keeping all.")
                relevant_files = all_files

        # --- Layer 1: Config Loading ---
        config_loader = ConfigLoader()
        # Pass code_context (FULL context or PRUNED? Config loader needs configs even if pruned? 
        # Pruner rule 1 says always keep config. So relevant_files should have them.)
        
        # We need a dict for ConfigLoader. 
        # But wait, if Pruner dropped 'defaults.yaml', we are in trouble.
        # Let's forcefully keep defaults.yaml / conf/ in the dict passed to ConfigLoader?
        # Better: Pass full code_context to ConfigLoader, but only analyze relevant_files in loop.
        
        config = config_loader.load_configs(job_params=job_params, config_files=code_context)
        
        # --- Layer 2 & 3: AST & Resolve ---
        resolver = SymbolResolver(config)
        found_assets = []
        
        for filename in relevant_files:
            if filename not in code_context: continue # Should not happen
            content = code_context[filename]
            
            resolution_trace.append(f"Analyzing {filename}...")
            
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
                        resolution_trace.append(f"  Defined {var} = {val} ({conf})")

                # Resolve I/O Calls from AST
                for io in parser.io_calls:
                    op = io['operation'] # READ / WRITE
                    arg_node = io['arg_node']
                    
                    val, conf = resolver.resolve(arg_node, local_vars)
                    
                    evidence = f"AST extraction line {io['line']}"
                    if conf == 'HIGH': evidence += " + Config/Literal Resolution"
                    elif conf == 'MEDIUM': evidence += " + Variable Tracing"
                    
                    if val:
                        self._add_asset(found_assets, resolution_trace, val, op, conf, evidence)
                    else:
                        resolution_trace.append(f"  Unresolved {op} at line {io['line']}")

            # --- Handler: SQL ---
            # --- Handler: SQL ---
            elif filename.endswith(".sql"):
                # Robust SQL Extraction
                # 1. Identify CTEs to exclude (e.g. "WITH cte_name AS")
                cte_names = set(re.findall(r"WITH\s+([a-zA-Z0-9_]+)\s+AS", content, re.IGNORECASE))
                
                # 2. Keywords to Ignore
                sql_keywords = {
                    "SELECT", "FROM", "WHERE", "JOIN", "AND", "OR", "ON", "IN", "NOT", "NULL", 
                    "GROUP", "ORDER", "BY", "HAVING", "LIMIT", "UNION", "ALL", "LEFT", "RIGHT", 
                    "INNER", "OUTER", "CROSS", "LATERAL", "VALUES", "UNNEST", "PARTITION", 
                    "OVER", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE", "END", "JSON"
                }

                # Helper to process extracted tokens
                def process_sql_token(token, op):
                    # Filter: Keywords
                    if token.upper() in sql_keywords: return
                    # Filter: CTEs
                    if token in cte_names: return
                    # Filter: Numbers or Invalid Start
                    if token[0].isdigit(): return

                    # Confidence Logic
                    conf = "HIGH"
                    evidence = "SQL Regex Extraction"
                    
                    if "." not in token:
                        # Single word identifier -> High chance of being an alias or local view
                        conf = "LOW" 
                        evidence += " (No Schema Qualifier, likely alias)"
                    
                    self._add_asset(found_assets, resolution_trace, token, op, conf, evidence)

                # Sources: FROM x, JOIN y
                sources = re.findall(r"(?:FROM|JOIN)\s+([a-zA-Z0-9_.]+)", content, re.IGNORECASE)
                for s in sources:
                    process_sql_token(s, "READ")
                    
                # Targets: INSERT INTO z, MERGE INTO w
                targets = re.findall(r"(?:INSERT\s+INTO|MERGE\s+INTO|UPDATE)\s+([a-zA-Z0-9_.]+)", content, re.IGNORECASE)
                for t in targets:
                    process_sql_token(t, "WRITE")

        # --- Layer 4: LLM Logic Summary (Optional) ---
        logic_summary = f"Identified {len(found_assets)} assets using Config-Driven Resolution."
        
        return HybridResult(
            assets=found_assets,
            logic_summary=logic_summary,
            resolution_trace=resolution_trace,
            ignored_files=ignored_files
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
