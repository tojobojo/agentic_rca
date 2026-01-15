from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from agents import Agent, Runner
from backend.config import get_config
import logging
import json
import asyncio
import nest_asyncio

logger = logging.getLogger(__name__)

class DataAsset(BaseModel):
    """Represents a resolved data asset (Table or File)."""
    asset_type: str = Field(description="TABLE or FILE")
    usage: str = Field(description="SOURCE or TARGET")
    identifier: str = Field(description="Full table name or file path")
    confidence: str = Field(description="HIGH, MEDIUM, or LOW")
    evidence: str = Field(description="How this was resolved (e.g. 'Config key', 'Code usage')")

class HybridResult(BaseModel):
    """Output of the Agentic Analysis."""
    assets: List[DataAsset] = Field(description="List of all identified sources and targets")
    logic_summary: str = Field(description="Summary of the transformation logic")
    resolution_trace: List[str] = Field(description="Step-by-step resolution log")
    ignored_files: List[str] = Field(default=[], description="List of files ignored by Context Pruner")

class MappingAgent:
    def __init__(self):
        self.config = get_config()
        
        # --- Agent 1: File Filter (Context Pruner) ---
        self.filter_agent = Agent(
             name="FileFilterAgent",
             model=self.config.model,
             model_settings=self.config.model_settings,
             instructions="""
You are an Intelligent File Filter for Data Lineage Analysis.
Your Goal: Given a Task Name and a list of File Names, identify ONLY the files relevant for extracting data lineage (sources and targets).

Rules:
1. **Config Files**: ALWAYS keep configuration files that appear to be for PRODUCTION (e.g., 'conf/prod/sales.yaml', 'prod.json'). 
   - IGNORE 'dev', 'staging', or 'test' configs unless no other configs exist.
   - If a file is just 'config.yaml', keep it.
2. **Task Relevance**: Identify the main script based on the Task Name.
   - Example: if task='process_sales', keep 'sales_etl.py' or 'process_sales_job.py'.
   - Include utils or shared modules ONLY if they seem critical for defining table names.
3. **Minimize Noise**: DISCARD unrelated scripts, unit tests, and documentation.
4. **Output**: Return the list of relevant filenames as a simple JSON list of strings.
""",
             output_type=List[str]
        )

        # --- Agent 2: Lineage Extractor ---
        self.extraction_agent = Agent(
            name="LineageExtractionAgent",
            model=self.config.model,
            model_settings=self.config.model_settings,
            instructions="""
You are a Data Lineage Extraction Expert.
Your Goal: Analyze the provided Code and Configuration files to extract Input Data (Sources) and Output Data (Targets).

Rules:
1. **Analyze Configs First**: Look for keys like `source_table`, `target_table`, `input_path`, `output_path`, or specific dataset names in the YAML/JSON content. 
   - Config values are usually the GROUND TRUTH. Confidence = HIGH.
2. **Analyze Code**: Look for data reading/writing patterns.
   - Spark: `spark.read.table(...)`, `spark.table(...)`, `df.write.saveAsTable(...)`.
   - SQL: `FROM table_name`, `JOIN table_name`, `INSERT INTO table_name`.
   - Python: Variable assignments that hold table names.
3. **Contextual Intelligence**:
   - If the code uses a variable `conf['input_table']`, look up 'input_table' in the provided config file content.
4. **Output**: Return a structured list of `DataAsset` objects (Source/Target, Identifier, Confidence).
   - `identifier` should be the fully qualified table name (catalog.schema.table) or file path if possible.
   - `confidence`: HIGH (Config/Explicit), MEDIUM (Variable/Inferred), LOW (Guessed).
   - `evidence`: Briefly explain where you found it (e.g., "Found in prod.yaml key 'source_table'").
5. **Logic Summary**: Provide a brief 1-sentence summary of what the job does.
""",
            output_type=HybridResult
        )

    async def analyze_code_async(self, code_context: dict) -> HybridResult:
        """
        Executes the 2-Step Agentic Pipeline:
        1. File Filtering (LLM)
        2. Lineage Extraction (LLM)
        """
        if not code_context:
            return HybridResult(assets=[], logic_summary="Empty context", resolution_trace=[])

        resolution_trace = []
        all_files = [f for f in code_context.keys() if f != "__metadata__"]
        
        # Metadata extraction
        task_info = code_context.get("__metadata__", "")
        
        # --- Step 1: File Filtering ---
        relevant_files = all_files
        ignored_files = []
        
        if len(all_files) > 1:
            resolution_trace.append(f"Step 1: Filtering relevant files from {len(all_files)} candidates...")
            try:
                filter_prompt = f"Task Info: {task_info}\nFiles: {json.dumps(all_files)}"
                filter_result = await Runner.run(self.filter_agent, filter_prompt)
                
                # Handle potential output wrapper
                if hasattr(filter_result, "final_output_as"):
                    relevant_files = filter_result.final_output_as(list)
                else:
                    relevant_files = filter_result

                # Fallback safety
                if not relevant_files:
                    relevant_files = all_files
                    resolution_trace.append("  Filter returned empty, keeping all files.")
                else:
                    ignored_files = list(set(all_files) - set(relevant_files))
                    logger.info(f"Context Pruner kept {len(relevant_files)}/{len(all_files)} files.")
                    logger.info(f"Relevant Files: {relevant_files}")
                    resolution_trace.append(f"  Selected {len(relevant_files)} files: {relevant_files}")
                    if ignored_files:
                        resolution_trace.append(f"  Ignored: {ignored_files}")
                        
            except Exception as e:
                logger.error(f"Filtering failed: {e}")
                resolution_trace.append(f"  Filtering failed ({e}), using all files.")
                relevant_files = all_files

        # --- Step 2: Lineage Extraction ---
        resolution_trace.append("Step 2: Extracting lineage (Batched Code/Config)...")
        
        # Split into Configs (Context) and Scripts (Logic)
        config_files = []
        script_files = []
        
        for f in relevant_files:
            # Heuristic: Configs are yaml/json or in conf folders
            is_config = any(f.endswith(ext) for ext in ['.yaml', '.yml', '.json', '.toml', '.ini'])
            if is_config or 'conf/' in f or 'config/' in f:
                config_files.append(f)
            else:
                script_files.append(f)
        
        # Prepare Shared Config Content
        config_context_str = ""
        for fname in config_files:
            if fname in code_context:
                config_context_str += f"\n--- CONFIG: {fname} ---\n{code_context[fname]}\n"

        all_assets = []
        
        # 2a. Analyze Configs (All together - usually low token count output)
        if config_context_str:
            try:
                resolution_trace.append(f"Analyzing {len(config_files)} config files...")
                config_prompt = f"Task Info: {task_info}\n\nAnalyze these CONFIGURATION files for finding source/target tables:\n{config_context_str}"
                
                res = await Runner.run(self.extraction_agent, config_prompt)
                if hasattr(res, "final_output_as"):
                    res = res.final_output_as(HybridResult)
                
                all_assets.extend(res.assets)
                resolution_trace.extend([f"Config: {t}" for t in res.resolution_trace])
            except Exception as e:
                logger.error(f"Config analysis failed: {e}")
                resolution_trace.append(f"Config analysis failed: {e}")

        # 2b. Analyze Scripts (Sequentially - reduces burst output tokens)
        for fname in script_files:
            if fname not in code_context: continue
            
            try:
                resolution_trace.append(f"Analyzing script: {fname}...")
                content = code_context[fname]
                
                # Context includes Configs for reference + Current Script
                script_prompt = f"""
Task Info: {task_info}

Generic Config Context (For Reference Only - Do not re-extract assets from here unless used in code):
{config_context_str}

--- CODE TO ANALYZE ({fname}) ---
{content}
"""
                res = await Runner.run(self.extraction_agent, script_prompt)
                if hasattr(res, "final_output_as"):
                    res = res.final_output_as(HybridResult)
                
                all_assets.extend(res.assets)
                resolution_trace.extend([f"{fname}: {t}" for t in res.resolution_trace])
                
            except Exception as e:
                logger.error(f"Analysis of {fname} failed: {e}")
                resolution_trace.append(f"Analysis of {fname} failed: {e}")

        # Deduplicate Assets (by identifier)
        unique_assets = {}
        for a in all_assets:
            if a.identifier not in unique_assets:
                unique_assets[a.identifier] = a
            else:
                # Keep the one with higher confidence if duplicate
                existing = unique_assets[a.identifier]
                if a.confidence == "HIGH" and existing.confidence != "HIGH":
                    unique_assets[a.identifier] = a

        final_assets = list(unique_assets.values())
        
        return HybridResult(
            assets=final_assets,
            logic_summary=f"Analyzed {len(relevant_files)} files. Found {len(final_assets)} assets.",
            resolution_trace=resolution_trace,
            ignored_files=ignored_files
        )

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

