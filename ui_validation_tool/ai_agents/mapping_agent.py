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
                    resolution_trace.append(f"  Selected {len(relevant_files)} files: {relevant_files}")
                    if ignored_files:
                        resolution_trace.append(f"  Ignored: {ignored_files}")
                        
            except Exception as e:
                logger.error(f"Filtering failed: {e}")
                resolution_trace.append(f"  Filtering failed ({e}), using all files.")
                relevant_files = all_files

        # --- Step 2: Lineage Extraction ---
        resolution_trace.append("Step 2: Extracting lineage from selected files...")
        
        # Prepare context for the extraction agent
        # We construct a string or dict representation of the file contents.
        # To avoid token limits, we might want to truncate very large files, but for now we pass mostly raw.
        extraction_context = ""
        for fname in relevant_files:
            if fname in code_context:
                content = code_context[fname]
                extraction_context += f"\n--- FILE: {fname} ---\n{content}\n"

        try:
            extraction_prompt = f"Task Info: {task_info}\n\nCode and Config Context:\n{extraction_context}"
            
            result = await Runner.run(self.extraction_agent, extraction_prompt)
            
            if hasattr(result, "final_output_as"):
                extraction_result = result.final_output_as(HybridResult)
            else:
                extraction_result = result

            # Merge traces
            final_trace = resolution_trace + extraction_result.resolution_trace
            extraction_result.resolution_trace = final_trace
            extraction_result.ignored_files = ignored_files
            
            return extraction_result

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            resolution_trace.append(f"Extraction failed: {e}")
            return HybridResult(
                assets=[], 
                logic_summary=f"Error during extraction: {e}", 
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

