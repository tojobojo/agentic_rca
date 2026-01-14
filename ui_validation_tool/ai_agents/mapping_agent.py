from agents import Agent, Runner
from pydantic import BaseModel, Field
from typing import List
import logging
import asyncio
from backend.config import get_config

logger = logging.getLogger(__name__)

class TableMapping(BaseModel):
    """Structured output for table identification."""
    sources: List[str] = Field(description="List of source tables or paths read from")
    targets: List[str] = Field(description="List of target tables or paths written to")
    logic_summary: str = Field(description="One sentence summary of the transformation logic")
    resolution_trace: List[str] = Field(default=[], description="Step-by-step log of how variables were resolved to table names")

MAPPING_INSTRUCTIONS = """
You are an expert Data Engineer specializing in PySpark/Databricks code analysis.
Your goal is to extract the **exact full table names** used as Sources (Read) and Targets (Write).

### CRITICAL: TRACE LIKE A HUMAN
Do not guess. You must traverse the code logically to resolve variables.
Populate the `resolution_trace` with your step-by-step findings.

**Traversal Algorithm:**
1.  **Start at the IO Operation**: Find `spark.read.table(x)`.
2.  **Check for Literal**: Is `x` "catalog.schema.table"? -> Done.
3.  **Trace Variable**:
    - "Found variable `x`."
    - "Searching for assignment of `x` in current file..."
    - "Searching for function definition `def run(x)`... found caller `run('my_table')` in `main.py`."
    - "Checking Metadata for parameters..."
4.  **Conclude**: "Resolved `x` to `my_table`."

### Rules
- **Sources**: `spark.table()`, `spark.read.table()`, `spark.sql()`.
- **Path Sources**: Look for `spark.read.load("path")`, `.parquet("path")`, `.csv("path")`.
- **Targets**: `.saveAsTable()`, `.insertInto()`, `MERGE INTO`, `COPY INTO`.
- **Path Targets**: Look for `.write.save("path")`, `.parquet("path")`.
- **Valid Formats**:
    - **Catalog**: `catalog.schema.table`
    - **Paths**: `abfss://...` (ADLS), `dbfs:/...` (DBFS), `/mnt/...` (Mounts), `/Volumes/...` (Unity Catalog).
- **Resolution Trace**: You MUST provide a log of your "mental walk" through the code for EVERY table/path found.
"""

class MappingAgent:
    def __init__(self):
        self.config = get_config()
        self.agent = Agent(
            name="MappingAgent",
            model=self.config.model,
            model_settings=self.config.model_settings,
            instructions=MAPPING_INSTRUCTIONS,
            output_type=TableMapping
        )

    async def analyze_code_async(self, code_context: dict) -> TableMapping:
        """Analyze code context to find tables."""
        if not code_context:
             return TableMapping(sources=[], targets=[], logic_summary="Empty code context")

        # Construct prompt from dictionary
        prompt_parts = ["Analyze this code context and extract sources/targets:\n"]
        
        # Add metadata if present
        if "__metadata__" in code_context:
            prompt_parts.append(f"Metadata:\n{code_context.pop('__metadata__')}\n")
            
        total_chars = 0
        MAX_CHARS = 120000 
        
        file_names = list(code_context.keys())
        logger.info(f"Analyzing files: {file_names}")
        
        for filename, content in code_context.items():
            if total_chars > MAX_CHARS:
                prompt_parts.append(f"\n... (Truncated remaining files due to size) ...")
                break
                
            file_block = f"\nFile: {filename}\n```python\n{content}\n```\n"
            prompt_parts.append(file_block)
            total_chars += len(file_block)

        prompt = "".join(prompt_parts)
        
        try:
            result = await Runner.run(self.agent, prompt)
            
            # Log the trace for visibility
            if result.final_output.resolution_trace:
                logger.info(f"Resolution Trace for {file_names}:")
                for step in result.final_output.resolution_trace:
                    logger.info(f"  -> {step}")
            
            return result.final_output
        except Exception as e:
            logger.error(f"Agent analysis failed: {e}")
            return TableMapping(sources=[], targets=[], logic_summary=f"Analysis failed: {str(e)}")

    def analyze_code(self, code_context: dict) -> TableMapping:
        """Sync wrapper."""
        return asyncio.run(self.analyze_code_async(code_context))
