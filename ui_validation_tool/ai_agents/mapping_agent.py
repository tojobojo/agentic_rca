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

MAPPING_INSTRUCTIONS = """
You are an expert Data Engineer specializing in PySpark/Databricks code analysis.
Your goal is to extract the **exact full table names** used as Sources (Read) and Targets (Write).

### CRITICAL: VARIABLE RESOLUTION
You will often see variables used in table operations, e.g., `spark.table(input_table)`.
**You MUST strictly separate VARIABLES from LITERALS.**

**Algorithm for Resolution:**
1.  **Identify**: Found `spark.read.table(x)`. Is `x` a string literal?
    - YES: Output it.
    - NO: It is a variable. **TRACE BACKWARD**.
2.  **Trace Backward**:
    - Look for assignments: `x = "catalog.schema.table"`?
    - Look for f-strings: `x = f"{env}.sales"` -> Search for `env`.
    - Look for function args: `def run(x):` -> Search for callers `run("literal_value")`.
    - Look for Job Parameters: Search the `Metadata` section.
3.  **Result**:
    - If resolved: Output the **resolved literal** value (e.g., `prod.sales_data`).
    - If partially resolved: Output the best inference (e.g., `{env}.sales_data`).
    - If unresolved: Output `VAR(x)` to indicate it's a variable.

### Rules
- **Sources**: `spark.table()`, `spark.read`, `FROM table`, `join(table)`.
- **Targets**: `.saveAsTable()`, `.insertInto()`, `MERGE INTO target`, `COPY INTO`.
- **Context**: You are provided with multiple files. Use the file hierarchy to trace imports and calls.
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
        MAX_CHARS = 120000 # Increase limit for multi-file contexts
        
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
            return result.final_output
        except Exception as e:
            logger.error(f"Agent analysis failed: {e}")
            return TableMapping(sources=[], targets=[], logic_summary=f"Analysis failed: {str(e)}")

    def analyze_code(self, code_context: dict) -> TableMapping:
        """Sync wrapper."""
        return asyncio.run(self.analyze_code_async(code_context))
