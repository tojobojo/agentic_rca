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
You are an expert Data Engineer.
Your task is to analyze the provided Python/SQL code (PySpark/Databricks) and identify:
1. Source Tables (tables READ from)
2. Target Tables (tables WRITTEN to)

Rules:
- Identify tables referenced in `spark.table('...')`, `spark.read.table('...')`, `spark.sql('FROM ...')`.
- Identify write targets in `.write.saveAsTable('...')`, `.insertInto('...')`, `MERGE INTO ...`.
- Context implies Databricks/Delta Lake.
- If a table name is constructed dynamically (e.g. variables), try to infer the intent or return the variable name representation.
- Be accurate.
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

    async def analyze_code_async(self, code_content: str) -> TableMapping:
        """Analyze code to find tables."""
        if not code_content or len(code_content.strip()) < 10:
             return TableMapping(sources=[], targets=[], logic_summary="Empty or too short code")

        prompt = f"Analyze this code and extract sources/targets:\n\n```python\n{code_content[:10000]}\n```" # Truncate if too huge
        
        try:
            result = await Runner.run(self.agent, prompt)
            return result.final_output
        except Exception as e:
            logger.error(f"Agent analysis failed: {e}")
            return TableMapping(sources=[], targets=[], logic_summary=f"Analysis failed: {str(e)}")

    def analyze_code(self, code_content: str) -> TableMapping:
        """Sync wrapper."""
        return asyncio.run(self.analyze_code_async(code_content))
