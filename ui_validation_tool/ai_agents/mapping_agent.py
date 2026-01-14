from agents import Agent, Runner
from pydantic import BaseModel, Field
from typing import List
import logging
import asyncio
from backend.config import get_config

logger = logging.getLogger(__name__)

class TraceResult(BaseModel):
    """Output of the Trace Agent: identified relevant code blocks."""
    relevant_files: List[str] = Field(description="List of filenames that are actually used/called during execution")
    trace_reasoning: str = Field(description="Explanation of the execution flow and why these files were selected")

class TableMapping(BaseModel):
    """Structured output for table identification."""
    sources: List[str] = Field(description="List of source tables or paths read from")
    targets: List[str] = Field(description="List of target tables or paths written to")
    logic_summary: str = Field(description="One sentence summary of the transformation logic")
    resolution_trace: List[str] = Field(default=[], description="Step-by-step log of how variables were resolved to table names")

TRACE_INSTRUCTIONS = """
You are the **Navigator Agent**. Your job is to map the execution flow.
1.  **Start** at the entry point (implied by the task metadata or main script).
2.  **Trace** all imports and function calls.
3.  **Filter**: Identify ONLY the files and modules that are *actually used* in this specific job logic. Ignore unused utils, tests, or dead code.
4.  **Output**: A list of `relevant_files` and your `trace_reasoning`.

Do NOT extract tables yet. Just find the code that matters.
"""

EXTRACT_INSTRUCTIONS = """
You are the **Analyzer Agent**. 
You are provided with a *Pruned Code Context* containing only relevant files.

**Goal**: Extract Source and Target tables/paths.

### CRITICAL: TRACE VARIABLES
You must resolve variables to their literal values.
**Traversal Algorithm:**
1.  **Start at IO**: Find `spark.read.load(x)` / `table(x)`.
2.  **Trace**: Look for `x`'s definition in the provided files.
3.  **Resolve**: `x = "db.table"` -> Source is `db.table`.

### Output rules
- **Sources**: Tables/Paths read.
- **Targets**: Tables/Paths written.
- **Trace**: Log your resolution steps in `resolution_trace`.
"""

class MappingAgent:
    def __init__(self):
        self.config = get_config()
        
        # Agent 1: Navigator
        self.trace_agent = Agent(
            name="TraceAgent",
            model=self.config.model,
            model_settings=self.config.model_settings,
            instructions=TRACE_INSTRUCTIONS,
            output_type=TraceResult
        )

        # Agent 2: Analyzer
        self.extract_agent = Agent(
            name="ExtractAgent",
            model=self.config.model,
            model_settings=self.config.model_settings,
            instructions=EXTRACT_INSTRUCTIONS,
            output_type=TableMapping
        )

    async def analyze_code_async(self, code_context: dict) -> TableMapping:
        """Two-step analysis: Trace -> Extract."""
        if not code_context:
             return TableMapping(sources=[], targets=[], logic_summary="Empty code context")

        # --- Step 1: Trace (Navigator) ---
        logger.info(f"Step 1: Tracing execution flow in {len(code_context)} files...")
        
        # Build Context String for Trace
        prompt_parts = ["Map the execution flow for this task:\n"]
        if "__metadata__" in code_context:
            prompt_parts.append(f"Metadata:\n{code_context['__metadata__']}\n")
        
        # We pass minimal context or full headers for tracing? 
        # Passing full content is safer for 120B.
        total_chars = 0
        MAX_CHARS = 100000 
        
        for filename, content in code_context.items():
            if filename == "__metadata__": continue
            if total_chars > MAX_CHARS:
                 prompt_parts.append(f"\n... (Truncated for Trace) ...")
                 break
            prompt_parts.append(f"\nFile: {filename}\n```python\n{content}\n```\n")
            total_chars += len(content)

        trace_prompt = "".join(prompt_parts)
        
        try:
            trace_result = await Runner.run(self.trace_agent, trace_prompt)
            relevant_files = trace_result.final_output.relevant_files
            trace_reasoning = trace_result.final_output.trace_reasoning
            logger.info(f"Trace Complete. Relevant files: {relevant_files}")
        except Exception as e:
            logger.error(f"Trace Agent failed: {e}. Falling back to analyzing all files.")
            relevant_files = list(code_context.keys())
            trace_reasoning = "Trace failed, using all files."

        # --- Step 2: Extract (Analyzer) ---
        logger.info(f"Step 2: Extracting from {len(relevant_files)} relevant files...")
        
        extract_parts = ["Analyze this PRUNED context to extract tables:\n"]
        if "__metadata__" in code_context:
             extract_parts.append(f"Metadata:\n{code_context['__metadata__']}\n")
        
        # Filter context
        pruned_context_size = 0
        used_files = []
        
        for filename, content in code_context.items():
            if filename == "__metadata__": continue
            # loose matching for filenames
            if any(f in filename for f in relevant_files) or filename in relevant_files or len(relevant_files) == 0:
                 extract_parts.append(f"\nFile: {filename}\n```python\n{content}\n```\n")
                 pruned_context_size += len(content)
                 used_files.append(filename)
        
        extract_parts.append(f"\nNavigator Context: {trace_reasoning}\n")
        extract_prompt = "".join(extract_parts)

        try:
            extract_result = await Runner.run(self.extract_agent, extract_prompt)
            mapping = extract_result.final_output
            
            # Prepend Trace reasoning to the logic summary or trace for visibility
            mapping.resolution_trace.insert(0, f"Navigator: {trace_reasoning}")
            
            # Log trace
            logger.info(f"Extraction Complete for {used_files}")
            return mapping
            
        except Exception as e:
            logger.error(f"Extract Agent failed: {e}")
            return TableMapping(sources=[], targets=[], logic_summary=f"Analysis failed: {str(e)}")

    def analyze_code(self, code_context: dict) -> TableMapping:
        """Sync wrapper that handles existing event loops."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            # If we are in a running loop (e.g. Streamlit/Jupyter), we check for nest_asyncio
            # If not present, we might risk RuntimeError. But usually this is the fix.
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(self.analyze_code_async(code_context))
        else:
            return loop.run_until_complete(self.analyze_code_async(code_context))
