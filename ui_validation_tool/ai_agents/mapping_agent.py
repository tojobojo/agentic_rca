from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from google.adk.agents import Agent
from google.adk.runners import InMemoryRunner
from google.genai import types
from backend.config import get_config
import logging
import json
import asyncio
import nest_asyncio

logger = logging.getLogger(__name__)

class DataAsset(BaseModel):
    """Represents a resolved data asset (Table or File)."""
    asset_type: str = Field(description="TABLE or FILE")
    subtype: str = Field(default="UNKNOWN", description="Detailed type: DELTA_TABLE, ADLS, S3, JDBC, PARQUET_FILE, etc.")
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
    # Added source_files to track what was analyzed (Source of Truth for Manifest)
    source_files: List[str] = Field(default=[], description="List of files that were actually analyzed")
    source_code_snapshot: Dict[str, str] = Field(default={}, description="Snapshot of the analyzed code content")
    token_stats: Dict[str, int] = Field(default={}, description="Token usage statistics (requests, input, output, total)")

class FilterResult(BaseModel):
    files: List[str] = Field(description="List of relevant filenames")

class ExtractionResult(BaseModel):
    """LLM Output Schema (No token_stats)."""
    assets: List[DataAsset] = Field(description="List of all identified sources and targets")
    logic_summary: str = Field(description="Summary of the transformation logic")
    resolution_trace: List[str] = Field(description="Step-by-step resolution log")

class MappingAgent:
    def __init__(self):
        self.config = get_config()
        
        # --- Agent 1: File Filter (Context Pruner) ---
        self.filter_agent = Agent(
             name="FileFilterAgent",
             model=self.config.model,
             # Pass a dict to avoid sending unsupported fields like reasoning_content defined in Types
             # Best effort config for JSON + Determinism
             generate_content_config={
                 "temperature": 0.1,
                 "max_output_tokens": self.config.max_tokens
             },
             instruction="""
You are a precise Data Pruning AI. You ONLY speak JSON.

Task: Identify relevant files for Data Lineage Extraction.
Input: A Task Name and a list of internal File Paths.

RULES:
1. MATCH Task Name logic.
2. KEEP Prod Configs.
3. DISCARD noise (tests, unrelated jobs).

OUTPUT FORMAT:
Strict JSON object with a single key "files". No markdown. No comments.

EXAMPLE:
{"files": ["src/job.py", "conf/prod.yaml"]}
""",
             output_schema=FilterResult
        )

        # --- Agent 2: Lineage Extractor ---
        self.extraction_agent = Agent(
            name="LineageExtractionAgent",
            model=self.config.model,
            # Pass a dict to avoid sending unsupported fields like reasoning_content defined in Types
            # Best effort config for JSON + Determinism
            generate_content_config={
                "temperature": 0.1,
                "max_output_tokens": self.config.max_tokens
            },
            instruction="""
You are a Data Lineage Extraction AI. You ONLY speak JSON.

Task: Extract Source/Target tables from Code/Configs.

RULES:
1. READ Configs for keys like `source_table`, `input_path`.
2. READ Code for Spark/SQL logic.
3. IDENTIFY Source vs Target usage.

OUTPUT FORMAT:
Strict JSON object. No markdown. No comments.

EXAMPLE:
{"assets": [{"asset_type": "TABLE", "subtype": "UNITY_CATALOG_TABLE", "usage": "SOURCE", "identifier": "catalog.schema.table", "confidence": "HIGH", "evidence": "Found in config"}], "logic_summary": "ETL job.", "resolution_trace": []}
""",
            output_schema=ExtractionResult
        )

        # --- Runners ---
        self._user_id = "mapping_agent_user"

        self._filter_runner = InMemoryRunner(
            agent=self.filter_agent,
            app_name="filter_app",
        )
        self._extraction_runner = InMemoryRunner(
            agent=self.extraction_agent,
            app_name="extraction_app",
        )

        # Pre-create one session per runner (reused for all calls)
        self._filter_session = None
        self._extraction_session = None

    async def _ensure_sessions(self):
        """Lazily create sessions on first use."""
        if self._filter_session is None:
            self._filter_session = await self._filter_runner.session_service.create_session(
                app_name="filter_app", user_id=self._user_id
            )
        if self._extraction_session is None:
            self._extraction_session = await self._extraction_runner.session_service.create_session(
                app_name="extraction_app", user_id=self._user_id
            )

    async def _run_agent(self, runner: InMemoryRunner, session_id: str, prompt: str) -> str:
        """Run an ADK agent and collect the text response."""
        content = types.Content(
            role="user",
            parts=[types.Part(text=prompt)],
        )

        final_text = ""
        async for event in runner.run_async(
            user_id=self._user_id,
            session_id=session_id,
            new_message=content,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        final_text += part.text
        
        return final_text

    async def analyze_code_async(self, code_context: dict, on_log=None) -> HybridResult:
        """
        Executes the 2-Step Agentic Pipeline:
        1. File Filtering (LLM)
        2. Lineage Extraction (LLM)
        """
        def log(msg):
            resolution_trace.append(msg)
            if on_log: on_log(msg)

        if not code_context:
            return HybridResult(assets=[], logic_summary="Empty context", resolution_trace=[])

        await self._ensure_sessions()

        resolution_trace = []
        token_usage = {"requests": 0, "input_tokens": 0, "output_tokens": 0, "total_tokens": 0}

        all_files = [f for f in code_context.keys() if f != "__metadata__"]
        
        # Metadata extraction
        task_info = code_context.get("__metadata__", "")
        
        # --- Step 1: File Filtering ---
        relevant_files = all_files
        ignored_files = []
        
        if len(all_files) > 1:
            log(f"Step 1: Filtering relevant files from {len(all_files)} candidates...")
            try:
                filter_prompt = f"Task Info: {task_info}\nFiles: {json.dumps(all_files)}"
                response_text = await self._run_agent(
                    self._filter_runner, self._filter_session.id, filter_prompt
                )
                
                token_usage["requests"] += 1

                # Parse the JSON response
                try:
                    filter_data = json.loads(response_text)
                    filter_result = FilterResult(**filter_data)
                except (json.JSONDecodeError, Exception) as e:
                    logger.warning(f"Failed to parse filter response as JSON: {e}. Raw: {response_text[:200]}")
                    # Try to extract JSON from the response
                    import re
                    json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                    if json_match:
                        filter_data = json.loads(json_match.group())
                        filter_result = FilterResult(**filter_data)
                    else:
                        raise ValueError(f"No valid JSON found in response: {response_text[:200]}")

                relevant_files = filter_result.files

                # Fallback safety
                if not relevant_files:
                    relevant_files = all_files
                    log("  Filter returned empty, keeping all files.")
                else:
                    ignored_files = list(set(all_files) - set(relevant_files))
                    logger.info(f"Context Pruner kept {len(relevant_files)}/{len(all_files)} files.")
                    logger.info(f"Relevant Files: {relevant_files}")
                    log(f"  Selected {len(relevant_files)} files: {relevant_files}")
                    if ignored_files:
                        log(f"  Ignored: {ignored_files}")
                        
            except Exception as e:
                logger.error(f"Filtering failed: {e}")
                log(f"  Filtering failed ({e}), using all files.")
                relevant_files = all_files

        # --- Step 2: Lineage Extraction ---
        log("Step 2: Extracting lineage (Batched Code/Config)...")
        
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
                log(f"Analyzing {len(config_files)} config files...")
                config_prompt = f"Task Info: {task_info}\n\nAnalyze these CONFIGURATION files for finding source/target tables:\n{config_context_str}"
                
                response_text = await self._run_agent(
                    self._extraction_runner, self._extraction_session.id, config_prompt
                )
                token_usage["requests"] += 1

                # Parse response
                try:
                    extraction_data = json.loads(response_text)
                    res = ExtractionResult(**extraction_data)
                except (json.JSONDecodeError, Exception):
                    import re
                    json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                    if json_match:
                        extraction_data = json.loads(json_match.group())
                        res = ExtractionResult(**extraction_data)
                    else:
                        raise ValueError(f"No valid JSON in config analysis response")

                all_assets.extend(res.assets)
                for t in res.resolution_trace: log(f"Config: {t}")
            except Exception as e:
                logger.error(f"Config analysis failed: {e}")
                log(f"Config analysis failed: {e}")

        # 2b. Analyze Scripts (Sequentially)
        for fname in script_files:
            if fname not in code_context: continue
            
            try:
                log(f"Analyzing script: {fname}...")
                content = code_context[fname]
                
                # Context includes Configs for reference + Current Script
                script_prompt = f"""
Task Info: {task_info}

Generic Config Context (For Reference Only - Do not re-extract assets from here unless used in code):
{config_context_str}

--- CODE TO ANALYZE ({fname}) ---
{content}
"""
                response_text = await self._run_agent(
                    self._extraction_runner, self._extraction_session.id, script_prompt
                )
                token_usage["requests"] += 1

                # Parse response
                try:
                    extraction_data = json.loads(response_text)
                    res = ExtractionResult(**extraction_data)
                except (json.JSONDecodeError, Exception):
                    import re
                    json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                    if json_match:
                        extraction_data = json.loads(json_match.group())
                        res = ExtractionResult(**extraction_data)
                    else:
                        raise ValueError(f"No valid JSON in script analysis response for {fname}")

                all_assets.extend(res.assets)
                for t in res.resolution_trace: log(f"{fname}: {t}")
                
            except Exception as e:
                logger.error(f"Analysis of {fname} failed: {e}")
                log(f"Analysis of {fname} failed: {e}")

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

        final_assets = []
        for a in unique_assets.values():
            a.subtype = self._classify_asset(a.identifier, a.asset_type)
            final_assets.append(a)
            
        return HybridResult(
            assets=final_assets,
            logic_summary=f"Analyzed {len(relevant_files)} files. Found {len(final_assets)} assets.",
            resolution_trace=resolution_trace,
            ignored_files=ignored_files,
            source_files=relevant_files, # Capture Source Files
            source_code_snapshot={f: code_context[f] for f in relevant_files if f in code_context}, # Capture Content
            token_stats=token_usage
        )

    def _classify_asset(self, identifier: str, asset_type: str) -> str:
        """Deterministically classifies the asset based on identifier patterns."""
        ident_lower = identifier.lower()
        # Deterministic check for Delta Path syntax
        if ident_lower.startswith("delta.") or "delta.`" in ident_lower:
             return "DELTA_PATH"

        if asset_type == "FILE" or "/" in ident_lower:
            # Cloud Storage
            if ident_lower.startswith(("abfss:", "abfs:", "adl:", "wasb:")): return "ADLS"
            if ident_lower.startswith(("s3:", "s3a:", "s3n:")): return "S3"
            if ident_lower.startswith("gs:"): return "GCS"
            if ident_lower.startswith("dbfs:"): return "DBFS"
            if ident_lower.startswith("file:"): return "LOCAL_FILE"
            
            # Formats
            if ident_lower.endswith(".parquet"): return "PARQUET_FILE"
            if ident_lower.endswith(".csv"): return "CSV_FILE"
            if ident_lower.endswith(".json"): return "JSON_FILE"
            if ident_lower.endswith(".avro"): return "AVRO_FILE"
            if ident_lower.endswith(".xml"): return "XML_FILE"
            if ident_lower.endswith("delta_log"): return "DELTA_PATH"
            
            return "FILE_PATH"

        elif asset_type == "TABLE":
            # JDBC / DB
            if ident_lower.startswith("jdbc:"):
                if "postgres" in ident_lower: return "JDBC_POSTGRES"
                if "mysql" in ident_lower: return "JDBC_MYSQL"
                if "oracle" in ident_lower: return "JDBC_ORACLE"
                if "sqlserver" in ident_lower: return "JDBC_SQLSERVER"
                return "JDBC_DB"
            
            # Catalog Tables
            parts = identifier.split(".")
            if len(parts) == 3: return "UNITY_CATALOG_TABLE"
            if len(parts) == 2: return "HIVE_METASTORE_TABLE"
            
        return "GENERIC_TABLE"

    def analyze_code(self, code_context: dict, on_log=None) -> HybridResult:
        """Sync wrapper."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            nest_asyncio.apply()
            return loop.run_until_complete(self.analyze_code_async(code_context, on_log))
        return loop.run_until_complete(self.analyze_code_async(code_context, on_log))
