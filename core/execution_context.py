"""
Execution Context Module.
The "Context Assembler" of the Agentic RCA system.
Consolidates Discovery (Git) and Parsing (Logic) into a single context builder.
"""
import logging
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field

from config.config import get_config, _get_or_create_spark
from agents.discovery_agent import DiscoveryAgent, StepInfo
from utils.pipeline_parser import PipelineParser, ParsedStep # Reuse existing logic for now
from utils.lineage_client import get_step_tables

logger = logging.getLogger(__name__)

class ExecutionContext(BaseModel):
    """Rich context for a specific execution step."""
    run_id: str
    job_id: str
    step_id: str  # task_key
    
    # Code Context
    code_content: str
    git_file_path: str
    git_commit: str
    
    # Logic Context
    logic_type: str
    logic_summary: str
    
    # Data Context
    source_tables: List[str]
    target_tables: List[str]
    schemas: Dict[str, str] = Field(default_factory=dict)  # table -> ddl

class ExecutionContextBuilder:
    """
    Builds the ExecutionContext for a given run and step.
    """
    
    def __init__(self):
        self.config = get_config()
        self.discovery = DiscoveryAgent()
        self.parser = PipelineParser()
        self._schema_cache: Dict[str, str] = {}  # Cache schemas to avoid repeated queries
    
    def _fetch_schemas(self, tables: List[str]) -> Dict[str, str]:
        """Fetch DDL schemas for tables."""
        schemas = {}
        spark = _get_or_create_spark()
        if not spark:
            logger.warning("Spark session not available for schema fetching")
            return schemas
        
        for table in tables:
            # Check cache first
            if table in self._schema_cache:
                schemas[table] = self._schema_cache[table]
                continue
            
            try:
                # Fetch DDL using SHOW CREATE TABLE
                result = spark.sql(f"SHOW CREATE TABLE {table}").collect()
                if result:
                    ddl = result[0][0]
                    schemas[table] = ddl
                    self._schema_cache[table] = ddl
                    logger.info(f"Fetched schema for {table}")
                else:
                    schemas[table] = "Error: No schema returned"
            except Exception as e:
                error_msg = f"Error: {str(e)}"
                logger.warning(f"Could not fetch schema for {table}: {e}")
                schemas[table] = error_msg
                self._schema_cache[table] = error_msg
        
        return schemas
        
    def build_context(
        self, 
        job_id: int, 
        run_id: int, 
        step_id: str,
        manifest_data: Optional[Dict] = None
    ) -> ExecutionContext:
        """
        Build the full execution context for a step.
        """
        logger.info(f"Building context for Job {job_id}, Run {run_id}, Step {step_id}")
        
        # 1. Discover Code (Git)
        # We assume the repo is already cloned or we clone it now.
        # Ideally, we should pass the gitlab_url from config or job settings.
        gitlab_url = self.config.gitlab_url
        if not gitlab_url:
             logger.warning("No GitLab URL configured. Code discovery might fail.")
        
        # This calls clone internally if needed
        steps = self.discovery.discover(job_id, gitlab_url or "")
        
        # Find the specific step
        target_step: Optional[StepInfo] = None
        for s in steps:
            if s.task_key == step_id:
                target_step = s
                break
        
        if not target_step:
            raise ValueError(f"Step {step_id} not found in Job {job_id}")
            
        # 2. Get Lineage (Data)
        # We fetch lineage for this specific step
        # Pass manifest_data as fallback
        table_map = get_step_tables(job_id, [step_id], fallback_to_manifest=manifest_data)
        tables = table_map.get(step_id, {})
        source_tables = tables.get("sources", [])
        target_tables = tables.get("targets", [])
        
        # 3. Parse Logic (Intent)
        parsed: ParsedStep = self.parser.parse_step(
            target_step, 
            source_tables=source_tables, 
            target_tables=target_tables
        )
        # 4. Fetch Schemas (Metadata)
        all_tables = list(set(source_tables + target_tables))  # Unique tables
        schemas = self._fetch_schemas(all_tables) if all_tables else {}
        
        return ExecutionContext(
            run_id=str(run_id),
            job_id=str(job_id),
            step_id=step_id,
            code_content=target_step.code_content or "",
            git_file_path=target_step.git_file_path or "",
            git_commit="HEAD", # TODO: Get actual commit
            logic_type=parsed.logic_type,
            logic_summary=parsed.logic_summary,
            source_tables=source_tables,
            target_tables=target_tables,
            schemas=schemas
        )


