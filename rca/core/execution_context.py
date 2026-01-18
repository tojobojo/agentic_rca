"""
Execution Context Module.
The "Context Assembler" of the Agentic RCA system.
Consolidates Discovery (Git) and Parsing (Logic) into a single context builder.
"""
import logging
from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field

from config.config import get_config, _get_or_create_spark
from ai_agents.discovery_agent import DiscoveryAgent, StepInfo
from utils.lineage_client import get_step_tables

logger = logging.getLogger(__name__)

class ExecutionContext(BaseModel):
    """Rich context for a specific execution step."""
    run_id: str
    job_id: str
    step_id: str  # task_key
    
    # Code Context
    code_content: str
    code_content: str
    code_source_type: str # WORKSPACE, MANIFEST, or N/A
    
    # Logic Context
    logic_type: str
    logic_summary: str
    
    # Data Context
    source_tables: List[str]
    target_tables: List[str]
    schemas: Dict[str, str] = Field(default_factory=dict)  # table -> ddl
    metrics_snapshot: Dict[str, Any] = Field(default_factory=dict) # Rich metrics (Source/Target/Nulls)
    
    # Drift Detection
    is_drift_detected: bool = False
    manifest_code_snapshot: Optional[str] = None

class ExecutionContextBuilder:
    """
    Builds the ExecutionContext for a given run and step.
    """
    
    def __init__(self):
        self.config = get_config()
        self.discovery = DiscoveryAgent()
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
        
        # 1. Discover Code (Manifest)
        # We no longer clone Git. We use the Manifest mapping.
        steps = self.discovery.discover(job_id, manifest_data=manifest_data)
        
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
        
        # 3. Parse Logic (Simplified)
        # We no longer rely on complex Regex parsing here. 
        # The LLM will analyze the code content directly.
        logic_type = "GENERIC"
        logic_summary = "Logic analysis deferred to LLM"
        
        # 4. Fetch Schemas (Metadata)
        all_tables = list(set(source_tables + target_tables))  # Unique tables
        schemas = self._fetch_schemas(all_tables) if all_tables else {}
        
        return ExecutionContext(
            run_id=str(run_id),
            job_id=str(job_id),
            step_id=step_id,
            code_content=target_step.code_content or "",
            code_source_type=target_step.code_source_type or "N/A", 
            logic_type=logic_type,
            logic_summary=logic_summary,
            source_tables=source_tables,
            target_tables=target_tables,
            schemas=schemas,
            is_drift_detected=target_step.is_drift_detected,
            manifest_code_snapshot=target_step.manifest_code_snapshot
        )

