"""
Agentic RCA System for Databricks ETL Pipelines.

A production-grade Root Cause Analysis system that:
- Discovers pipeline structure from Databricks Jobs API
- Maps code from GitLab repositories  
- Collects observability metrics and detects anomalies
- Uses AI agents to investigate and explain data drops
"""

from .config import Config, get_config
from .discovery_agent import DiscoveryAgent, StepInfo
from .pipeline_parser import PipelineParser, ParsedStep
from .anomaly_engine import AnomalyDetectionEngine, Anomaly
from .execution_context import ExecutionContextBuilder, ExecutionContext
from .observability_collector import ObservabilityCollector, MetricRecord
from .rca_agent import RCAAgent
from .lineage_client import LineageClient, TableLineage, get_step_tables
from .main import run_rca_orchestrator

__version__ = "2.0.0"
__all__ = [
    "Config",
    "get_config", 
    "DiscoveryAgent",
    "StepInfo",
    "PipelineParser", 
    "ParsedStep",
    "AnomalyDetectionEngine",
    "Anomaly",
    "ExecutionContextBuilder",
    "ExecutionContext",
    "ObservabilityCollector", 
    "MetricRecord",
    "RCAAgent",
    "LineageClient",
    "TableLineage", 
    "get_step_tables",
    "run_rca_orchestrator",
]
