"""
Agentic RCA System for Databricks ETL Pipelines.

A production-grade Root Cause Analysis system that:
- Discovers pipeline structure from Databricks Jobs API
- Maps code from GitLab repositories
- Validates row counts and detects anomalies
- Uses AI agents to investigate and explain data drops
"""

from .config import Config, get_config
from .discovery_agent import DiscoveryAgent, StepInfo
from .pipeline_parser import PipelineParser, ParsedStep
from .validation_engine import ValidationEngine, StepMetrics, Anomaly
from .rca_agent import RCAAgent
from .performance_agent import PerformanceAgent, PerformanceContext
from .history_collector import HistoryCollector, TableMetrics
from .lineage_client import LineageClient, TableLineage, get_step_tables
from .main import run_rca_pipeline

__version__ = "1.2.0"
__all__ = [
    "Config",
    "get_config",
    "DiscoveryAgent",
    "StepInfo",
    "PipelineParser",
    "ParsedStep",
    "ValidationEngine",
    "StepMetrics",
    "Anomaly",
    "RCAAgent",
    "PerformanceAgent",
    "PerformanceContext",
    "HistoryCollector",
    "TableMetrics",
    "LineageClient",
    "TableLineage",
    "get_step_tables",
    "run_rca_pipeline",
]
