# Agentic RCA Monorepo

This repository contains two independent projects related to the Agentic Root Cause Analysis system.

## Projects

### 1. `agentic_rca/`
**The Core Analysis Engine.**
- **Role**: Backend Service / Agent.
- **Function**: Investigates pipeline failures using Databricks APIs, Metrics, and LLMs.
- **Environment**: Optimized for Databricks Runtime (Linux/Spark), but runnable locally.
- **Key Feature**: Drift-Aware Analysis using the "Portable Manifest".

### 2. `ui_validation_tool/`
**The Configuration & Lineage Builder.**
- **Role**: Frontend / Utility.
- **Function**: Allows users to validate manifest files, map lineage, and embed code snapshots.
- **Environment**: Runs locally (Streamlit) on User's Laptop.
- **Output**: Generates the `manifest.json` used by `agentic_rca`.

### 3. `observability/`
**The Data Collector.**
- **Role**: Metrics Ingestion Service.
- **Function**: Queries Databricks (SQL/System Tables) to fetch metrics for a Job Run.
- **Output**: Saves metrics to Delta Table (`metrics_history`) for RCA to analyze.

## Workflow
1. **Validation**: Run `ui_validation_tool` to analyze your project and generate a `manifest.json`.
2. **Observability**: Run `python observability/collect_metrics.py --job-id <ID>` to ingest metrics into Databricks.
3. **Investigation**: Run `python rca/main.py --job-id <ID> --manifest <manifest.json>` to diagnose issues.

Each component operates independently. The `manifest.json` from Step 1 is the shared contract.
