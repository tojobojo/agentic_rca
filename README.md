# Agentic RCA System - Databricks Native

## 🚀 Overview

The **Agentic RCA System** is a production-grade framework designed to automatically detect, diagnose, and explain data anomalies (like row drops) in Databricks ETL pipelines.

It leverages a **Databricks-Native Architecture**, using Unity Catalog, Delta Transaction Logs, and AI Agents to provide "self-healing" observability.

## 🏗️ Architecture

The system is organized around a strictly decoupled **Execution Context** which drives both detection and investigation.

| Component | Responsibility | Key Tech |
| :--- | :--- | :--- |
| **Observability Collector** | The "Sentry". Collects execution metrics and Delta history statistics. | Databricks Jobs API, Delta Log |
| **Execution Context** | The "Assembler". Rebuilds the code + data state of a specific pipeline step. | Git, Lineage API, AST Parsing |
| **Anomaly Engine** | The "Monitor". Detects statistical or logic-based anomalies. | Z-Score, Thresholds |
| **RCA Agent** | The "Detective". AI Agent that investigates anomalies using tools. | OpenAI, Spark SQL |
| **Orchestrator** | Coordinates the entire workflow. | Python |

## 🛠️ Installation

```bash
# 1. Clone the repository
git clone https://github.com/your-org/agentic-rca.git
cd agentic-rca

# 2. Install dependencies
pip install -r requirements.txt
# Requirements: databricks-sdk, pyspark, openai, numpy
```

## ⚙️ Configuration

Set the following Environment Variables or Databricks Secrets:

| Variable | Description |
| :--- | :--- |
| `DATABRICKS_HOST` | Your Workspace URL (e.g. `https://adb-123...`) |
| `DATABRICKS_TOKEN` | Personal Access Token |
| `OPENAI_API_KEY` | Key for the RCA Agent model |
| `GITLAB_URL` | URL for code discovery (optional) |

## 🏃 Usage

Run the `main.py` orchestrator to analyze a specific Databricks Job Run:

```bash
python main.py \
  --job-id 12345 \
  --run-id 98765 \
  --collect \
  --manifest sample_manifest.json
```

### Arguments
- `--job-id`: The Databricks Job ID to analyze.
- `--run-id`: The specific Run ID where the issue occurred.
- `--collect`: (Optional) Force fresh metric collection from Delta Logs.
- `--manifest`: (Optional) Path to a JSON file mapping task keys to tables (fallback for Lineage API).

## 📊 Output

The system generates a Markdown report (`rca_report.md`) containing:
1.  **Executive Summary**: Overview of anomalies found.
2.  **Execution Log**: Status of each pipeline step (OK / ANOMALY).
3.  **Root Cause Analysis**: Detailed AI investigation for every anomaly, including evidence and SQL query results.

## 📂 Project Structure

```
├── main.py                     # Entry point & Orchestrator
├── observability_collector.py  # Metrics Collection
├── execution_context.py        # Context Building (Git + Lineage)
├── anomaly_engine.py           # Anomaly Detection Logic
├── rca_agent.py                # AI Agent Definition
├── config.py                   # Configuration Management
└── lineage_client.py           # Unity Catalog Integration
```
