# Agentic RCA System - Databricks Native

## 🚀 Overview

The **Agentic RCA System** is a production-grade framework designed to automatically detect, diagnose, and explain data anomalies (like row drops) in Databricks ETL pipelines.

It leverages a **Databricks-Native Architecture**, using Unity Catalog, Delta Transaction Logs, and AI Agents to provide "self-healing" observability.

- **Schema Fetching**: Automatic table DDL retrieval for enhanced AI context
- **Pydantic Validation**: Runtime validation for all configuration and data models
- **Performance Telemetry**: Built-in performance tracking and reporting

## 🏗️ Architecture

The system is organized around a strictly decoupled **Execution Context** which drives both detection and investigation.

| Component | Responsibility | Key Tech |
| :--- | :--- | :--- |
| **Observability Collector** | The "Sentry". Collects execution metrics and Delta history statistics. | Databricks Jobs API, Delta Log |
| **Execution Context** | The "Assembler". Rebuilds the code + data + schema state of a specific pipeline step. | Git, Lineage API, Schema Fetching |
| **Anomaly Engine** | The "Monitor". Detects statistical or logic-based anomalies. | Z-Score, Configurable Thresholds |
| **RCA Agent** | The "Detective". AI Agent that investigates anomalies using tools. | OpenAI Agents SDK, Spark SQL |
| **Telemetry** | The "Observer". Tracks performance metrics across the workflow. | Phase Timers, Performance Reports |
| **Orchestrator** | Coordinates the entire workflow. | Python |

## 🛠️ Installation

```bash
# 1. Clone the repository
git clone https://github.com/your-org/agentic-rca.git
cd agentic-rca

# 2. Install dependencies
pip install -r requirements.txt
# Requirements: databricks-sdk, pyspark, openai-agents, pydantic, tqdm
```

## ⚙️ Configuration

Set the following Environment Variables or Databricks Secrets:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DATABRICKS_HOST` | Your Workspace URL (e.g. `https://adb-123...`) | Required |
| `DATABRICKS_TOKEN` | Personal Access Token | Required |
| `OPENAI_API_KEY` | Key for the RCA Agent model | Required |
| `GITLAB_URL` | URL for code discovery | Required |
| `RCA_ANOMALY_Z_SCORE` | Z-score threshold for anomaly detection | 3.0 |
| `RCA_ANOMALY_DROP_RATE` | Drop rate threshold (0-1) | 0.1 |
| `RCA_LLM_MAX_RETRIES` | Max retries for LLM API calls | 3 |

## 🏃 Usage

Run the `main.py` orchestrator to analyze a Databricks Job.

**Analyze Latest Run (Default):**
```bash
python main.py --job-id 12345
```

**Analyze Specific Run:**
```bash
python main.py --job-id 12345 --run-id 98765
```

### Arguments
- `--job-id`: **(Required)** The Databricks Job ID.
- `--run-id`: (Optional) Specific Run ID. Defaults to **latest run** if omitted.
- `--collect`: (Optional) Force fresh metric collection.
- `--manifest`: (Optional) Path to table mapping JSON.

---

### 📡 Standalone Metrics Collection (Phase 1)

You can run Phase 1 independently using the dedicated script:

```bash
# Collect for latest run
python collect_metrics.py --job-id 12345

# Collect for specific run
python collect_metrics.py --job-id 12345 --run-id 98765
```

This is useful for scheduling regular observability capture without running full analysis.

## 📊 Output

The system generates a Markdown report (`rca_report.md`) containing:
1. **Executive Summary**: Overview of anomalies found.
2. **Performance Metrics**: Execution time breakdown by phase.
3. **Root Cause Analysis**: Detailed AI investigation for every anomaly, including evidence and SQL query results.
4. **Execution Log**: Status of each pipeline step (OK / ANOMALY).

### Sample Performance Metrics
```markdown
## Performance Metrics

- **Total Execution Time**: 45.23s
- **Discovery**: 8.12s
- **Context Building**: 22.45s (15 steps)
- **Anomaly Detection**: 3.21s
- **AI Investigation**: 11.45s (3 anomalies)

**Average Time per Step**: 1.50s
```

## 📂 Project Structure

```
├── main.py                     # Entry point & Orchestrator
├── config.py                   # Configuration Management (Pydantic)
├── observability_collector.py  # Metrics Collection
├── execution_context.py        # Context Building (Git + Lineage + Schemas)
├── anomaly_engine.py           # Anomaly Detection Logic
├── rca_agent.py                # AI Agent Definition
├── discovery_agent.py          # Code Discovery & Git Integration
├── lineage_client.py           # Unity Catalog Integration
├── pipeline_parser.py          # Logic Type Detection
├── telemetry.py                # Performance Tracking
└── requirements.txt            # Dependencies
```

## 🔧 Advanced Features

### Schema Fetching
Automatically fetches table DDL for all source and target tables, providing the AI agent with complete schema context for better investigations.

### Pydantic Validation
All configuration and data models use Pydantic for runtime validation, ensuring data integrity and providing clear error messages.

### Performance Telemetry
Built-in performance tracking measures execution time for each phase (Discovery, Context Building, Detection, Investigation) and includes metrics in the final report.

### Retry Logic
LLM API calls include exponential backoff retry logic for improved reliability.

### Git Caching
Repository cloning is cached to avoid repeated downloads on subsequent runs.

