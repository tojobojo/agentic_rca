# Implementation Plan - Validation UI Layer

## Goal
Build a standalone UI Layer (using Streamlit) that allows users to:
1. Input Databricks Job ID and Git Repo URL.
2. Select tasks to analyze.
3. Automatically map code files and identify Source/Target tables using an AI Agent.
4. Validate and edit these mappings.
5. Generate a `manifest.json` for the core RCA system.

## Proposed Architecture

### Directory Structure
New isolated directory: `ui_validation_tool/`

```
ui_validation_tool/
├── app.py                  # Main Streamlit UI
├── backend/
│   ├── databricks_client.py # Wrapper for Databricks SDK (Jobs/Tasks)
│   ├── git_manager.py       # Wrapper for Git Cloning (Reuses core/DiscoveryAgent logic)
│   └── state_manager.py     # Session State helper
├── agents/
│   └── mapping_agent.py     # OpenAI Agent for Table Discovery
└── requirements.txt         # UI-specific dependencies
```

## User Review Required
**IMPORTANT**
Tech Choice: We will use Streamlit for the UI. It provides the fastest way to build data-interactive tools in Python, allows direct integration with the openai-agents-sdk, and fits the "Internal Tool" use case perfectly.

## Proposed Changes

### 1. Project Setup
**[NEW] `ui_validation_tool/requirements.txt`**
- `streamlit`
- Dependencies from main `requirements.txt` (databricks-sdk, openai-agents, etc.)

### 2. Backend Services
**[NEW] `ui_validation_tool/backend/databricks_client.py`**
- `get_job_tasks(job_id)`: Fetches job definition.
- Filters for unique tasks and preserves dependency order.

**[NEW] `ui_validation_tool/backend/git_manager.py`**
- Leverages existing `DiscoveryAgent` or implements similar logic to clone repo and finding matching files for selected tasks.

### 3. AI Agent
**[NEW] `ui_validation_tool/agents/mapping_agent.py`**
- Uses `openai-agents-sdk`.
- Prompt: Reads Python/SQL code and extracts:
  - `sources`: List of ADLS paths or Delta tables read.
  - `targets`: List of ADLS paths or Delta tables written.
- Returns structured JSON.

### 4. Frontend
**[NEW] `ui_validation_tool/app.py`**
- **Step 1: Input Form** (Job ID, GitLab URL).
- **Step 2: Task Selection** (Checkbox list with "Select All").
- **Step 3: "Analyze" Button** -> Triggers Agent.
- **Step 4: Data Editor** (Editable Table) showing Task | Script | Sources | Targets.
- **Step 5: "Save Manifest" Button** -> Writes `manifest.json`.

## Verification Plan
1. **Manual Test**: Run `streamlit run ui_validation_tool/app.py`.
2. Input a valid Job ID and Repo.
3. Verify tasks are listed.
4. Run analysis and check if the Agent correctly identifies tables in the sample code.
5. Save manifest and verify it can be loaded by `main.py`.
