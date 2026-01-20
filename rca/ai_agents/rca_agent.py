"""
RCA Agent Module.
Implements the RCADetective agent using the OpenAI Agents SDK.
The agent autonomously investigates anomalies using Spark SQL tools.
"""
from typing import Optional, List
from dataclasses import dataclass
import logging

from agents import Agent, Runner, function_tool
from agents.run_context import RunContextWrapper
import asyncio

from core.anomaly_engine import Anomaly
from core.execution_context import ExecutionContext
from config.config import get_config

logger = logging.getLogger(__name__)


# --- Tool Definitions ---
# These are the tools the RCA Agent can use to investigate anomalies.

@function_tool
def get_table_schema(table_name: str) -> str:
    """
    Get the schema (columns and types) of a Databricks table.
    Use this to understand the structure of source/target tables.
    
    Args:
        table_name: Fully qualified table name (e.g., catalog.schema.table)
    
    Returns:
        DDL string describing the table schema
    """
    try:
        from config.config import _get_or_create_spark
        spark = _get_or_create_spark()
        
        # Get schema
        df = spark.table(table_name)
        schema_str = df._jdf.schema().treeString()
        return schema_str
    except Exception as e:
        return f"Error getting schema for {table_name}: {str(e)}"


@function_tool
def query_spark_sql(query: str) -> str:
    """
    Execute a SparkSQL query on the Databricks cluster and return results.
    Use this to verify hypotheses about data issues (e.g., count nulls, check key distributions).
    
    IMPORTANT: Always use SELECT queries. Do not modify data.
    Keep queries simple and focused (e.g., COUNT, GROUP BY).
    
    Args:
        query: A valid SparkSQL SELECT query
    
    Returns:
        Query results as a formatted string (limited to 20 rows)
    """
    try:
        from config.config import _get_or_create_spark
        spark = _get_or_create_spark()
        
        # Safety check: Only allow SELECT
        if not query.strip().upper().startswith("SELECT"):
            return "Error: Only SELECT queries are allowed."
        
        # Execute
        result_df = spark.sql(query)
        rows = result_df.limit(20).collect()
        
        if not rows:
            return "Query returned 0 rows."
        
        # Format as table
        columns = result_df.columns
        output = " | ".join(columns) + "\n"
        output += "-" * len(output) + "\n"
        for row in rows:
            output += " | ".join(str(row[c]) for c in columns) + "\n"
        
        return output
    except Exception as e:
        return f"Error executing query: {str(e)}"


@function_tool
def count_nulls_in_column(table_name: str, column_name: str) -> str:
    """
    Count the number of NULL values in a specific column of a table.
    Use this to quickly check for NULL-related join failures.
    
    Args:
        table_name: Fully qualified table name
        column_name: Name of the column to check
    
    Returns:
        Count of NULL values and percentage
    """
    try:
        from config.config import _get_or_create_spark
        import pyspark.sql.functions as F
        
        spark = _get_or_create_spark()
        df = spark.table(table_name)
        
        total = df.count()
        null_count = df.filter(col(column_name).isNull()).count()
        pct = (null_count / total * 100) if total > 0 else 0
        
        return f"Table: {table_name}\nColumn: {column_name}\nTotal Rows: {total}\nNULL Count: {null_count} ({pct:.1f}%)"
    except Exception as e:
        return f"Error counting nulls: {str(e)}"


@function_tool
def get_delta_history(table_name: str, num_versions: int = 5) -> str:
    """
    Get the transaction history for a Delta Lake table.
    Use this to check recent writes, deletes, or unexpected overwrites.
    
    NOTE: Only works for Delta tables. Will error for Parquet/Hive tables.
    
    Args:
        table_name: Fully qualified Delta table name
        num_versions: Number of recent versions to show (default: 5)
    
    Returns:
        Transaction history showing operations, timestamps, and metrics
    """
    from config.config import _get_or_create_spark
    spark = _get_or_create_spark()
    try:
        history_df = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT {num_versions}")
        rows = history_df.select(
            "version", "timestamp", "operation", "operationParameters", "operationMetrics"
        ).collect()
        
        if not rows:
            return f"No history found for {table_name}"
        
        output = f"Delta History for: {table_name}\n"
        output += "=" * 50 + "\n"
        
        for row in rows:
            output += f"\nVersion {row.version} ({row.timestamp})\n"
            output += f"  Operation: {row.operation}\n"
            
            if row.operationMetrics:
                metrics = row.operationMetrics
                if "numOutputRows" in metrics:
                    output += f"  Rows Written: {metrics['numOutputRows']}\n"
                if "numDeletedRows" in metrics:
                    output += f"  Rows Deleted: {metrics['numDeletedRows']}\n"
                if "numTargetRowsInserted" in metrics:
                    output += f"  Rows Inserted: {metrics['numTargetRowsInserted']}\n"
                if "numTargetRowsUpdated" in metrics:
                    output += f"  Rows Updated: {metrics['numTargetRowsUpdated']}\n"
        
        return output
    except Exception as e:
        if "not a Delta table" in str(e) or "DESCRIBE HISTORY" in str(e):
            return f"Table {table_name} is not a Delta table. History not available."
        return f"Error getting Delta history: {str(e)}"


# --- Agent Definition ---

RCA_DETECTIVE_INSTRUCTIONS = """
You are an expert Data Engineer investigating why a Databricks ETL pipeline step dropped rows.

Your Task:
1. Analyze the provided step code to understand the logic (JOIN, FILTER, AGGREGATION, etc.)
2. Use the available tools to verify your hypotheses about why rows were dropped.
3. Provide a clear, actionable explanation of the root cause.

Guidelines:
- For JOINs: Check for NULL keys, unmatched keys, or data skew.
- For FILTERs: Check how many rows fail the filter condition.
- For AGGREGATIONs: Row reduction is expected; focus on unexpected changes.
- For Delta tables: Check history for unexpected overwrites or deletes.
- Always quantify your findings (e.g., "X rows had NULL customer_id").
- Suggest fixes when possible.

Output Format:
## Root Cause Summary
[Brief summary of the main issue]

## Evidence
[Specific data points from your investigations]

## Recommendation
[Suggested fix or next steps]
"""


class RCAAgent:
    """
    The Investigator: Uses OpenAI Agent to analyze and explain row drops.
    """
    
    def __init__(self):
        self.config = get_config()
        
        # Define the agent
        self.agent = Agent(
            name="RCADetective",
            model=self.config.model,
            instructions=RCA_DETECTIVE_INSTRUCTIONS,
            tools=[
                get_table_schema,
                query_spark_sql,
                count_nulls_in_column,
                get_delta_history,
            ],
            model_settings={
                "temperature": self.config.temperature,
                "max_tokens": self.config.max_tokens,
                "timeout": self.config.llm_timeout
            }
        )
    
    def _build_prompt(self, anomaly: Anomaly, context: ExecutionContext) -> str:
        """Build the investigation prompt for the agent."""
        
        # 1. Format Metrics
        metrics_info = f"""
        Metric: {anomaly.metric_name}
        Current Value: {anomaly.current_value:.2f}
        Historical Avg: {anomaly.historical_avg:.2f}
        Z-Score: {anomaly.deviation_z_score:.2f}
        """
        
        # 2. Format Forensic Evidence (Deep Inspection)
        forensics = "No deep inspection metrics available."
        snapshot = context.metrics_snapshot
        if snapshot:
            rows_in = snapshot.get("rows_in", 0)
            rows_out = snapshot.get("rows_out", 0)
            duration = snapshot.get("duration_ms", 0)
            
            # Helper to format nulls/distincts
            def fmt_cols(metrics_list):
                if not metrics_list: return "None"
                # Combine info from all sources/targets
                combined = []
                for m in metrics_list:
                    name = m.get("target_table", "Unknown")
                    nulls = m.get("rows_null_vital", {})
                    distincts = m.get("distinct_counts", {})
                    
                    # Find interesting stats (non-zero nulls)
                    issues = []
                    for col, count in nulls.items():
                        if count > 0:
                            issues.append(f"{col}: {count} NULLs")
                    
                    combined.append(f"  - {name}: {m.get('rows_total', 0)} rows. Issues: {', '.join(issues) or 'None'}")
                return "\n".join(combined)

            forensics = f"""
            **Execution Stats**:
            - Duration: {duration} ms
            - Total Input Rows: {rows_in}
            - Total Output Rows: {rows_out}
            
            **Source Details**:
            {fmt_cols(snapshot.get("metrics_by_type", {}).get("SOURCE", []))}
            
            **Target Details**:
            {fmt_cols(snapshot.get("metrics_by_type", {}).get("TARGET", []))}
            """
        
        # 3. Code Drift Analysis
        drift_section = ""
        if context.is_drift_detected and context.manifest_code_snapshot:
            import difflib
            # Generate unified diff
            diff = difflib.unified_diff(
                context.manifest_code_snapshot.splitlines(),
                context.code_content.splitlines(),
                fromfile='Manifest Code',
                tofile='Executed Code',
                lineterm=''
            )
            diff_text = "\n".join(list(diff))
            
            drift_section = f"""
            > [!WARNING] CODE DRIFT DETECTED
            > The code executed differs from the version in the lineage manifest.
            > This change might be the root cause of the anomaly.
            
            **Code Changes (Diff)**:
            ```diff
            {diff_text}
            ```
            """
            
        prompt = f"""
Investigate the following anomaly in a Databricks ETL pipeline:

**Step Name**: {context.step_id}
**Logic Type**: {context.logic_type}
**Logic Summary**: {context.logic_summary}

**Source Tables**: {', '.join(context.source_tables) or 'Unknown'}
**Target Tables**: {', '.join(context.target_tables) or 'Unknown'}

**Anomaly Details**:
{metrics_info}
**Reason**: {anomaly.reason}

**Forensic Evidence (Deep Inspection)**:
{forensics}

{drift_section}

**Step Code (Executed)**:
```python
{context.code_content[:3000]}
```

Please investigate why this step behaved anomalously.
1. **CRITICAL**: Check the Code Drift section above. Did a recent code change cause this?
2. Analyze the Code Logic vs the Forensic Evidence.
3. If Source has NULLs in join keys (see Evidence), that is a likely cause.
4. If Duration is high, check for Cartesian joins or scan issues.
5. Use tools (query_spark_sql, get_delta_history) ONLY if needed to verify missing details.
"""
        return prompt
    
    async def analyze_async(self, anomaly: Anomaly, context: ExecutionContext) -> str:
        """
        Analyze an anomaly using the RCA Agent (async version).
        Includes retry logic with exponential backoff.
        Returns the agent's explanation.
        """
        prompt = self._build_prompt(anomaly, context)
        
        max_retries = self.config.llm_max_retries
        retry_delay = self.config.llm_retry_delay_seconds
        
        for attempt in range(max_retries):
            try:
                result = await Runner.run(
                    self.agent,
                    prompt,
                )
                
                final_report = result.final_output
                
                # Append Token Usage if available
                if hasattr(result, "usage") and result.usage:
                    u = result.usage
                    final_report += f"\n\n## Token Usage\n"
                    final_report += f"- Total: {u.total_tokens}\n"
                    final_report += f"- Prompt: {u.prompt_tokens}\n"
                    final_report += f"- Completion: {u.completion_tokens}\n"
                    
                return final_report
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(
                        f"LLM call failed (attempt {attempt + 1}/{max_retries}): {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"LLM call failed after {max_retries} attempts: {e}")
                    return f"""## Error

Failed to analyze anomaly after {max_retries} attempts.

**Error**: {str(e)}

**Anomaly Details**:
- Step: {context.step_id}
- Metric: {anomaly.metric_name}
- Current Value: {anomaly.current_value:.2f}
- Reason: {anomaly.reason}

**Recommendation**: Please check the OpenAI API key and model configuration, then retry the analysis.
"""
    
    def analyze(self, anomaly: Anomaly, context: ExecutionContext) -> str:
        """
        Analyze an anomaly using the RCA Agent (sync wrapper).
        """
        import asyncio
        
        # Handle event loop in different environments
        try:
            loop = asyncio.get_running_loop()
            # If we're already in an async context, use run_until_complete on a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, self.analyze_async(anomaly, context))
                return future.result()
        except RuntimeError:
            # No running loop, we can use asyncio.run
            return asyncio.run(self.analyze_async(anomaly, context))
    
    def analyze_all(self, anomalies_with_context: List[tuple]) -> List[str]:
        """
        Analyze all anomalies and return a list of reports.
        Args:
            anomalies_with_context: List of (Anomaly, ExecutionContext) tuples
        """
        reports = []
        
        for i, (anomaly, context) in enumerate(anomalies_with_context):
            logger.info("[RCA Agent] Analyzing anomaly %d/%d: %s...", i+1, len(anomalies_with_context), context.step_id)
            try:
                report = self.analyze(anomaly, context)
                reports.append(f"# RCA for Step: {context.step_id}\n\n{report}")
            except Exception as e:
                error_report = f"# RCA for Step: {context.step_id}\n\nError during analysis: {str(e)}"
                reports.append(error_report)
                logger.error("RCA error for %s: %s", context.step_id, e)
        
        return reports
