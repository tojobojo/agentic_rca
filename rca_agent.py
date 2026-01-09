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

from validation_engine import Anomaly
from config import get_config

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
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
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
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
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
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, when
        
        spark = SparkSession.builder.getOrCreate()
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
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
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
            model=self.config.openai_model,
            instructions=RCA_DETECTIVE_INSTRUCTIONS,
            tools=[
                get_table_schema,
                query_spark_sql,
                count_nulls_in_column,
                get_delta_history,
            ]
        )
    
    def _build_prompt(self, anomaly: Anomaly) -> str:
        """Build the investigation prompt for the agent."""
        step = anomaly.step
        metrics = anomaly.metrics
        
        prompt = f"""
Investigate the following anomaly in a Databricks ETL pipeline:

**Step Name**: {step.task_key}
**Logic Type**: {step.logic_type}
**Logic Summary**: {step.logic_summary}

**Source Tables**: {', '.join(step.source_tables) or 'Unknown'}
**Target Tables**: {', '.join(step.target_tables) or 'Unknown'}

**Metrics**:
- Input Rows: {metrics.input_count:,}
- Output Rows: {metrics.output_count:,}
- Dropped Rows: {metrics.drop_count:,}
- Drop Rate: {metrics.drop_rate:.1%}

**Anomaly Reason**: {anomaly.reason}
**Historical Average Drop Rate**: {anomaly.historical_avg:.1%}

**Step Code**:
```python
{step.code_content[:3000]}
```

Please investigate why this step dropped more rows than expected and provide your findings.
"""
        return prompt
    
    async def analyze_async(self, anomaly: Anomaly) -> str:
        """
        Analyze an anomaly using the RCA Agent (async version).
        Returns the agent's explanation.
        """
        prompt = self._build_prompt(anomaly)
        
        # Run the agent
        result = await Runner.run(
            self.agent,
            prompt,
        )
        
        return result.final_output
    
    def analyze(self, anomaly: Anomaly) -> str:
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
                future = pool.submit(asyncio.run, self.analyze_async(anomaly))
                return future.result()
        except RuntimeError:
            # No running loop, we can use asyncio.run
            return asyncio.run(self.analyze_async(anomaly))
    
    def analyze_all(self, anomalies: List[Anomaly]) -> List[str]:
        """
        Analyze all anomalies and return a list of reports.
        """
        reports = []
        
        for i, anomaly in enumerate(anomalies):
            logger.info("[RCA Agent] Analyzing anomaly %d/%d: %s...", i+1, len(anomalies), anomaly.step.task_key)
            try:
                report = self.analyze(anomaly)
                reports.append(f"# RCA for Step: {anomaly.step.task_key}\n\n{report}")
            except Exception as e:
                error_report = f"# RCA for Step: {anomaly.step.task_key}\n\nError during analysis: {str(e)}"
                reports.append(error_report)
                logger.error("RCA error for %s: %s", anomaly.step.task_key, e)
        
        return reports
