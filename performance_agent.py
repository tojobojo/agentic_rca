"""
Performance Agent Module.
Implements the PerformanceAnalyzer agent using OpenAI Agents SDK.
Debugs slow steps, detects data skew, shuffle issues, and spills.
"""
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from agents import Agent, Runner, function_tool

from config import get_config


# --- Tool Definitions ---

@function_tool
def get_query_plan(table_name: str) -> str:
    """
    Get the physical query plan for reading a table.
    Use this to understand how Spark will execute the query.
    
    Args:
        table_name: Fully qualified table name
    
    Returns:
        Physical plan as string showing operations like Scan, Filter, Join, etc.
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        df = spark.table(table_name)
        plan = df._jdf.queryExecution().explainString("extended")
        return plan
    except Exception as e:
        return f"Error getting query plan: {str(e)}"


@function_tool
def get_stage_metrics(run_id: str) -> str:
    """
    Get stage-level metrics from a Spark job run.
    Shows shuffle bytes, spill, task duration, and record counts.
    
    Args:
        run_id: Databricks run ID or Spark application ID
    
    Returns:
        Formatted metrics showing performance characteristics
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        # Get Spark UI metrics via REST API
        sc = spark.sparkContext
        status_tracker = sc.statusTracker()
        
        # Get active jobs and stages
        job_ids = status_tracker.getJobIdsForGroup()
        
        results = []
        for job_id in job_ids[-5:]:  # Last 5 jobs
            job_info = status_tracker.getJobInfo(job_id)
            if job_info:
                results.append(f"Job {job_id}: {job_info.status()}")
                for stage_id in job_info.stageIds()[:5]:
                    stage_info = status_tracker.getStageInfo(stage_id)
                    if stage_info:
                        results.append(
                            f"  Stage {stage_id}: Tasks={stage_info.numTasks()}, "
                            f"Active={stage_info.numActiveTasks()}, "
                            f"Completed={stage_info.numCompletedTasks()}"
                        )
        
        if not results:
            return "No active job metrics available. Check Spark UI for historical runs."
        
        return "\n".join(results)
    except Exception as e:
        return f"Error getting stage metrics: {str(e)}"


@function_tool
def detect_data_skew(table_name: str, column_name: str) -> str:
    """
    Detect data skew in a column by analyzing value distribution.
    Skew causes uneven task distribution and slow stages.
    
    Args:
        table_name: Fully qualified table name
        column_name: Column to analyze for skew
    
    Returns:
        Distribution analysis showing top values and their frequencies
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, count, desc
        
        spark = SparkSession.builder.getOrCreate()
        df = spark.table(table_name)
        
        total_rows = df.count()
        
        # Get top 10 values by frequency
        distribution = (
            df.groupBy(column_name)
            .agg(count("*").alias("cnt"))
            .orderBy(desc("cnt"))
            .limit(10)
            .collect()
        )
        
        output = f"Table: {table_name}\nColumn: {column_name}\nTotal Rows: {total_rows:,}\n\n"
        output += "Top 10 Values by Frequency:\n"
        output += "-" * 40 + "\n"
        
        for row in distribution:
            value = row[column_name]
            cnt = row["cnt"]
            pct = (cnt / total_rows) * 100
            skew_indicator = " ⚠️ SKEWED" if pct > 20 else ""
            output += f"  {value}: {cnt:,} ({pct:.1f}%){skew_indicator}\n"
        
        # Skew detection
        if distribution:
            top_pct = (distribution[0]["cnt"] / total_rows) * 100
            if top_pct > 30:
                output += f"\n⚠️ WARNING: Single value contains {top_pct:.1f}% of data. Consider salting or repartitioning."
        
        return output
    except Exception as e:
        return f"Error detecting skew: {str(e)}"


@function_tool
def analyze_shuffle(query: str) -> str:
    """
    Analyze a query for potential shuffle operations.
    Shuffles are expensive; this helps identify optimization opportunities.
    
    Args:
        query: SQL query to analyze
    
    Returns:
        Analysis of shuffle-inducing operations
    """
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        
        # Get the plan
        df = spark.sql(f"EXPLAIN EXTENDED {query}")
        plan_rows = df.collect()
        plan_text = "\n".join([row[0] for row in plan_rows])
        
        # Analyze for shuffle indicators
        shuffle_ops = []
        if "Exchange" in plan_text:
            shuffle_ops.append("Exchange (Shuffle)")
        if "SortMergeJoin" in plan_text:
            shuffle_ops.append("SortMergeJoin (requires shuffle on both sides)")
        if "ShuffledHashJoin" in plan_text:
            shuffle_ops.append("ShuffledHashJoin (shuffles one side)")
        if "BroadcastHashJoin" in plan_text:
            shuffle_ops.append("BroadcastHashJoin (no shuffle - optimal)")
        if "Sort" in plan_text and "Exchange" in plan_text:
            shuffle_ops.append("Sort with Exchange (full shuffle + sort)")
        
        output = "Shuffle Analysis:\n"
        output += "-" * 40 + "\n"
        
        if shuffle_ops:
            for op in shuffle_ops:
                output += f"  • {op}\n"
            output += "\nRecommendations:\n"
            if "SortMergeJoin" in str(shuffle_ops):
                output += "  - Consider broadcast hint for smaller table: /*+ BROADCAST(small_table) */\n"
            if "Sort with Exchange" in str(shuffle_ops):
                output += "  - Pre-partition data on join keys to avoid shuffle\n"
        else:
            output += "  No significant shuffle operations detected.\n"
        
        return output
    except Exception as e:
        return f"Error analyzing shuffle: {str(e)}"


# --- Agent Definition ---

PERFORMANCE_ANALYZER_INSTRUCTIONS = """
You are an expert Spark Performance Engineer analyzing slow or inefficient Databricks ETL jobs.

Your Task:
1. Analyze query plans to identify bottlenecks.
2. Detect data skew that causes uneven task distribution.
3. Identify expensive shuffle operations.
4. Recommend optimizations.

Guidelines:
- Focus on the largest stages (by data volume or duration).
- Look for skew: one partition with disproportionate data.
- Check for unnecessary shuffles that could be avoided with broadcast joins.
- Recommend partitioning strategies when appropriate.

Output Format:
## Performance Summary
[Brief summary of main issues]

## Bottlenecks Identified
[Specific operations causing slowness]

## Recommendations
[Actionable optimization suggestions]
"""


@dataclass
class PerformanceContext:
    """Context for performance analysis."""
    step_name: str
    source_tables: List[str]
    target_tables: List[str]
    code_content: str
    execution_time_seconds: float = 0


class PerformanceAgent:
    """
    The Performance Analyzer: Debugs slow steps and identifies optimizations.
    """
    
    def __init__(self):
        self.config = get_config()
        
        self.agent = Agent(
            name="PerformanceAnalyzer",
            model=self.config.openai_model,
            instructions=PERFORMANCE_ANALYZER_INSTRUCTIONS,
            tools=[
                get_query_plan,
                get_stage_metrics,
                detect_data_skew,
                analyze_shuffle,
            ]
        )
    
    def _build_prompt(self, context: PerformanceContext) -> str:
        """Build the analysis prompt."""
        prompt = f"""
Analyze the performance of this Databricks ETL step:

**Step Name**: {context.step_name}
**Execution Time**: {context.execution_time_seconds:.1f} seconds

**Source Tables**: {', '.join(context.source_tables) or 'Unknown'}
**Target Tables**: {', '.join(context.target_tables) or 'Unknown'}

**Step Code**:
```python
{context.code_content[:3000]}
```

Please analyze for:
1. Data skew on join/group keys
2. Expensive shuffle operations
3. Query plan inefficiencies
4. Optimization opportunities
"""
        return prompt
    
    async def analyze_async(self, context: PerformanceContext) -> str:
        """Analyze performance (async)."""
        prompt = self._build_prompt(context)
        result = await Runner.run(self.agent, prompt)
        return result.final_output
    
    def analyze(self, context: PerformanceContext) -> str:
        """Analyze performance (sync wrapper)."""
        import asyncio
        
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, self.analyze_async(context))
                return future.result()
        except RuntimeError:
            return asyncio.run(self.analyze_async(context))
