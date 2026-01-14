import asyncio
import logging
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from backend.config import setup_logging
from ai_agents.mapping_agent import MappingAgent

# Setup simple logging to console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_verification():
    print("-" * 50)
    print("Verifying Hybrid Resolution (Config + AST + Resolve)")
    print("-" * 50)

    # 1. Mock Code with Variables
    code_content = """
import logging

def run():
    env = "dev" # Local override? No, we trust config first usually, or local definitions overrides config?
    # ASTParser currently prioritizes local assignments if found.
    
    # Test 1: f-string with Config Param
    # 'env' is defined locally above, so resolver should use "dev" (MEDIUM)
    # But let's rely on Job Param 'env' if variable name matches?
    # Actually ASTParser logic: if 'env' is assigned locally, it uses that.
    
    table_name = f"{env}.sales.customers" 
    df = spark.read.table(table_name)
    
    # Test 2: Direct Config Key usage (if we could detect it, but here we simulate via param)
    # Let's say we have a global 'schema' param
    
    target = f"prod.{schema}.metrics"
    df.write.saveAsTable(target)
    
    # Test 3: ADLS Path
    path = f"abfss://{storage_account}@dfs.core.windows.net/raw/"
    spark.read.parquet(path)
"""

    # 2. Mock Metadata with Parameters
    # Global 'env' is 'prod', 'schema' is 'marketing', 'storage_account' is 'datalake'
    metadata = "Task: test_task\nParameters: {'env': 'prod', 'schema': 'marketing', 'storage_account': 'datalake'}"
    
    code_context = {
        "test_script.py": code_content,
        "__metadata__": metadata
    }

    # 3. Run Agent
    agent = MappingAgent()
    result = await agent.analyze_code_async(code_context)

    # 4. Print Results
    print("\nAnalysis Complete. Found Assets:")
    for asset in result.assets:
        print(f"[{asset.confidence}] {asset.usage} {asset.asset_type}: {asset.identifier}")
        print(f"   Evidence: {asset.evidence}")

    # Validate Expectations
    # NOTE: In my ASTParser implementation, I track local assignments.
    # So 'env' = "dev" in code should override 'env'="prod" in config? 
    # Let's see what happens.
    
    # We expect:
    # 1. dev.sales.customers (from local 'dev') -> MEDIUM confidence
    # 2. prod.marketing.metrics (from config 'schema') -> HIGH confidence
    # 3. abfss://datalake@dfs... (from config 'storage_account') -> HIGH confidence

if __name__ == "__main__":
    asyncio.run(run_verification())
