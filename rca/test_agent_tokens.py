import logging
import asyncio
import os
import sys
from dotenv import load_dotenv

# Ensure we can import from local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 1. Setup Environment & Patching
# These MUST be set before importing LitellmModel if not already handling it globally
os.environ["LITELLM_LOGGING"] = "False"
os.environ["OPENAI_AGENTS_ENABLE_LITELLM_SERIALIZER_PATCH"] = "True"

from agents import Agent, Runner
from agents.extensions.models.litellm_model import LitellmModel
from config.config import get_config

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TokenTest")

async def run_test():
    """
    Runs a simple agent task and prints token usage metrics.
    """
    logger.info("Initializing Config...")
    config = get_config()
    
    # 2. Define Model
    # We use the configuration's model settings
    logger.info(f"Using Model: {config.llm_model}")
    
    # Check if model is properly initialized in config
    if not config.model:
        logger.warning("Config.model is None. Initializing manually for test...")
        config.model = LitellmModel(
            model=config.llm_model,
            api_key=config.databricks_token
        )

    # 3. Define Agent
    agent = Agent(
        name="TokenCounterBot",
        model=config.model,
        instructions="You are a helpful assistant. value brevity.",
        tools=[],  # No tools needed for this test
        model_settings={
            "temperature": config.temperature,
            "max_tokens": config.max_tokens,
            "timeout": config.llm_timeout
        }
    )
    
    prompt = "Hello! Please tell me a very short joke about data engineering."
    logger.info(f"Sending Prompt: '{prompt}'")
    
    # 4. Run Agent
    try:
        result = await Runner.run(agent, prompt)
        
        logger.info("\n" + "="*40)
        logger.info("           TEST RESULTS           ")
        logger.info("="*40)
        logger.info(f"Output: {result.final_output}")
        logger.info("-" * 40)
        
        # 5. Inspect Token Usage
        # The Runner returns a RunResult which should contain usage info if supported by the model/library
        if hasattr(result, "usage"):
            usage = result.usage
            logger.info("TOKEN USAGE METRICS:")
            logger.info(f"  - Prompt Tokens: {usage.prompt_tokens}")
            logger.info(f"  - Completion Tokens: {usage.completion_tokens}")
            logger.info(f"  - Total Tokens: {usage.total_tokens}")
            
            # Additional cost calculation could go here if price per token is known
        else:
            logger.warning("Usage metrics not found in RunResult object.")
            # Debugging: Print available attributes
            logger.info(f"Available attributes on result: {dir(result)}")

    except Exception as e:
        logger.error(f"Agent execution failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run_test())
