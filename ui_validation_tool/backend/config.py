import os
import logging
from typing import Optional
from pydantic import BaseModel, ConfigDict
from dotenv import load_dotenv

load_dotenv()

# fixing asyncio logs from litellm
os.environ["LITELLM_LOGGING"] = "False"
os.environ["LITELLM_DISABLE_LOGGING"] = "True"

from agents.extensions.models.litellm_model import LitellmModel
from agents import set_tracing_disabled, ModelSettings

set_tracing_disabled(True)

class UIConfig(BaseModel):
    """Configuration for the UI Validation Tool."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    databricks_host: str = os.getenv("DATABRICKS_HOST", "")
    databricks_token: str = os.getenv("DATABRICKS_TOKEN", "")

    # LLM settings
    llm_model: str = os.getenv("LLM_MODEL", "databricks/databricks-gpt-oss-20b")

    model: LitellmModel = LitellmModel(
        model=llm_model,
        api_key=databricks_token
    )

    # Model Settings
    temperature: float = float(os.getenv("TEMPERATURE", "0.1"))
    max_tokens: int = int(os.getenv("MAX_TOKENS", "20000"))
    timeout: int = int(os.getenv("LLM_TIMEOUT", "120"))

    model_settings: ModelSettings = ModelSettings(
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout
    )
    
    # Defaults
    temp_dir: str = os.path.join(os.getcwd(), "temp_ui_cache")
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

    def validate(self):
        errors = []
        if not self.databricks_host: errors.append("DATABRICKS_HOST is missing.")
        if not self.databricks_token: errors.append("DATABRICKS_TOKEN is missing.")
        return errors

def get_config() -> UIConfig:
    return UIConfig()

def setup_logging(level: str = "INFO"):
    """Configures logging for the application."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
