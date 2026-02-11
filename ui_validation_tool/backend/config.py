import os
import logging
from typing import Optional, Any
from pydantic import BaseModel, ConfigDict
from dotenv import load_dotenv

load_dotenv()

# fixing asyncio logs from litellm
os.environ["LITELLM_LOGGING"] = "False"
os.environ["LITELLM_DISABLE_LOGGING"] = "True"
os.environ["OPENAI_AGENTS_ENABLE_LITELLM_SERIALIZER_PATCH"] = "True"

from agents.extensions.models.litellm_model import LitellmModel

from agents import set_tracing_disabled, ModelSettings
import requests

set_tracing_disabled(True)

def fix_tiktoken_cache():
    """
    Fixes SSL errors in corporate networks by pre-downloading the tiktoken encoding file
    with verification disabled if it doesn't exist.
    """
    try:
        cache_dir = os.path.join(os.getcwd(), "tiktoken_cache")
        os.makedirs(cache_dir, exist_ok=True)
        os.environ["TIKTOKEN_CACHE_DIR"] = cache_dir

        filename = "9b5ad71b2ce5302211f9c61530b329a4922fc6a4"  # Sha1 of cl100k_base
        file_path = os.path.join(cache_dir, filename)

        if not os.path.exists(file_path):
            url = "https://openaipublic.blob.core.windows.net/encodings/cl100k_base.tiktoken"
            print(f"Downloading tiktoken encoding from {url} (SSL verify=False)...")
            response = requests.get(url, verify=False, timeout=30)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(response.content)
                print("Tiktoken encoding downloaded successfully.")
            else:
                print(f"Failed to download tiktoken encoding: {response.status_code}")
    except Exception as e:
        print(f"Warning: Failed to fix tiktoken cache: {e}")

fix_tiktoken_cache()

class UIConfig(BaseModel):
    """Configuration for the UI Validation Tool."""
    model_config = ConfigDict(arbitrary_types_allowed=True)

    databricks_host: str = os.getenv("DATABRICKS_HOST", "")
    databricks_token: str = os.getenv("DATABRICKS_TOKEN", "")
    databricks_cluster_id: str = os.getenv("DATABRICKS_CLUSTER_ID", "")

    # LLM settings
    llm_model: str = os.getenv("LLM_MODEL", "databricks/databricks-gpt-oss-20b")

    model: Optional[Any] = None
    if LitellmModel:
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
        timeout=timeout,
        include_usage=True
    )
    
    # Defaults
    temp_dir: str = os.path.join(os.getcwd(), "temp_ui_cache")
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # Feature Flags / Extras
    manifest_table: str = os.getenv("RCA_MANIFEST_TABLE", "rca_manifest_log")

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
