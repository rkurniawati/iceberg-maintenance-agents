from google.adk.agents import Agent
from .prompt import TRINO_EXECUTOR_PROMPT
from google.adk.models.google_llm import Gemini
from ...config import get_retry_config, get_fast_model
from .tools import run_compaction, run_expire_snapshots, run_optimize_manifests, run_remove_orphan_files

def get_iceberg_maintenance() -> Agent:
    return Agent(
        name="iceberg_maintenance_agent",
        model=Gemini(model=get_fast_model(), retry_options=get_retry_config()),
        description="An agent that can performs Apache Iceberg maintenance operations such as compaction, optimize manifests, removal of orphan files, and snapshot expirations. It doesn't have information about the user's datalake.",
        instruction=TRINO_EXECUTOR_PROMPT,
        disallow_transfer_to_parent=True,
        tools=[run_compaction, run_expire_snapshots, run_optimize_manifests, run_remove_orphan_files]
    )
