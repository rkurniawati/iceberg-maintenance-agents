from google.adk.agents import Agent
from .prompt import TRINO_EXECUTOR_PROMPT
from google.adk.models.google_llm import Gemini
from iceberg.config import get_retry_config, get_fast_model
from .tools import run_compaction, run_expire_snapshots, run_optimize_manifests, run_remove_orphan_files

iceberg_maintenance = Agent(
    name="iceberg_maintenance_agent",
    model=Gemini(model=get_fast_model(), retry_options=get_retry_config()),
    description="An agent that accepts Apache Iceberg maintenance requests such as compaction, optimize manifests, removal of orphan files, and snapshot expirations, and executes them.",
    instruction=TRINO_EXECUTOR_PROMPT,
    disallow_transfer_to_parent=True,
    tools=[run_compaction, run_expire_snapshots, run_optimize_manifests, run_remove_orphan_files]
)
