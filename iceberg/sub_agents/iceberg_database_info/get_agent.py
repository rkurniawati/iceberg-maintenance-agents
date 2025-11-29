from google.adk.agents import Agent
from google.adk.models.google_llm import Gemini
from .tools import get_tables, get_table_schema, get_all_current_table_files, get_manifest_files, find_orphan_files, get_snapshots
from ...config import get_retry_config, get_fast_model

def get_iceberg_database_info_agent() -> Agent:
    return Agent(
        name="iceberg_datalake_info",
        model=Gemini(model=get_fast_model(), retry_options=get_retry_config()),
        description="An agent that can answer questions about the user's Apache Iceberg datalake. It knows the tables, the data file information, partition information, etc",
        instruction="You are a helpful assistant that can answer questions about the user's datalake.",
        tools=[get_tables, get_table_schema, get_all_current_table_files, find_orphan_files, get_manifest_files, get_snapshots],
        disallow_transfer_to_parent=True,
    )
