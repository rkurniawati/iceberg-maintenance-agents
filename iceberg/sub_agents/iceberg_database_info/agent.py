from google.adk.agents import Agent
from google.adk.models.google_llm import Gemini
from iceberg.tools import get_tables, get_table_schema
from iceberg.config import get_retry_config, get_fast_model

iceberg_database_info_agent = Agent(
    name="database_assistant",
    model=Gemini(model=get_fast_model(), retry_options=get_retry_config()),
    description="A simple agent that can answer questions about the user's Apache Iceberg datalake.",
    instruction="You are a helpful assistant that can answer questions about the user's datalake.",
    tools=[get_tables,get_table_schema],
    disallow_transfer_to_parent=True,
)
