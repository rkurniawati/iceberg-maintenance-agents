from google.adk.agents import Agent
from google.adk.models.google_llm import Gemini
from iceberg.tools import get_tables, get_table_schema
from iceberg.config import model_config

iceberg_database_info_agent = Agent(
    name="database_assistant",
    model=model_config.fast_model,
    description="A simple agent that can answer questions about the user's database.",
    instruction="You are a helpful assistant that can answer questions about the user's database.",
    tools=[get_tables,get_table_schema],
    disallow_transfer_to_parent=True,
)
