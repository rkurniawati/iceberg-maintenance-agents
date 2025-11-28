from google.adk.agents import Agent
from google.adk.models.google_llm import Gemini
from iceberg.tools import get_tables, get_table_schema, get_data_file_info
from iceberg.config import get_retry_config, get_fast_model

iceberg_database_info_agent = Agent(
    name="iceberg_datalake_info",
    model=Gemini(model=get_fast_model(), retry_options=get_retry_config()),
    description="An agent that can answer questions about the user's Apache Iceberg datalake. It knows the tables, the data file information, partition information, etc",
    instruction="You are a helpful assistant that can answer questions about the user's datalake.",
    tools=[get_tables,get_table_schema,get_data_file_info],
    disallow_transfer_to_parent=True,
)
