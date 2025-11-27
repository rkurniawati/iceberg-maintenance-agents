from google.adk.agents import Agent
from google.adk.models.google_llm import Gemini
from google.adk.tools import google_search
from google.adk.tools import VertexAiSearchTool

from iceberg.config import model_config
from dotenv import load_dotenv
import os

# iceberg_knowledge_agent = Agent(
#     name="iceberg_knowledge_agent",
#     model=model_config.fast_model,
#     description="An agent that is very knowledgeable about Apache Iceberg.",
#     instruction="""
#     You are an Apache Iceberg expert. 
#     Your primary role is to assist users with Apache Iceberg tasks. 
#     Use Google search to find information about Apache Iceberg. 
#     """,
#     tools=[google_search],
#     disallow_transfer_to_parent=True,
# )

load_dotenv()
# cloud_logging_client = google.cloud.logging.Client()
# cloud_logging_client.setup_logging()

# Create your vertexai_search_tool and update its path below
DATASTORE_ID = "projects/iceberg-agents/locations/global/collections/default_collection/dataStores/iceberg-docs_1764271782550"

iceberg_knowledge_agent = Agent(
    name="iceberg_knowledge_agent",
    model="gemini-2.5-flash",
    description="An agent that is very knowledgeable about Apache Iceberg.",
    instruction="""
    You are an Apache Iceberg expert. 
    Your primary role is to assist users with Apache Iceberg tasks. 
    Use your search tool to find information about Apache Iceberg. 
    """,
    tools=[VertexAiSearchTool(data_store_id=DATASTORE_ID)]
)
