from google.adk.agents import Agent
from google.adk.models.google_llm import Gemini
from google.adk.tools import google_search
from ..config import model_config

iceberg_knowledge_agent = Agent(
    name="iceberg_knowledge_agent",
    model=model_config.fast_model,
    description="An agent that is very knowledgeable about Apache Iceberg.",
    instruction="""
    You are an Apache Iceberg expert. 
    Your primary role is to assist users with Apache Iceberg tasks. 
    Use Google search to find information about Apache Iceberg. 
    """,
    tools=[google_search],
    disallow_transfer_to_parent=True,
)
