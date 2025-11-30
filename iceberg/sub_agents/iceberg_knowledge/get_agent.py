from google.adk.agents import Agent
from google.adk.tools import VertexAiSearchTool
from .prompt import ICEBERG_KNOWLEDGE_AGENT_PROMPT
from google.adk.models.google_llm import Gemini
from ...config import get_retry_config, get_fast_model

def get_iceberg_knowledge_agent() -> Agent:
    return Agent(
        name="iceberg_knowledge_agent",
        model=Gemini(model=get_fast_model(), retry_options=get_retry_config()),
        description="An agent that is very knowledgeable about Apache Iceberg in general. It knows about the best practices and can give recommendations if given specific information.",
        instruction=ICEBERG_KNOWLEDGE_AGENT_PROMPT,
        tools=[VertexAiSearchTool(data_store_id="projects/iceberg-agents/locations/global/collections/default_collection/dataStores/iceberg-docs_1764271782550")]
    )
