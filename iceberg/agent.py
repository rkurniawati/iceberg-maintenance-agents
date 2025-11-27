from google.adk.agents import Agent
from .sub_agents.iceberg_database_info import iceberg_database_info_agent
from .sub_agents.iceberg_knowledge import iceberg_knowledge_agent
from google.adk.models.google_llm import Gemini
from google.adk.tools.agent_tool import AgentTool

DATASTORE_ID = "projects/iceberg-agents/locations/global/collections/default_collection/dataStores/iceberg-docs_1764271782550"

root_agent = Agent(
    name="root_agent",
    model=Gemini(
        model="gemini-2.5-flash"),
    description="A multi agent system for Apache Iceberg",
    instruction="You are a coordinator of multiple agents that maintain and optimize the user's Apache Iceberg data lakehouse. For any questions about Apache Iceberg that are not specific to the data lakehouse, use your search tool to find information about Apache Iceberg.",
    sub_agents=[iceberg_database_info_agent],
    tools=[AgentTool(agent=iceberg_knowledge_agent)],
)
