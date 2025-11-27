from google.adk.agents import Agent
from google.adk.runners import InMemoryRunner
from google.adk.tools import google_search
from .config import model_config
from .sub_agents.iceberg_database_info_agent import iceberg_database_info_agent
from .sub_agents.iceberg_knowledge_agent import iceberg_knowledge_agent
from google.adk.models.google_llm import Gemini
from google.adk.tools.agent_tool import AgentTool

root_agent = Agent(
    name="iceberg_agent",
    model=Gemini(
        model="gemini-2.5-flash-lite"),
    description="A multi agent system for Apache Iceberg",
    instruction="You are a coordinator of multiple agents that help user to maintain and optimize their Apache Iceberg database.",
    sub_agents=[iceberg_database_info_agent],
    tools=[AgentTool(agent=iceberg_knowledge_agent)],
)
