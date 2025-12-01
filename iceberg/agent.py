from typing import Union

from google.adk.agents import Agent
from google.adk.models import Gemini
from google.adk.tools import VertexAiSearchTool

from .sub_agents.iceberg_database_info import get_iceberg_database_info_agent
from .sub_agents.iceberg_maintenance import get_iceberg_maintenance
from google.adk.tools.agent_tool import AgentTool
from .config import get_fast_model, get_retry_config, iceberg_config
from .prompt import ICEBERG_AGENT_PROMPT
from dotenv import load_dotenv
from .tools import recall_user_preferences, save_user_preferences

load_dotenv()

iceberg_knowledge_agent = Agent(
    name="iceberg_knowledge_agent",
    model=Gemini(model="gemini-2.5-flash"),
    description="""
        An agent that is very knowledgeable about Apache Iceberg in general. 
        It knows about the general best practices and can give generic information about Apache Iceberg maintenance.
        """,
    instruction="""
        You are an Apache Iceberg best practice expert.  
        Your primary role is to assist users with Apache Iceberg tasks. 
        """,
    tools=[VertexAiSearchTool(data_store_id="projects/iceberg-agents/locations/global/collections/default_collection/dataStores/iceberg-docs_1764271782550")]
)

root_agent = Agent(
    name="root_agent",
    model=Gemini(model=get_fast_model(), retry_options=get_retry_config()),
    description="A multi agent system for Apache Iceberg",
    instruction=ICEBERG_AGENT_PROMPT,
    sub_agents=[get_iceberg_database_info_agent(), get_iceberg_maintenance()],
    tools=[AgentTool(agent=iceberg_knowledge_agent),recall_user_preferences,save_user_preferences,
],
)


