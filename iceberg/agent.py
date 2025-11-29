from google.adk.agents import Agent
from google.adk.models import Gemini

from .sub_agents.iceberg_database_info import get_iceberg_database_info_agent
from .sub_agents.iceberg_knowledge import get_iceberg_knowledge_agent
from .sub_agents.iceberg_maintenance import get_iceberg_maintenance
from google.adk.tools.agent_tool import AgentTool
from .config import get_fast_model, get_retry_config
from .prompt import ICEBERG_AGENT_PROMPT


root_agent = Agent(
    name="root_agent",
    model=Gemini(model=get_fast_model(), retry_options=get_retry_config()),
    description="A multi agent system for Apache Iceberg",
    instruction=ICEBERG_AGENT_PROMPT,
    sub_agents=[get_iceberg_database_info_agent(), get_iceberg_maintenance()],
    tools=[AgentTool(agent=get_iceberg_knowledge_agent())],
)



