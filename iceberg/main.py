import logging
from google.adk.sessions.sqlite_session_service import SqliteSessionService
from google.genai import types
import asyncio
import sys

from google.adk import Runner
from .agent import root_agent
from .config import get_fast_model
from .prometheus import PrometheusPlugin

logging.basicConfig(
    filename='iceberg-maintenance-agent.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)

async def run_session(
        runner_instance: Runner,
        user_id: str,
        session_name: str = "default",
):
    logging.debug(f"\n ### Session: {session_name}")

    # Get app name from the Runner
    app_name = runner_instance.app_name

    # Attempt to create a new session or retrieve an existing one
    try:
        session = await session_service.create_session(
            app_name=app_name, user_id=user_id, session_id=session_name
        )
    except:
        session = await session_service.get_session(
            app_name=app_name, user_id=user_id, session_id=session_name
        )

    # Process queries if provided
    if not session:
        logging.error("Session could not be created or retrieved.")
        return

    while True:
        try:
            query_text = input("User > ")
            if query_text.lower() in ["exit", "quit"]:
                break

            query = types.Content(role="user", parts=[types.Part(text=query_text)])

            async for event in runner_instance.run_async(
                    user_id=user_id, session_id=session.id, new_message=query
            ):
                if event.content and event.content.parts:
                    response_text = event.content.parts[0].text
                    if response_text and response_text != "None":
                        sys.stdout.write(f"{get_fast_model()} > {response_text}\n")
                        sys.stdout.flush()
        except EOFError:
            break
        except Exception as e:
            logging.error(f"An error occurred during session run: {e}")
            break

if __name__ == "__main__":
    session_service = SqliteSessionService(db_path="iceberg_agent_main.db")
    prometheus_plugin = PrometheusPlugin("http://localhost:9099")

    runner = Runner(
        agent=root_agent,
        app_name="Iceberg Multi-agent Maintainer",
        session_service=session_service,
        plugins=[prometheus_plugin]
    )
    asyncio.run(run_session(runner, "test123", "stateful-agentic-session"))

