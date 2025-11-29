from google.adk.sessions import InMemorySessionService
from google.adk.sessions.sqlite_session_service import SqliteSessionService
from google.genai import types
import asyncio

from google.adk import Runner
from .agent import root_agent
from .config import get_fast_model

async def run_session(
        runner_instance: Runner,
        user_id: str,
        user_queries: list[str] | str | None = None,
        session_name: str = "default",
):
    print(f"\n ### Session: {session_name}")

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
    if session and user_queries:
        # Convert single query to list for uniform processing
        if type(user_queries) == str:
            user_queries = [user_queries]

        # Process each query in the list sequentially
        for query in user_queries:
            print(f"\nUser > {query}")

            # Convert the query string to the ADK Content format
            query = types.Content(role="user", parts=[types.Part(text=query)])

            # Stream the agent's response asynchronously
            async for event in runner_instance.run_async(
                    user_id=user_id, session_id=session.id, new_message=query
            ):
                # Check if the event contains valid content
                if event.content and event.content.parts:
                    # Filter out empty or "None" responses before printing
                    if (
                            event.content.parts[0].text != "None"
                            and event.content.parts[0].text
                    ):
                        print(f"{get_fast_model()} > ", event.content.parts[0].text)
    elif not session:
        print("Session could not be created or retrieved.")
    else:
        print("No queries!")

if __name__ == "__main__":
    session_service = SqliteSessionService(db_path="iceberg_agent_main.db")
    runner = Runner(
        agent=root_agent,
        app_name="Iceberg Multi-agent Maintainer",
        session_service=session_service,
    )
    asyncio.run(
        run_session(
            runner,
            "bob123",
            [
                "Do I have any preferences set?",
                "Please use schema dogs in the datalake",
                "How many tables are there in the datalake?",
            ],
            "stateful-agentic-session",
        )
    )

