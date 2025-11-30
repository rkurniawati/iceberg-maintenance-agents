from typing import Union

from google.adk.tools import ToolContext
from .config import iceberg_config

# adapted from adk_tutorial
def save_user_preferences(tool_context: ToolContext, new_preferences: dict[str, Union[str, int]]) -> str:
    """
    Saves or updates user preferences in the persistent session storage.
    It merges new preferences with any existing ones.

    Args:
        new_preferences: A dictionary of new preferences to save.
                         Example:
                         {"schema": "dogs",
                          "snapshot_retention_threshold_days": 2,
                          "orphan_retention_threshold_days": 3,
                          "file_size_in_bytes_for_compaction": 512000000}
    """

    # get the current preference from context
    current_preferences = tool_context.state.get('user_preferences') or {}
    current_preferences.update(new_preferences)

    # update the context
    if current_preferences and "schema" in current_preferences:
        iceberg_config.schema=current_preferences.get('schema')

    tool_context.state['user_preferences'] = current_preferences

    return f"Preferences updated successfully: {new_preferences}"

def recall_user_preferences(tool_context: ToolContext) -> dict[str, Union[str, int]]:
    """Recalls all saved preferences for the current user from the session."""
    preferences = tool_context.state.get('user_preferences')

    if preferences and "schema" in preferences:
        iceberg_config.schema=preferences.get('schema')

    if preferences:
        return preferences
    else:
        return {"message": "No preferences found."}
