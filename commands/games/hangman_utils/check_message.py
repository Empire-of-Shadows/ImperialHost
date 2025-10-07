# Python
from typing import Optional

import discord

from utilities.logger_setup import get_logger

logger = get_logger("CheckMessage")


async def check_message_exists(channel: Optional[discord.abc.Messageable], message_id: Optional[int]) -> bool:
    """
    Return True if a message with the given ID exists and is accessible in the provided channel.

    Defensive improvements:
    - Handles None channel/message_id gracefully.
    - Logs with context and avoids raising on permission/network issues.
    """
    if channel is None:
        logger.debug("check_message_exists: channel is None")
        return False

    if not message_id:
        logger.debug("check_message_exists: message_id is missing/invalid")
        return False

    # Some channel types (e.g., DMs) still support fetch_message; rely on runtime capability
    try:
        await channel.fetch_message(message_id)  # type: ignore[attr-defined]
        return True
    except discord.NotFound:
        # Message does not exist or was deleted
        return False
    except discord.Forbidden:
        logger.warning(f"Insufficient permissions to fetch message_id={message_id} in channel={getattr(channel, 'id', 'unknown')}")
        return False
    except discord.HTTPException as e:
        logger.error(f"HTTP error fetching message_id={message_id} in channel={getattr(channel, 'id', 'unknown')}: {e}")
        return False
    except AttributeError:
        # Channel type doesn't implement fetch_message
        logger.warning(f"Channel type {type(channel).__name__} does not support fetch_message")
        return False