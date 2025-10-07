# Python
import discord
from typing import Iterable, Dict, Any, Optional, Union

from utilities.logger_setup import get_logger

logger = get_logger("GamePermissions")


def _resolve_member(obj: Any) -> Optional[Union[discord.Member, discord.User]]:
    """
    Best-effort extraction of a discord.Member/User from various player shapes.
    Supports objects with `.user` (custom Player types) or direct Member/User instances.
    """
    if isinstance(obj, (discord.Member, discord.User)):
        return obj
    candidate = getattr(obj, "user", None)
    if isinstance(candidate, (discord.Member, discord.User)):
        return candidate
    return None


def setup_game_permissions(
    guild: discord.Guild,
    creator: Union[discord.Member, discord.User],
    players: Optional[Iterable[Any]] = None,
    is_voice: bool = False,
) -> Dict[Union[discord.Role, discord.Member, discord.User], discord.PermissionOverwrite]:
    """
    Build channel permission overwrites for a game.

    - Text channels: Only players + creator can send messages (others read-only).
    - Voice channels: Only players + creator can join/speak (others view-only).

    Args:
        guild: The guild for which to set permissions.
        creator: The creator of the game (Member/User).
        players: An iterable of players; entries can be Member/User or objects with a `.user` attribute.
        is_voice: Whether the target is a voice-like channel (connect/speak) vs text/thread (send).

    Returns:
        A dict suitable for Channel.create_* or edit with permission_overwrites.
    """
    try:
        gid = getattr(guild, "id", "unknown")
        gname = getattr(guild, "name", "unknown")
        creator_id = getattr(creator, "id", "unknown")
        players_count = sum(1 for _ in players) if players is not None else 0

        logger.info(
            "Building game permissions (guild=%s:%s, creator=%s, is_voice=%s, players=%d)",
            gid, gname, creator_id, is_voice, players_count
        )

        everyone = guild.default_role
        overwrites: Dict[Union[discord.Role, discord.Member, discord.User], discord.PermissionOverwrite] = {}

        if is_voice:
            overwrites = {
                everyone: discord.PermissionOverwrite(connect=False, view_channel=True),
                creator: discord.PermissionOverwrite(connect=True, speak=True, view_channel=True),
                guild.me: discord.PermissionOverwrite(connect=True, speak=True, view_channel=True, manage_channels=True),
            }
            logger.debug("Base voice overwrites initialized.")
        else:
            overwrites = {
                everyone: discord.PermissionOverwrite(view_channel=True, send_messages=False),
                creator: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True),
            }
            logger.debug("Base text/thread overwrites initialized.")

        added_players = 0
        skipped_players = 0

        if players:
            for idx, raw_player in enumerate(players, start=1):
                member_or_user = _resolve_member(raw_player)
                if not member_or_user:
                    skipped_players += 1
                    logger.warning(
                        "Skipping player #%d: cannot resolve to Member/User (type=%s, value=%r)",
                        idx, type(raw_player).__name__, raw_player
                    )
                    continue

                if is_voice:
                    overwrites[member_or_user] = discord.PermissionOverwrite(
                        view_channel=True,
                        connect=True,
                        speak=True,
                    )
                    logger.debug(
                        "Voice perms set for player #%d (id=%s): view_channel=True, connect=True, speak=True",
                        idx, getattr(member_or_user, "id", "unknown")
                    )
                else:
                    overwrites[member_or_user] = discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                    )
                    logger.debug(
                        "Text perms set for player #%d (id=%s): view_channel=True, send_messages=True",
                        idx, getattr(member_or_user, "id", "unknown")
                    )
                added_players += 1

        logger.info(
            "Permissions built: base_entries=%d, players_added=%d, players_skipped=%d, total_overwrites=%d",
            3, added_players, skipped_players, len(overwrites)
        )
        return overwrites

    except Exception as e:
        logger.error("Failed to build game permissions.", exc_info=e)
        # Fall back to a minimal safe overwrite that at least restricts non-players
        try:
            fallback = {
                guild.default_role: discord.PermissionOverwrite(view_channel=True, send_messages=False),
                creator: discord.PermissionOverwrite(view_channel=True, send_messages=True),
                guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True),
            } if not is_voice else {
                guild.default_role: discord.PermissionOverwrite(connect=False, view_channel=True),
                creator: discord.PermissionOverwrite(connect=True, speak=True, view_channel=True),
                guild.me: discord.PermissionOverwrite(connect=True, speak=True, view_channel=True, manage_channels=True),
            }
            logger.warning("Returning fallback overwrites due to error. is_voice=%s", is_voice)
            return fallback
        except Exception as inner:
            logger.error("Failed to build fallback overwrites.", exc_info=inner)
            # As a last resort, return empty dict; caller should handle missing overwrites.
            return {}