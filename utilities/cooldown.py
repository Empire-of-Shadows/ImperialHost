# Python
import time
from typing import Iterable, Optional, Tuple

import discord
from discord import Interaction
from discord.ext import commands
from discord.ext.commands import CooldownMapping, BucketType, CommandOnCooldown
from utilities.logger_setup import get_logger

logger = get_logger(__name__)

# Predefined cooldowns (kept for compatibility)
user_cooldown = commands.CooldownMapping.from_cooldown(1, 60, commands.BucketType.user)
guild_cooldown = commands.CooldownMapping.from_cooldown(1, 60, commands.BucketType.guild)
leaderboard_cd = commands.CooldownMapping.from_cooldown(1, 30, BucketType.user)
rival_cd = commands.CooldownMapping.from_cooldown(1, 20, BucketType.user)
tictactoe_cd = commands.CooldownMapping.from_cooldown(1, 15, BucketType.user)
tictactoe_guild_cd = commands.CooldownMapping.from_cooldown(5, 20, BucketType.guild)
hangman_start_cd = commands.CooldownMapping.from_cooldown(1, 20, BucketType.user)
hangman_start_guild_cd = commands.CooldownMapping.from_cooldown(5, 30, BucketType.guild)
guess_cd = commands.CooldownMapping.from_cooldown(1, 1, BucketType.user)
guess_guild_cd = commands.CooldownMapping.from_cooldown(20, 5, BucketType.guild)
uno_new_cd_user = commands.CooldownMapping.from_cooldown(1, 45, BucketType.user)
uno_new_cd_guild = commands.CooldownMapping.from_cooldown(2, 60, BucketType.guild)
uno_start_cd = commands.CooldownMapping.from_cooldown(1, 20, BucketType.user)
uno_play_cd = commands.CooldownMapping.from_cooldown(1, 2, BucketType.user)
uno_draw_cd = commands.CooldownMapping.from_cooldown(1, 1.5, BucketType.user)
uno_pass_cd = commands.CooldownMapping.from_cooldown(1, 1.5, BucketType.user)
uno_call_cd = commands.CooldownMapping.from_cooldown(1, 5, BucketType.user)
uno_declare_cd = commands.CooldownMapping.from_cooldown(1, 3, BucketType.user)
uno_quit_cd = commands.CooldownMapping.from_cooldown(1, 10, BucketType.user)
uno_cancel_cd = commands.CooldownMapping.from_cooldown(1, 30, BucketType.user)
create_cd = commands.CooldownMapping.from_cooldown(1, 300, BucketType.user)
edit_cd = commands.CooldownMapping.from_cooldown(1, 60, BucketType.user)
clone_cd = commands.CooldownMapping.from_cooldown(1, 10, BucketType.user)
welcome_cd = commands.CooldownMapping.from_cooldown(1, 10, BucketType.user)


def _fake_ctx_from_interaction(inter: Interaction):
    # Create a minimal message-like object required by CooldownMapping buckets
    return type(
        "FakeContext",
        (),
        {
            "author": inter.user,
            "guild": inter.guild,
            "channel": inter.channel,
        },
    )()


def _round_retry_after(seconds: float) -> float:
    # Round up a bit to avoid users hitting a boundary too early
    return max(0.0, round(seconds + 0.05, 2))


async def _respond_cooldown(inter: Interaction, content: str, ephemeral: bool = True) -> None:
    try:
        if inter.response.is_done():
            await inter.followup.send(content, ephemeral=ephemeral)
        else:
            await inter.response.send_message(content, ephemeral=ephemeral)
    except Exception as e:
        # Log but don't re-raise here; we'll still raise CommandOnCooldown below
        logger.warning(
            "Cooldown response failed",
            extra={
                "error": str(e),
                "user_id": getattr(inter.user, "id", None),
                "guild_id": getattr(inter.guild, "id", None) if inter.guild else None,
                "channel_id": getattr(inter.channel, "id", None) if inter.channel else None,
            },
        )


def cooldown_enforcer(cooldown_map: CooldownMapping, bucket_type: BucketType, *, label: Optional[str] = None):
    """
    Backward-compatible single-cooldown check.
    Use make_cooldown_check for multi-cooldown enforcement.
    """
    return make_cooldown_check([((label or "cooldown"), cooldown_map, bucket_type)])


def make_cooldown_check(
    cooldowns: Iterable[Tuple[str, CooldownMapping, BucketType]],
    *,
    notify_ephemeral: bool = True,
):
    """
    Create an app_commands check that enforces one or more cooldowns.

    cooldowns: Iterable of (label, CooldownMapping, BucketType)
    notify_ephemeral: whether cooldown messages should be ephemeral
    """
    def wrapper():
        async def check(interaction: Interaction):
            fake_ctx = _fake_ctx_from_interaction(interaction)
            command_name = None
            try:
                # If slash/app command, attempt a readable name
                if isinstance(interaction.command, discord.app_commands.Command):
                    command_name = interaction.command.qualified_name
            except Exception:
                pass

            now = time.time()
            for label, mapping, btype in cooldowns:
                bucket = mapping.get_bucket(fake_ctx)
                retry_after = bucket.update_rate_limit(now)

                if retry_after:
                    retry_after = _round_retry_after(retry_after)

                    # Log with structured context
                    logger.info(
                        "Command on cooldown",
                        extra={
                            "event": "cooldown_hit",
                            "label": label,
                            "retry_after": retry_after,
                            "bucket_type": str(btype),
                            "command": command_name,
                            "user_id": getattr(interaction.user, "id", None),
                            "guild_id": getattr(interaction.guild, "id", None) if interaction.guild else None,
                            "channel_id": getattr(interaction.channel, "id", None) if interaction.channel else None,
                        },
                    )

                    # User-facing message
                    await _respond_cooldown(
                        interaction,
                        f"You are on {label} cooldown. Try again in {retry_after:.2f} seconds.",
                        ephemeral=notify_ephemeral,
                    )

                    # Raise the standard exception so upstream handlers can react
                    raise CommandOnCooldown(bucket, retry_after, btype)

            # Log success path (optional but helpful for diagnostics at debug level)
            logger.debug(
                "Cooldown check passed",
                extra={
                    "event": "cooldown_pass",
                    "command": command_name,
                    "user_id": getattr(interaction.user, "id", None),
                    "guild_id": getattr(interaction.guild, "id", None) if interaction.guild else None,
                    "channel_id": getattr(interaction.channel, "id", None) if interaction.channel else None,
                },
            )

            return True

        return check

    return wrapper