import asyncio
import os
import platform
import sys
import time
from utilities.bot import bot, TOKEN

import discord
from host_startup.startup import load_cogs, log_synced_commands, shutdown, _register_posix_signals
from utilities.idle import rotate_status
from utilities.logger_setup import get_logger, log_performance, log_context, PerformanceLogger

logger = get_logger("Host")


@bot.event
async def on_ready():
    """
    Lightweight ready handler – avoid heavy work here to prevent duplicate work on reconnects.
    """
    logger.info(f"Logged in as {bot.user} ({bot.user.id})")
    await bot.change_presence(status=discord.Status.online)
    logger.info("Bot is online")


@bot.event
async def on_message(message: discord.Message):
    """
    Process incoming messages and pass to commands.
    """
    await bot.process_commands(message)


@log_performance("command_sync")
async def _sync_commands():
    """
    Sync application commands.
    Supports optional GUILD_ID for fast dev sync.
    """
    guild_id = os.getenv("SYNC_GUILD_ID")
    if guild_id:
        guild_obj = discord.Object(id=int(guild_id))
        # Copy globals to guild for quick iteration
        bot.tree.copy_global_to(guild=guild_obj)
        await bot.tree.sync(guild=guild_obj)
        logger.info(f"Commands synced to guild {guild_id} (dev mode).")
    else:
        await bot.tree.sync()
        logger.info("Global commands synced.")

    log_synced_commands(bot.tree.get_commands())


@bot.event
async def setup_hook():
    """
    Runs once after login, before on_ready.
    Ideal for loading cogs, syncing commands, and starting background tasks.
    """
    with PerformanceLogger(logger, "setup_hook"):
        # Load cogs
        with PerformanceLogger(logger, "load_cogs"):
            await load_cogs()

        # Sync commands
        await _sync_commands()

        # Start background tasks
        if not rotate_status.is_running():
            rotate_status.start()
            logger.debug("Status rotation task started")


async def main():
    """
    Structured async entry point with clean startup and shutdown.
    """
    if not TOKEN or not TOKEN.strip():
        logger.error("TOKEN is missing or empty. Set it before starting the bot.")
        return

    # Environment introspection
    logger.info(
        "Starting bot with environment: "
        f"Python={sys.version.split()[0]}, Platform={platform.system()} {platform.release()}"
    )

    with log_context(logger, "bot_runtime"):
        try:
            if platform.system() != "Windows":
                _register_posix_signals()
                logger.debug("POSIX signals registered")

            logger.info("Starting Discord bot connection...")
            await bot.start(TOKEN)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Initiating shutdown...")
        except Exception:
            logger.exception("Fatal error in bot runtime")
        finally:
            # Ensure orderly shutdown
            logger.info("Beginning graceful shutdown...")
            await shutdown()
            logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())