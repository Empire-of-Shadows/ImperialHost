import asyncio
import logging
import os
import signal
from pathlib import Path
from typing import List, Dict, Any

import discord
from tabulate import tabulate

from utilities.bot import bot
from utilities.idle import rotate_status

from utilities.logger_setup import (
    setup_application_logging,
    log_performance,
    PerformanceLogger,
    log_context,
    get_logger,
)

COG_DIRECTORIES = ["./NewMembers", "./listeners", "./games", "./commands", "./daily", "./ECOM", "./Guide", "./updates-drops", "profiles"]

# Configure application-wide logging once. Level can be overridden via env LOG_LEVEL.
_LOG_LEVEL = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logger = setup_application_logging(
    app_name="app.startup",
    log_level=_LOG_LEVEL,
    log_dir="logs",
    enable_performance_logging=True,
)

# Module-specific child logger (optional, keeps namespace tidy in log files)
logger = get_logger("app.startup")

s = " " * 5
@log_performance("load_cogs")
async def load_cogs():
    """
    Load all cogs from specified directories in `COG_DIRECTORIES`.
    Group and log successful loads (`✅`) and failed ones (`❌`) together.
    """
    success_logs = [f"{s}🔄 Starting cog loading process...\n"]
    failed_logs = []

    for base_dir in COG_DIRECTORIES:
        for root, _, files in os.walk(base_dir):
            for file in files:
                if not file.endswith(".py") or file.startswith("__"):
                    continue

                module_name = generate_cog_module_name(root, file)

                # Skip specific cases
                if module_name in bot.extensions:
                    success_logs.append(f"{s}🔄 Skipping already loaded cog: {module_name}\n")
                    continue

                # Safely load the cog and append to appropriate log
                result, is_success = await safely_load_cog(module_name, os.path.join(root, file))
                if is_success:
                    success_logs.append(result)
                else:
                    failed_logs.append(result)

    # Add summary headers and combine logs
    if failed_logs:
        failed_logs.insert(0, f"{s}❌ Failed to load the following cogs:\n")
    success_logs.append(f"{s}✅ Successfully loaded the following cogs:\n")

    # Combine and log the final output
    final_logs = failed_logs + success_logs if failed_logs else success_logs
    logger.info("\n" + "".join(final_logs) + f"{s}✅ Cog loading process completed.\n")


async def safely_load_cog(module, file_path):
    """
    Dynamically import and load a cog module.
    Returns the result as a formatted string and a success status.
    """
    try:
        await bot.load_extension(module)
        return f"{s}✅ {module}\n", True
    except Exception as e:
        return f"{s}❌ {module} → Error: {e}\n", False


def generate_cog_module_name(root, file):
    """
    Helper to generate the fully qualified module name from root and file.
    """
    # Normalize paths and remove leading "./" if present
    relative_path = os.path.relpath(os.path.join(root, file), start=str(Path("."))).replace("\\", "/")
    # Convert to Python module format
    module_name = relative_path.replace("/", ".").removesuffix(".py")
    logger.info(f"Generating module name for {file}: {module_name}")
    return module_name


def log_synced_commands(app_commands_list):
    """
    Logs all synced commands (including groups/subcommands) in a table.
    """
    if not app_commands_list:
        logger.info("No commands found to sync.")
        return

    def command_kind(cmd: object) -> str:
        try:
            if isinstance(cmd, discord.app_commands.Group):
                return "Group"
            if isinstance(cmd, discord.app_commands.ContextMenu):
                return f"ContextMenu:{getattr(cmd.type, 'name', 'Unknown')}"
            if isinstance(cmd, discord.app_commands.Command):
                return "Slash"
        except Exception:
            pass
        return cmd.__class__.__name__

    command_data = []

    def parse_command(command, prefix=""):
        if isinstance(command, discord.app_commands.Group):
            command_data.append(
                [
                    f"{prefix}{command.name} (Group)",
                    command.description or "No description provided.",
                    "Group",
                ]
            )
            for subcommand in getattr(command, "commands", []):
                parse_command(subcommand, prefix=prefix + "  └─ ")
        else:
            command_data.append(
                [
                    f"{prefix}{command.name}",
                    getattr(command, "description", None) or "No description provided.",
                    command_kind(command),
                ]
            )

    with PerformanceLogger(logger, "render_synced_commands_table"):
        for cmd in app_commands_list:
            parse_command(cmd)

        table = tabulate(command_data, headers=["Name", "Description", "Type"], tablefmt="fancy_grid")

    logger.info("Synced Commands:\n%s", table)


@log_performance("shutdown")
async def shutdown():
    """
    Gracefully shuts down the bot and stops background tasks.
    """
    logger.info("Shutting down gracefully...")
    try:
        # Stop rotating the bot status
        if rotate_status.is_running():
            logger.info("Stopping the status rotation task.")
            rotate_status.stop()

        # Close the bot
        await bot.close()
    except Exception:
        logger.exception("Error during shutdown")
    finally:
        logger.info("Shutdown complete.")


def _register_posix_signals():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
            logger.debug("Registered signal handler for %s", sig.name)
        except NotImplementedError:
            # Not available on some platforms/event loops
            logger.debug("Signal handlers not supported for %s on this platform/event loop", sig)
            pass