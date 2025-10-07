import os
from dotenv import load_dotenv
import discord
from discord.ext import commands

# Load environment variables
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

# Discord bot setup
intents = discord.Intents.default()
intents.messages = True
intents.members = True
intents.message_content = True
intents.guilds = True

bot = commands.Bot(
    command_prefix=".",
    intents=intents,
    help_command=None,
    shard_id=0,
    shard_count=1
)


class RequestDispatcher:
    """Simple dispatcher for handling async message sending"""

    @staticmethod
    async def submit(coro):
        """Submit a coroutine for execution"""
        return await coro


# Create dispatcher instance
dispatcher = RequestDispatcher()

# Additional Bot Configurations
# dispatcher = RequestDispatcher()
DISCORD_CHANNEL_ID = 1365254749456171068  # Example: Your Discord channel ID
TIMEZONE_NAME = "America/Chicago"  # Preferred timezone
GUILD_ID = 1265120128295632926
guild_object = discord.Object(id=1265120128295632926)

SIMILARITY_THRESHOLD = 80
WELCOME_CHANNEL_ID = 1371686628510269460

# Expose shared resources for other files
__all__ = [
    "bot", "TOKEN", "guild_object", "dispatcher",
    "GUILD_ID", "WELCOME_CHANNEL_ID", "SIMILARITY_THRESHOLD"
]