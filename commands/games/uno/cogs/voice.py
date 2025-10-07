from typing import Optional

import discord
from discord.ext import commands

from utilities.logger_setup import get_logger

logger = get_logger("VoiceManager")

class VoiceManager(commands.Cog):
    """Cog for managing voice channels specifically tied to UNO games."""

    def __init__(self, bot: commands.Bot):
        """
        Initialize the VoiceManager cog.
        """
        self.bot = bot
        logger.info("VoiceManager Cog initialized.")

    async def create_game_voice_channel(
            self,
            guild: discord.Guild,
            category: discord.CategoryChannel,
            game_channel: discord.TextChannel,
            players: list[discord.Member]
    ) -> Optional[discord.VoiceChannel]:
        """
        Create a voice channel tied to a UNO game with access restricted to players.

        Args:
            guild (discord.Guild): The guild where the channel will be created.
            category (discord.CategoryChannel): The category for the voice channel.
            game_channel (discord.TextChannel): The text channel tied to the game.
            players (list[discord.Member]): List of players with access to the channel.

        Returns:
            discord.VoiceChannel: The created voice channel or None if creation fails.
        """
        try:
            if not guild or not category or not game_channel:
                logger.warning("Missing required parameters for creating a voice channel.")
                return None

            # Define permissions (restrict to players and the bot)
            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),  # Default role cannot view
                self.bot.user: discord.PermissionOverwrite(view_channel=True, speak=True, connect=True)
            }

            # Add permissions for each player (Ensure they are valid members)
            for player in players:
                if isinstance(player, discord.Member):
                    overwrites[player] = discord.PermissionOverwrite(view_channel=True, speak=True, connect=True)
                else:
                    logger.warning(f"Player {player} is not a valid discord.Member. Skipping.")

            # Create the voice channel
            logger.info(f"Creating voice channel for game channel {game_channel.name} in category {category.name}.")
            voice_channel = await guild.create_voice_channel(
                name=f"{game_channel.name}-voice",
                overwrites=overwrites,
                category=category
            )

            logger.info(f"Voice channel '{voice_channel.name}' created successfully in guild '{guild.name}'.")
            return voice_channel

        except discord.Forbidden as e:
            logger.error(f"Insufficient permissions to create voice channel in guild '{guild.name}': {e}")
        except discord.HTTPException as e:
            logger.error(f"Failed to create voice channel in guild '{guild.name}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error while creating a voice channel: {e}", exc_info=True)

        return None

    async def delete_game_voice_channel(self, voice_channel: discord.VoiceChannel) -> bool:
        """
        Delete the temporary UNO voice channel.

        Args:
            voice_channel (discord.VoiceChannel): The voice channel to delete.

        Returns:
            bool: True if successfully deleted, False otherwise.
        """
        try:
            if not voice_channel or not voice_channel.guild:
                logger.warning("The provided voice channel is invalid or does not exist.")
                return False

            if not voice_channel.permissions_for(voice_channel.guild.me).manage_channels:
                logger.warning(f"Missing permissions to delete voice channel '{voice_channel.name}'.")
                return False

            # Delete the channel
            logger.info(f"Deleting voice channel '{voice_channel.name}' in guild '{voice_channel.guild.name}'.")
            await voice_channel.delete()
            logger.info(f"Voice channel '{voice_channel.name}' deleted successfully.")
            return True

        except discord.Forbidden as e:
            logger.error(f"Insufficient permissions to delete the voice channel '{voice_channel.name}': {e}")
        except discord.HTTPException as e:
            logger.error(f"Failed to delete voice channel '{voice_channel.name}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error while deleting voice channel '{voice_channel.name}': {e}", exc_info=True)

        return False
async def voice(bot):
    await bot.add_cog(VoiceManager(bot))