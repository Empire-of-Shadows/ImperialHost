import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands
from discord.ext.commands import BucketType

from commands.games.Hangman.check_message import check_message_exists
from commands.games.Hangman.hangmangame import hangman_game_manager
from storage.config_system import config
from utilities.cooldown import cooldown_enforcer, guess_cd, hangman_start_cd
from utilities.logger_setup import get_logger

logger = get_logger("Hangman")


class HangmanCommandCog(commands.GroupCog, name="hangman"):
    """Manage Hangman-related commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        super().__init__()

    @app_commands.command(name="start", description="Start a Hangman game!")
    @app_commands.describe(
        word="Provide a custom word if desired (overrides other parameters).",
        participants="Mention specific participants (leave empty for everyone).",
        min_length="Minimum word length (default: 3).",
        max_length="Maximum word length (default: 8).",
    )
    @app_commands.check(cooldown_enforcer(hangman_start_cd, BucketType.user)())
    async def start(
        self,
        interaction: discord.Interaction,
        word: Optional[str] = None,
        participants: Optional[str] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ):
        """
        Starts a Hangman game in a new channel under the configured category.
        """
        try:
            channel_id = interaction.channel_id

            # Restrict command to a specific channel
            allowed_channel_id = 1406642649997508758
            if channel_id != allowed_channel_id:
                # Prefer a clean ephemeral notice
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"This command can only be used in <#{allowed_channel_id}>.",
                        ephemeral=True,
                    )
                else:
                    await interaction.followup.send(
                        f"This command can only be used in <#{allowed_channel_id}>.",
                        ephemeral=True,
                    )
                return

            # Cleanup stale state if message no longer exists (for the allowed channel slot only)
            # Use HangmanGameManager's cache/state
            try:
                cache = hangman_game_manager._get_cache()
                if cache:
                    game_state = await cache.get_state(channel_id)
                else:
                    game_state = None
                if game_state and game_state.get("message_id"):
                    message_id = game_state["message_id"]
                    channel = interaction.channel
                    try:
                        exists = await check_message_exists(channel, message_id)
                    except Exception:
                        exists = True  # Be conservative if check fails
                    if not exists and cache:
                        logger.info("Stale game message not found; clearing state | channel=%s", channel_id)
                        await cache.replace_state(channel_id, {})
            except LookupError:
                # No game state exists - this is fine
                pass
            except Exception as e:
                logger.error(f"Error checking for stale game state: {e}")
                # Continue with game start despite error

            # Parse participants into user ids
            selected_participants = await self.parse_participants(interaction, participants)
            if participants and not selected_participants:
                await interaction.response.send_message(
                    "Unable to process the provided participants. Please check mentions and try again.",
                    ephemeral=True,
                )
                return

            # Validate word length bounds; also applied to custom word
            if min_length is not None and max_length is not None:
                if min_length < 3 or max_length > 20 or min_length > max_length:
                    await interaction.response.send_message(
                        "Please provide a valid word length range (min: 3, max: 20, and min <= max).",
                        ephemeral=True,
                    )
                    return

            # Defaults
            min_length = min_length or 3
            max_length = max_length or 8

            # Create a dedicated game channel under the specified category
            guild = interaction.guild
            if guild is None:
                await interaction.response.send_message(
                    "This command can only be used in a server (guild).", ephemeral=True
                )
                return

            category = guild.get_channel(config.game_category_id)
            if not category or not isinstance(category, discord.CategoryChannel):
                await interaction.response.send_message(
                    "Game category not found or invalid. Please contact an administrator.", ephemeral=True
                )
                return

            game_name = f"hangman-{discord.utils.utcnow().strftime('%Y%m%d-%H%M%S')}"
            try:
                new_channel = await guild.create_text_channel(
                    name=game_name,
                    category=category,
                    reason="Hangman: New game channel",
                )
            except Exception as e:
                logger.error(f"Failed to create Hangman game channel: {e}")
                await interaction.response.send_message(
                    "Failed to create a new game channel. Please try again later.", ephemeral=True
                )
                return

            # Pick a word: if custom word not provided, set a placeholder/length policy.
            # Here we enforce length only if a custom word is given.
            if word:
                if not (min_length <= len(word) <= max_length):
                    await interaction.response.send_message(
                        f"Custom word must be between {min_length} and {max_length} characters.",
                        ephemeral=True,
                    )
                    return
                chosen_word = word
            else:
                # Fallback simple word to satisfy start; real selection can be integrated later.
                chosen_word = "hangman"

            # Start the game in the new channel using HangmanGameManager
            try:
                await hangman_game_manager.start_game(
                    channel_id=new_channel.id,
                    word=chosen_word,
                    max_attempts=6,
                    guild=guild,
                    game_name=new_channel.name,
                )
            except Exception as e:
                logger.error(f"Failed to start Hangman game: {e}")
                await interaction.response.send_message(
                    "Failed to initialize the game. Please try again later.", ephemeral=True
                )
                try:
                    await new_channel.delete(reason="Hangman: Failed to initialize game")
                except Exception:
                    pass
                return

            # Acknowledge start
            await interaction.response.send_message(
                f"Hangman game created: {new_channel.mention}", ephemeral=True
            )

        except Exception as e:
            logger.exception("Error in /hangman start command")
            # Use response if possible, otherwise followup
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"An error occurred while starting the game: {e}", ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"An error occurred while starting the game: {e}", ephemeral=True
                )

    @staticmethod
    async def parse_participants(interaction: discord.Interaction, participants: Optional[str]):
        """
        Parse participant mentions into a list of user IDs.
        """
        selected_participants = []
        if participants:
            for mention in participants.split():
                if mention.startswith("<@") and mention.endswith(">"):
                    try:
                        member_id = int(mention.strip("<@!>"))
                        member = await interaction.guild.fetch_member(member_id)
                        if member:
                            selected_participants.append(member.id)
                    except discord.NotFound:
                        logger.debug("Participant not found; ignoring | mention=%s", mention)
                    except Exception as e:
                        logger.debug("Failed to parse participant | mention=%s error=%s", mention, e)
        return selected_participants

    @app_commands.command(name="guess", description="Guess a letter or word in an ongoing Hangman game.")
    @app_commands.describe(letter="The letter you want to guess (one character).")
    @app_commands.describe(word="Try to guess the word.")
    @app_commands.check(cooldown_enforcer(guess_cd, BucketType.user)())
    async def guess(self, interaction: discord.Interaction, letter: Optional[str] = None, word: Optional[str] = None):
        """
        Allows users to make guesses for Hangman.
        """
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=False)

            if not letter and not word:
                await interaction.followup.send("You must provide either a letter or a word.", ephemeral=False)
                return

            # Route guesses through the button/manager mechanics:
            # If a single character, simulate the guess button path; otherwise, ignore for now.
            guess_value = (letter or word).strip()
            if len(guess_value) == 1:
                # Use the existing manager flow by creating a temporary button instance
                from commands.games.Hangman.hangmangame import HangmanGuessButton  # local import to avoid cycles
                btn = HangmanGuessButton(guess_value.lower())
                await btn.callback(interaction)
            else:
                await interaction.followup.send(
                    "Word guesses are not supported via this command yet. Use letter guesses (A-Z).",
                    ephemeral=False,
                )
        except Exception as e:
            logger.debug("Guess failed | error=%s", e)
            if not interaction.response.is_done():
                await interaction.response.send_message("Something went wrong with your guess.", ephemeral=True)
            else:
                await interaction.followup.send("Something went wrong with your guess.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(HangmanCommandCog(bot))