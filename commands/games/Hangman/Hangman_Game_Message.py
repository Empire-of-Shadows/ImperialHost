import logging
from typing import Optional, Set, List

import discord

from commands.games.Hangman.hangman_globals import HANGMAN_STAGES
from utilities.logger_setup import get_logger
logger = get_logger("HangmanMessageEditor")


class HangmanMessageEditor:
    def __init__(self, master_cache=None):
        self.cache = master_cache

    async def build_hangman_embed(self, channel_id: int, game_over: bool = False, victory: bool = False) -> Optional[discord.Embed]:
        """
        Build a status embed for the current Hangman game in the channel.

        - Safely clamps the stage index to available visuals.
        - Shows remaining attempts, wrong guesses, and current turn (if enabled).
        - Reads game state from MasterCache instead of global dict.
        """
        # Get game state from cache
        try:
            game = await self.cache.get_state(channel_id)
        except LookupError:
            # No game found in cache
            logger.debug(f"No hangman game found in cache for channel {channel_id}")
            return None
        except Exception as e:
            logger.error(f"Error reading from cache for channel {channel_id}: {e}")
            return None

        if not game or "word" not in game:
            return None

        word: str = str(game.get("word", "")).replace(" ", "-")
        correct: Set[str] = set(game.get("correct", []))  # Now reading from list stored in cache
        wrong: Set[str] = set(game.get("wrong", []))      # Now reading from list stored in cache
        attempts_total = len(HANGMAN_STAGES) - 1
        attempts_left: int = int(game.get("attempts", attempts_total))

        if not word:
            return None

        # Clamp stage index
        stage_index = max(0, min(len(HANGMAN_STAGES) - 1, (len(HANGMAN_STAGES) - 1) - attempts_left))
        hangman_visual = HANGMAN_STAGES[stage_index]

        word_display = self._format_word_display(word, correct)

        # Lower log verbosity for internal values
        logger.debug(f"Hangman word='{word}' correct={sorted(correct)} wrong={sorted(wrong)} attempts_left={attempts_left}")

        wrong_guesses = ", ".join(sorted(wrong)) or "None"
        participants: List[int] = list(game.get("participants", []))
        participants_display = ", ".join([f"<@{user_id}>" for user_id in participants]) if participants else "Everyone"

        embed = discord.Embed(title="Hangman", color=discord.Color.blurple())
        embed.add_field(name="Word", value=f"**{word_display}**", inline=False)
        embed.add_field(name="Hangman", value=f"```\n{hangman_visual}\n```", inline=False)
        embed.add_field(name="Wrong Letters", value=wrong_guesses, inline=False)
        embed.add_field(name="Attempts Left", value=str(max(0, attempts_left)), inline=True)
        embed.add_field(name="Participants", value=participants_display, inline=False)

        # Footer and turn
        self._add_footer(embed, game, game_over, victory)

        if participants and not game_over and not victory:
            current_turn_index = int(game.get("current_turn_index", 0))
            if participants:
                # Clamp index defensively
                current_turn_index = current_turn_index % len(participants)
                current_participant = participants[current_turn_index]
                embed.add_field(name="Current Turn", value=f"It's <@{current_participant}>'s turn!", inline=False)

        # Outcome-specific styling
        if game_over and not victory:
            embed.description = f"**Game Over!** The word was: **{word}**"
            embed.color = discord.Color.red()
        elif victory:
            embed.description = f"**Victory!** You guessed the word: **{word}**"
            embed.color = discord.Color.green()

        return embed

    async def edit_game_message(self, interaction: discord.Interaction, channel_id: int, embed: discord.Embed):
        """
        Edit the persistent game message for this channel with the latest embed.
        Reads message_id from cache.
        """
        try:
            # Get game state from cache to find message_id
            game = await self.cache.get_state(channel_id)
            message_id = game.get("message_id") if game else None
        except LookupError:
            logger.warning(f"No game state found in cache for channel {channel_id}")
            return
        except Exception as e:
            logger.error(f"Error reading from cache for channel {channel_id}: {e}")
            return

        try:
            if not game or not message_id:
                logger.warning(f"No game state or message found for channel {channel_id}.")
                return

            channel = interaction.channel
            if channel is None:
                logger.warning(f"Interaction has no channel for channel_id={channel_id}")
                return

            old_message = await channel.fetch_message(message_id)
            await old_message.edit(embed=embed)
            logger.info(f"Message {message_id} updated successfully.")
        except discord.NotFound:
            logger.error(f"Message {message_id} not found in channel {channel_id}.")
        except discord.Forbidden:
            logger.error(f"Permission issue while editing message {message_id}.")
        except Exception as e:
            logger.error(f"Unexpected error in edit_game_message: {e}", exc_info=True)

    def _add_footer(self, embed: discord.Embed, game: dict, game_over: bool, victory: bool) -> None:
        if game.get("custom_word", False):
            embed.set_footer(text="Custom Game")
            return

        if game_over or victory:
            return

        participants = game.get("participants", [])
        if len(participants) >= 2:
            embed.set_footer(text="Leaderboard Active")
        else:
            embed.set_footer(text="Leaderboard Inactive — add participants to activate")

    def _format_word_display(self, word: str, correct: Set[str]) -> str:
        """
        Replace unguessed letters with escaped underscores for Discord formatting safety.
        Keep hyphens visible (converted from spaces earlier).
        """
        parts = []
        for ch in word:
            if ch == "-":
                parts.append("-")
            elif ch in correct:
                parts.append(ch)
            else:
                # Escape underscore so Discord doesn't treat it as italics
                parts.append(r"\_")
        return " ".join(parts)