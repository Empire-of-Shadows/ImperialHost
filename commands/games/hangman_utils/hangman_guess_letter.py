import logging
import string
from typing import Optional

import discord

from commands.games.hangman_utils.hangman_globals import hangman_games
from commands.games.hangman_utils.Hangman_Game_Message import message_editor
from utilities.logger_setup import get_logger

logger = get_logger("HangmanGameHandler")


class HangmanGameHandler:
    """
    Handles guess processing and game progression for Hangman.

    Improvements:
    - Normalizes and validates guesses (letters only).
    - Skips advancing turn on repeated guesses.
    - Increments leaderboard via MasterCache on victory (best-effort).
    - Defensive messaging to avoid interaction double-respond errors.
    """

    def __init__(self, master_cache: Optional[object] = None):
        # Optional dependency; if not provided, we'll try to import lazily on first use
        self._master_cache = master_cache

    async def process_guess(self, interaction: discord.Interaction, guess: str) -> None:
        channel_id = interaction.channel_id

        if channel_id not in hangman_games:
            await self._safe_followup(
                interaction,
                "No active Hangman game in this channel. Start one with `/hangman start`.",
                ephemeral=False,
            )
            return

        game = hangman_games[channel_id]
        user_id = interaction.user.id

        # Normalize guess: keep only a-z and lower-case
        norm = self._normalize_guess(guess)
        if not norm:
            await self._safe_followup(interaction, "Please guess letters A-Z (no spaces or symbols).", False)
            return

        # If participants are set, enforce membership
        if game.get("participants") and user_id not in game["participants"]:
            await self._safe_followup(interaction, "You are not a participant in this game.", False)
            return

        # Turn order enforcement
        if game.get("participants"):
            current_turn_index = game.get("current_turn_index", 0)
            current_user_turn = game["participants"][current_turn_index]
            if user_id != current_user_turn:
                await self._safe_followup(
                    interaction,
                    f"It’s not your turn! It’s <@{current_user_turn}>'s turn.",
                    False,
                )
                return

        # Letter or word guess
        if len(norm) > 1:
            result = await self._handle_word_guess(game, norm)
        else:
            result = await self._handle_letter_guess(interaction, game, norm)

        # Only advance turn on actual progress ("continue" or "victory"), not "repeat"
        if result != "repeat":
            self._advance_turn(game)

        await self._update_game_state(interaction, game)

        # If game ended, finalize and award points
        game_over = game["attempts"] <= 0
        victory = set(game["word"]) == game["correct"]
        if game_over or victory:
            await self._finalize_game(interaction, game, game_over=game_over, victory=victory)

    async def _handle_word_guess(self, game: dict, word_guess: str) -> str:
        if word_guess == game["word"]:
            game["correct"].update(set(game["word"]))  # Mark all letters correct
            return "victory"
        else:
            game["attempts"] -= 1  # Penalize for incorrect word
            return "continue"

    async def _handle_letter_guess(self, interaction: discord.Interaction, game: dict, letter: str) -> str:
        if letter in game["correct"] or letter in game["wrong"]:
            await self._safe_followup(interaction, f"The letter `{letter}` has already been guessed!", False)
            return "repeat"

        if letter in game["word"]:
            game["correct"].add(letter)
        else:
            game["wrong"].add(letter)
            game["attempts"] -= 1

        return "continue"

    def _advance_turn(self, game: dict) -> None:
        participants = game.get("participants") or []
        if participants:
            game["current_turn_index"] = (game.get("current_turn_index", 0) + 1) % len(participants)

    async def _update_game_state(self, interaction: discord.Interaction, game: dict) -> None:
        channel_id = interaction.channel_id
        game_over = game["attempts"] <= 0
        victory = set(game["word"]) == game["correct"]

        embed = await message_editor.build_hangman_embed(channel_id, game_over=game_over, victory=victory)
        if embed:
            await message_editor.edit_game_message(interaction, channel_id, embed)

    async def _finalize_game(self, interaction: discord.Interaction, game: dict, *, game_over: bool, victory: bool):
        channel_id = interaction.channel_id

        try:
            # Announce result
            if game_over and not victory:
                await self._safe_followup(interaction, f"Game over! The word was `{game['word']}`.", False)
                logger.info(f"Game over | channel=%s word=%s", channel_id, game.get('word'))
            elif victory:
                await self._safe_followup(
                    interaction,
                    f"Congratulations! The word `{game['word']}` was guessed correctly!",
                    False,
                )
                logger.info(f"Victory | channel=%s word=%s", channel_id, game.get('word'))

                # Increment leaderboard for the winner via MasterCache (best-effort)
                winner_id = interaction.user.id
                await self._increment_leaderboard_best_effort(winner_id)

        finally:
            # Remove game state regardless of outcome
            hangman_games.pop(channel_id, None)

    async def _increment_leaderboard_best_effort(self, user_id: int) -> None:
        """
        Try to increment leaderboard for the given user. This integrates with MasterCache.increment_leaderboard.
        This is best-effort and will not raise if the cache is not available.
        """
        cache = self._master_cache
        if cache is None:
            # Lazy import attempt to avoid hard dependency at import time
            try:
                # Adjust import path if your project exposes the singleton elsewhere
                from utilities.master_cache import master_cache as lazy_cache  # type: ignore
                cache = lazy_cache
                self._master_cache = lazy_cache
            except Exception:
                logger.debug("MasterCache not available; skipping leaderboard increment")
                return

        try:
            await cache.increment_leaderboard(user_id, 1)  # +1 for a win
            logger.debug("Leaderboard incremented for user_id=%s", user_id)
        except Exception as e:
            logger.error("Failed to increment leaderboard for user_id=%s: %s", user_id, e, exc_info=True)

    def _normalize_guess(self, raw: str) -> str:
        # Keep only ASCII letters and lower-case
        s = "".join(ch for ch in (raw or "").lower().strip() if ch in string.ascii_lowercase)
        return s

    async def _safe_followup(self, interaction: discord.Interaction, content: str, ephemeral: bool = False) -> None:
        try:
            await interaction.followup.send(content, ephemeral=ephemeral)
        except Exception:
            # As a fallback, try initial response if not done (edge case)
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(content, ephemeral=ephemeral)
            except Exception:
                logger.debug("Failed to send message to interaction")


# Singleton handler; optional MasterCache can be injected by importing code if desired
hangman_game_handler = HangmanGameHandler(master_cache=None)


async def guess_letter(interaction: discord.Interaction, guess: str) -> None:
    """Handle letter or word guesses in Hangman."""
    # Ensure an initial defer was made by the caller; if not, this will still work via followup
    await hangman_game_handler.process_guess(interaction, guess or "")