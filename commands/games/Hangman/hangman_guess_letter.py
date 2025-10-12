import logging
import string
from typing import Optional

import discord

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
    - Uses HangmanGameManager instance from hangmangame.py for state operations.
    """

    def __init__(self, master_cache: Optional[object] = None):
        # Master cache not directly used here; state is managed by HangmanGameManager
        self.cache = master_cache

    async def process_guess(self, interaction: discord.Interaction, guess: str) -> None:
        from commands.games.Hangman.hangmangame import hangman_game_manager  # local import to avoid cycles

        channel_id = interaction.channel_id

        # Load or hydrate the game via manager
        game = await hangman_game_manager.get_game(channel_id)
        if not game:
            await self._safe_followup(
                interaction,
                "No active Hangman game in this channel. Start one with `/hangman start`.",
                ephemeral=False,
            )
            return

        # If game already ended
        if game.winner or game.loser:
            await self._safe_followup(interaction, "This hangman game has ended.", False)
            return

        user_id = interaction.user.id

        # Normalize guess: keep only a-z and lower-case
        norm = self._normalize_guess(guess)
        if not norm:
            await self._safe_followup(interaction, "Please guess letters A-Z (no spaces or symbols).", False)
            return

        # Enforce single-player seat from HangmanGame
        if game.player_id and user_id != game.player_id:
            await self._safe_followup(interaction, "Only the current player can make guesses.", False)
            return
        if not game.player_id:
            game.player_id = user_id
            await hangman_game_manager._persist_update(game)

        # Ensure discussion thread and guild id
        try:
            if interaction.guild and not game.guild_id:
                game.guild_id = int(interaction.guild.id)
                await hangman_game_manager._persist_update(game)
        except Exception:
            pass
        await hangman_game_manager._ensure_discussion_thread(interaction, game)

        # Process guess
        if len(norm) == 1:
            valid = game.guess_letter(norm)
            if not valid:
                await self._safe_followup(interaction, "Invalid or duplicate guess.", False)
                return
        else:
            # Word guess: reveal or penalize
            if norm == game.secret_word:
                # Fill progress completely
                for i, ch in enumerate(game.secret_word):
                    if ch.isalpha():
                        game.progress[i] = ch
                game.winner = game.player_id
            else:
                # Penalize incorrect word guess by one wrong attempt
                game.wrong_guesses += 1

        # Persist after guess
        await hangman_game_manager._persist_update(game)

        # End states
        if game.is_solved() or game.winner:
            game.winner = game.player_id
            await hangman_game_manager._persist_update(game)
            await hangman_game_manager._handle_win(interaction, game)
            return

        if game.is_failed():
            game.loser = game.player_id
            await hangman_game_manager._persist_update(game)
            await hangman_game_manager._handle_loss(interaction, game)
            return

        # Ongoing state: update message in place
        embed = discord.Embed(
            title="Hangman",
            description=game.format_board(),
            color=discord.Color.blue(),
        )
        from commands.games.Hangman.hangmangame import make_view  # local import

        view = await make_view(game, disable_inputs=False)
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
            else:
                sent = False
                try:
                    if game.thread_id and interaction.guild:
                        thread = interaction.guild.get_thread(game.thread_id)
                        if thread:
                            await thread.send(embed=embed, view=view)
                            sent = True
                except Exception:
                    pass
                if not sent:
                    await interaction.followup.send(embed=embed, view=view, ephemeral=False)
        except discord.errors.InteractionResponded:
            pass
        finally:
            await hangman_game_manager._persist_update(game)

    async def _handle_word_guess(self, game: dict, word_guess: str) -> str:
        # Not used in the refactored flow; kept for compatibility if needed
        return "continue"

    async def _handle_letter_guess(self, interaction: discord.Interaction, game: dict, letter: str) -> str:
        # Not used in the refactored flow; kept for compatibility if needed
        return "continue"

    def _advance_turn(self, game: dict) -> None:
        # Not applicable in single-player manager flow
        return

    async def _update_game_state(self, interaction: discord.Interaction, game: dict) -> None:
        # Not used; state updates are handled inline via manager
        return

    async def _finalize_game(self, interaction: discord.Interaction, game: dict, *, game_over: bool, victory: bool):
        # Not used; finalization handled by manager
        return

    async def _increment_leaderboard_best_effort(self, user_id: int) -> None:
        """
        Try to increment leaderboard for the given user. Best-effort; safe to skip if unavailable.
        """
        try:
            from commands.games.Hangman.hangmangame import master_cache  # local import
            if master_cache is None:
                return
            await master_cache.increment_leaderboard(user_id, 1)  # type: ignore[attr-defined]
        except Exception:
            logger.debug("Skipping leaderboard increment (best-effort).")

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


async def guess_letter(interaction: discord.Interaction, guess: str) -> None:
    """Handle letter or word guesses in Hangman."""
    # Route to local handler; avoid unresolved reference to external symbol
    handler = HangmanGameHandler()
    await handler.process_guess(interaction, guess or "")