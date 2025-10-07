import datetime
from typing import Optional, Dict, Any

import discord
from discord import app_commands

from storage.config_system import config
from utilities.logger_setup import get_logger

logger = get_logger("TicTacToeEndGameManager")


class TicTacToeEndGameManager:
    """
    Tracks active Tic-Tac-Toe lobbies/games for health checks and end-of-game reporting.
    - Register lobbies when they are created.
    - Keep last interaction timestamps for inactivity cleanup.
    - Validate the sticky/lobby message to ensure UI consistency.
    - Post a final summary embed to a designated channel when a game ends.
    """

    INACTIVITY_TIMEOUT = datetime.timedelta(seconds=15)
    FINAL_EMBED_CHANNEL_ID = config.win_feed_channel_id

    def __init__(self):
        """Initialize the game manager with an empty dictionary for active games."""
        # { channel_id: {
        #     "message": discord.PartialMessage | discord.Message,
        #     "players": list[int],
        #     "winner": Optional[int],
        #     "difficulty": str,
        #     "thread_id": Optional[int],
        #     "last_interaction": datetime.datetime,
        #   }
        # }
        self.games: Dict[int, Dict[str, Any]] = {}

    # ---------- Registration and updates ----------

    def register_lobby(
        self,
        message: discord.Message,
        players: list[int] | None = None,
        difficulty: str = "easy",
        thread_id: Optional[int] = None,
    ) -> None:
        """
        Register or update a lobby/game record for the given channel using the lobby/sticky message.

        Args:
            message: The lobby or sticky message associated with this game.
            players: Optional initial list of player IDs (up to 2), missing slots as None.
            difficulty: "easy" or "medium" for board size context in summaries.
            thread_id: Optional discussion thread ID.
        """
        ch_id = int(message.channel.id)
        safe_players = list(players or [])
        while len(safe_players) < 2:
            safe_players.append(None)

        self.games[ch_id] = {
            "message": message,
            "players": safe_players[:2],
            "winner": None,
            "difficulty": "easy" if difficulty not in ("easy", "medium") else difficulty,
            "thread_id": int(thread_id) if thread_id else None,
            "last_interaction": discord.utils.utcnow(),
        }
        logger.info(f"TicTacToeEndGameManager: registered lobby for channel {ch_id}")

    def set_message(self, channel: discord.TextChannel, message_id: int) -> None:
        """
        Store/replace the tracked message reference for a game using a known message ID.
        """
        ch_id = int(channel.id)
        if ch_id not in self.games:
            self.games[ch_id] = {}
        self.games[ch_id]["message"] = channel.get_partial_message(int(message_id))
        self.games[ch_id]["last_interaction"] = discord.utils.utcnow()

    def set_thread(self, channel_id: int, thread_id: int | None) -> None:
        """Attach or update the discussion thread for a registered game."""
        game = self.games.get(int(channel_id))
        if not game:
            return
        game["thread_id"] = int(thread_id) if thread_id else None
        game["last_interaction"] = discord.utils.utcnow()

    def set_players(self, channel_id: int, p0: Optional[int], p1: Optional[int]) -> None:
        """Update the two player slots for the given game/channel."""
        game = self.games.get(int(channel_id))
        if not game:
            return
        game["players"] = [p0, p1]
        game["last_interaction"] = discord.utils.utcnow()

    async def update_last_interaction(self, interaction: discord.Interaction):
        """Update the last interaction timestamp for an ongoing game."""
        game = self.games.get(interaction.channel.id if interaction.channel else 0)
        if game:
            game["last_interaction"] = discord.utils.utcnow()

    # ---------- Validation helpers ----------

    async def is_game_embed_valid(self, channel_id: int) -> bool:
        """
        Check if the tracked message for a channel exists and still has an embed (UI not deleted).
        Returns True if valid; False otherwise.
        """
        game = self.games.get(int(channel_id))
        if not game or not game.get("message"):
            return False
        try:
            message = await game["message"].fetch()
            if not message.embeds:
                return False
            return True
        except discord.NotFound:
            return False
        except Exception as e:
            logger.warning(f"Embed validation failed for channel {channel_id}: {e}")
            return False

    async def is_game_active(self, channel_id: int) -> bool:
        """
        Determine if a game is active:
        - Must be registered
        - Must have players
        - Must not have a winner set
        - Must have a valid UI message with an embed
        """
        game = self.games.get(int(channel_id))
        if not game:
            return False

        if not game.get("players"):
            return False

        if game.get("winner") is not None:
            return False

        # Validate associated message/embed
        try:
            msg = game.get("message")
            if msg:
                fetched = await msg.fetch()
                if not fetched.embeds:
                    logger.info(f"Game in channel {channel_id} is invalid: missing embed.")
                    return False
                return True
            else:
                logger.info(f"Game in channel {channel_id} is invalid: missing message.")
                return False
        except discord.NotFound:
            logger.info(f"Game in channel {channel_id} is invalid: message deleted.")
            return False
        except Exception as e:
            logger.warning(f"is_game_active check failed for channel {channel_id}: {e}")
            return False

    # ---------- Cleanup ----------

    async def cleanup_inactive_games(self):
        """Checks and removes games that are inactive or completed or have invalid UI."""
        now = discord.utils.utcnow()
        to_remove: list[int] = []

        for channel_id, game in list(self.games.items()):
            if not game.get("players") or game.get("winner") is not None:
                to_remove.append(channel_id)
                continue

            last = game.get("last_interaction") or now
            if (now - last) > self.INACTIVITY_TIMEOUT:
                logger.info(f"Game in channel {channel_id} timed out due to inactivity.")
                to_remove.append(channel_id)
                continue

            if not await self.is_game_active(channel_id):
                logger.info(f"Game in channel {channel_id} is invalid and will be cleaned.")
                to_remove.append(channel_id)

        for channel_id in to_remove:
            self.games.pop(channel_id, None)
            logger.info(f"Game data cleared for channel {channel_id}")

    # ---------- Finalization ----------

    async def post_final_summary(
        self,
        guild: discord.Guild,
        board_display: str,
        channel_id: int,
        winner_id: Optional[int] = None,
        tie: bool = False,
    ) -> None:
        """
        Post a final summary embed to the configured final channel, if present.
        """
        try:
            final_ch = guild.get_channel(self.FINAL_EMBED_CHANNEL_ID)
            if not isinstance(final_ch, (discord.TextChannel, discord.Thread, discord.VoiceChannel)):
                logger.warning(f"Final summary channel {self.FINAL_EMBED_CHANNEL_ID} not found or not text-capable.")
                return

            title = "Tic-Tac-Toe: Game Over"
            if tie:
                desc = f"Result: Tie 🤝\n\n{board_display}"
                color = discord.Color.gold()
            else:
                win_mention = f"<@{winner_id}>" if winner_id else "Unknown"
                desc = f"Winner: {win_mention} 🎉\n\n{board_display}"
                color = discord.Color.green()

            embed = discord.Embed(title=title, description=desc, color=color)
            embed.add_field(name="Channel", value=f"<#{int(channel_id)}>", inline=True)

            game = self.games.get(int(channel_id))
            if game and game.get("thread_id"):
                embed.add_field(name="Discussion", value=f"<#{int(game['thread_id'])}>", inline=True)

            await final_ch.send(embed=embed)
        except Exception as e:
            logger.error(f"Failed to post final summary for ch {channel_id}: {e}")

    def mark_winner(self, channel_id: int, winner_id: Optional[int]) -> None:
        """Set the winner id in the tracked game (None for tie) and update last interaction."""
        game = self.games.get(int(channel_id))
        if not game:
            return
        game["winner"] = winner_id
        game["last_interaction"] = discord.utils.utcnow()

    def unregister(self, channel_id: int) -> None:
        """Remove a game from tracking (use after you’ve cleaned up views/state elsewhere)."""
        if int(channel_id) in self.games:
            self.games.pop(int(channel_id), None)
            logger.info(f"TicTacToeEndGameManager: unregistered game {channel_id}")