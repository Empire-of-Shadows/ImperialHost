import os
import datetime
import asyncio
from typing import Optional, Dict, Any, List

import discord
from discord.ext import commands, tasks
from discord.ui import View, Button
from discord.utils import get
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

from commands.games.TicTacToe.check_winner import check_winner
from commands.games.MasterCache import MasterCache
from storage.config_system import config
from utilities.bot import bot
from utilities.logger_setup import get_logger, PerformanceLogger

logger = get_logger("TicTacToeGameManager")
# ---------------- Environment ----------------

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

# Global cache reference initialized to avoid NameError before it's set via set_tictactoe_cache()
master_cache: Optional[MasterCache] = None


def set_tictactoe_cache(cache: MasterCache):
    global master_cache
    master_cache = cache
    tictactoe_game_manager.cache = cache
    logger.info(
        f"TicTacToe MasterCache is set. Global: {master_cache is not None}, Manager: {tictactoe_game_manager.cache is not None}"
    )


def _now_ts() -> float:
    return discord.utils.utcnow().timestamp()


def _serialize_board(board: List[List[str]]) -> List[List[str]]:
    return [[cell for cell in row] for row in board]


def _deserialize_board(data: Any) -> List[List[str]]:
    if not isinstance(data, list):
        return [[" " for _ in range(3)] for _ in range(3)]
    return [[str(cell) for cell in row] for row in data]


async def _record_win(user_id: int):
    if not master_cache:
        logger.warning("MasterCache not set; skipping leaderboard update (win)")
        return
    try:
        await master_cache.leaderboard_collection.update_one(  # type: ignore[attr-defined]
            {"_id": str(user_id)},
            {
                "$inc": {"wins": 1},
                "$setOnInsert": {"ties": 0, "losses": 0},
            },
            upsert=True,
        )
    except Exception as e:
        logger.error(f"Failed to update leaderboard wins for user {user_id}: {e}", exc_info=True)


async def _record_tie(user_id: int):
    if not master_cache:
        logger.warning("MasterCache not set; skipping leaderboard update (tie)")
        return
    try:
        await master_cache.leaderboard_collection.update_one(  # type: ignore[attr-defined]
            {"_id": str(user_id)},
            {
                "$inc": {"ties": 1},
                "$setOnInsert": {"wins": 0, "losses": 0},
            },
            upsert=True,
        )
    except Exception as e:
        logger.error(f"Failed to update leaderboard ties for user {user_id}: {e}", exc_info=True)


async def _record_loss(user_id: int):
    if not master_cache:
        logger.warning("MasterCache not set; skipping leaderboard update (loss)")
        return
    try:
        await master_cache.leaderboard_collection.update_one(  # type: ignore[attr-defined]
            {"_id": str(user_id)},
            {
                "$inc": {"losses": 1},
                "$setOnInsert": {"wins": 0, "ties": 0},
            },
            upsert=True,
        )
    except Exception as e:
        logger.error(f"Failed to update leaderboard losses for user {user_id}: {e}", exc_info=True)


class TicTacToeGame:
    """
    Represents a single Tic-Tac-Toe game, including logic for moves, board state, and outcomes.
    """

    def __init__(self, channel_id: int, difficulty: str):
        self.channel_id = channel_id
        self.board = self._create_board(difficulty)
        self.players = [None, None]  # Player 1 and Player 2
        self.turn = 0  # 0 for Player 1, 1 for Player 2
        self.moves = 0
        self.winner = None
        self.difficulty = difficulty
        self.last_interaction = discord.utils.utcnow()
        self.thread_id: Optional[int] = None
        self.root_message_id: Optional[int] = None
        self.guild_id: Optional[int] = None  # Track owning guild for reliable channel resolution

    @staticmethod
    def _create_board(difficulty: str):
        """Dynamically generate an empty board based on difficulty."""
        if difficulty == "easy":
            return [[" " for _ in range(3)] for _ in range(3)]
        elif difficulty == "medium":
            return [[" " for _ in range(5)] for _ in range(5)]
        raise ValueError("Invalid difficulty level. Choose 'easy' or 'medium'.")

    @classmethod
    def from_state(cls, state: Dict[str, Any]) -> "TicTacToeGame":
        difficulty = state.get("difficulty", "easy")
        channel_id = int(state.get("_id", "0"))
        game = cls(channel_id, difficulty)
        game.board = _deserialize_board(state.get("board"))
        players = state.get("players", [None, None])
        game.players = [int(p) if p is not None else None for p in players]
        game.turn = int(state.get("turn", 0))
        game.moves = int(state.get("moves", 0))
        game.winner = state.get("winner")
        _ = float(state.get("last_interaction_ts", _now_ts()))
        game.last_interaction = discord.utils.utcnow()
        # Discussion/thread metadata
        tid = state.get("thread_id")
        game.thread_id = int(tid) if tid is not None else None
        rm = state.get("root_message_id")
        game.root_message_id = int(rm) if rm is not None else None
        # Guild tracking
        gid = state.get("guild_id")
        game.guild_id = int(gid) if gid is not None else None
        return game

    def to_state(self) -> Dict[str, Any]:
        return {
            "_id": str(self.channel_id),
            "difficulty": self.difficulty,
            "board": _serialize_board(self.board),
            "players": self.players,
            "turn": self.turn,
            "moves": self.moves,
            "winner": self.winner,
            "last_interaction_ts": _now_ts(),
            "thread_id": self.thread_id,
            "root_message_id": self.root_message_id,
            "guild_id": self.guild_id,
        }

    def format_board(self) -> str:
        """Return the game board formatted for display in Discord."""
        return f"```\n" + "\n".join([" | ".join(row) for row in self.board]) + "\n```"

    def format_board_with_turn(self, members) -> str:
        """
        Return the game board formatted for display in Discord with the current player's turn.
        """
        player_symbols = ["X", "O"]
        players = [get(members, id=player_id).mention if player_id else "Waiting..." for player_id in self.players]
        board_display = f"```\n" + "\n".join([" | ".join(row) for row in self.board]) + "\n```"
        current_turn = f"It's {players[self.turn]}'s turn! ({player_symbols[self.turn]})"
        return f"{board_display}\n\n{current_turn}"

    def is_full(self) -> bool:
        """Check if all spots on the board are filled."""
        return self.moves == len(self.board) * len(self.board[0])

    def place_move(self, player_idx: int, x: int, y: int) -> bool:
        """Place a move on the board."""
        if self.board[y][x] == " " and self.players[player_idx]:
            symbol = "X" if player_idx == 0 else "O"
            self.board[y][x] = symbol
            self.moves += 1
            self.turn = 1 - self.turn  # Switch turn
            self.last_interaction = discord.utils.utcnow()
            return True
        return False

    def check_for_winner(self) -> str:
        """Check for a winner using the `check_winner` utility."""
        self.winner = check_winner(self.board)
        return self.winner


class TicTacToeGameManager(commands.Cog, name="TicTacToeGame"):
    """
    Manages multiple Tic-Tac-Toe games, including their creation, state, and cleanup.
    Also acts as the Cog responsible for DB/cache lifecycle.
    """
    INACTIVITY_TIMEOUT = datetime.timedelta(seconds=15)

    def __init__(self, bot: Optional[commands.Bot] = None):
        super().__init__()
        self.bot: Optional[commands.Bot] = bot
        self.games: Dict[int, TicTacToeGame] = {}  # Maps channel_id to TicTacToeGame
        self.cache: Optional[MasterCache] = None

        # DB/Cache lifecycle (moved from previous TicTacToeCog)
        self.db_client: Optional[AsyncIOMotorClient] = None
        self.state = None
        self.leaderboard = None
        self.ttt_cache: Optional[MasterCache] = None
        self.known_channel_ids: set[int] = set()

        # Queue for delayed cleanup tasks
        self.cleanup_queue: Dict[int, asyncio.Task] = {}

    def _get_cache(self) -> Optional[MasterCache]:
        """Get cache with logging for diagnostics."""
        cache = self.cache or master_cache

        # Fallback: read from bot.ttt_cache if available
        if cache is None:
            try:
                bot_cache = getattr(bot, "ttt_cache", None)
                if bot_cache is not None:
                    cache = bot_cache
                    # Wire it back so we don't need to fall back again
                    self.cache = bot_cache
            except Exception:
                # If anything goes wrong, keep the cache as None and continue
                pass

        # Auto-heal the module-global master_cache if we have a cache instance
        if cache is not None and master_cache is None:
            try:
                # Prefer using the provided setter to keep both global and manager wired
                set_tictactoe_cache(cache)
            except Exception:
                # If healing fails, just proceed; we'll still return a usable cache
                pass

        logger.debug(
            f"_get_cache: self.cache={self.cache is not None}, master_cache={master_cache is not None}, "
            f"bot_cache={'yes' if getattr(globals().get('bot', object()), 'ttt_cache', None) else 'no'}, "
            f"result={cache is not None}"
        )
        return cache

    async def _persist_new_game(self, game: TicTacToeGame):
        cache = self._get_cache()
        if not cache:
            logger.warning("MasterCache not set; cannot persist new game")
            return
        await cache.replace_state(game.channel_id, game.to_state())

    async def _persist_update(self, game: TicTacToeGame):
        cache = self._get_cache()
        if not cache:
            logger.warning("MasterCache not set; cannot persist game update")
            return
        await cache.update_state(game.channel_id, game.to_state())

    async def _load_game_from_cache(self, channel_id: int) -> Optional[TicTacToeGame]:
        cache = self._get_cache()
        if not cache:
            return None
        try:
            state = await cache.get_state(channel_id)
            if not state:
                return None
            return TicTacToeGame.from_state(state)
        except Exception:
            return None

    async def _ensure_category(self, channel: discord.abc.GuildChannel) -> bool:
        try:
            cat_id = getattr(channel.category, "id", None)
        except Exception:
            cat_id = None
        return cat_id == config.game_category_id

    async def _ensure_discussion_thread(self, interaction: discord.Interaction, game: TicTacToeGame):
        """
        - Disallow normal chat in the channel (deny @everyone send_messages)
        - Ensure a discussion thread exists for the game; create if missing
        - Store thread_id and root_message_id in state
        """
        ch = interaction.channel
        guild = interaction.guild
        if not isinstance(ch, discord.TextChannel) or not guild:
            return

        # 1) Enforce no-chat in the channel
        try:
            default_role = guild.default_role
            overwrites = ch.overwrites_for(default_role)
            if overwrites.send_messages is not False:
                overwrites.send_messages = False
                await ch.set_permissions(default_role, overwrite=overwrites, reason="TicTacToe: Use thread for chat")
        except Exception as e:
            logger.warning(f"Failed to update channel permissions for no-chat in {ch.id}: {e}")

        # 2) Ensure a discussion thread
        # Try to use the message being interacted with as the root if possible
        root_message = None
        try:
            if interaction.message:
                root_message = interaction.message
                game.root_message_id = int(root_message.id)
        except Exception:
            root_message = None

        # Create a thread if missing
        if not game.thread_id:
            try:
                thread_name = f"ttt-discussion-{ch.id}"
                if root_message:
                    thread = await root_message.create_thread(name=thread_name, auto_archive_duration=60)
                else:
                    # Fallback to creating a standalone thread
                    thread = await ch.create_thread(
                        name=thread_name,
                        type=discord.ChannelType.public_thread,
                        auto_archive_duration=60
                    )
                game.thread_id = int(thread.id)
                await self._persist_update(game)

                # Drop a starter message
                try:
                    await thread.send("Thread created. Discuss the game here and keep the channel clean!")
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Failed to create discussion thread in channel {ch.id}: {e}")

    async def start_game(self, channel_id: int, difficulty: str, guild: discord.Guild = None,
                         game_name: str = None) -> TicTacToeGame:
        """Start a new game for the given channel."""
        cache = self._get_cache()
        logger.info(f"start_game called: channel={channel_id}, cache_available={cache is not None}")

        if cache is None:
            logger.error(
                f"MasterCache not configured; refusing to start game to prevent data loss. self.cache={self.cache}, master_cache={master_cache}")
            raise RuntimeError("Tic-Tac-Toe storage is not ready. Please try again shortly.")

        # For new channel creation, we won't check if the game exists in channel_id since it's a new channel
        if channel_id in self.games:
            raise ValueError("A game is already active in this channel.")

        # If guild is provided, create a new channel in the game category
        if guild:
            try:
                category = guild.get_channel(config.game_category_id)
                if not category or not isinstance(category, discord.CategoryChannel):
                    raise ValueError("Game category not found or invalid.")

                # Create a new channel name
                if not game_name:
                    game_name = f"tictactoe-{difficulty}-{discord.utils.utcnow().strftime('%Y%m%d-%H%M%S')}"

                # Create the new channel
                new_channel = await guild.create_text_channel(
                    name=game_name,
                    category=category,
                    reason="TicTacToe: New game channel",
                    overwrites={
                        guild.default_role: discord.PermissionOverwrite(send_messages=False)
                    }
                )

                # Update channel_id to the new channel
                channel_id = new_channel.id
                logger.info(f"Created new game channel: {new_channel.name} ({channel_id})")

            except Exception as e:
                logger.error(f"Failed to create new game channel: {e}")
                raise RuntimeError("Failed to create new game channel.")

        game = TicTacToeGame(channel_id, difficulty)
        # Track guild for later cleanup resolution
        try:
            if guild:
                game.guild_id = int(guild.id)
        except Exception:
            pass

        self.games[channel_id] = game
        await self._persist_new_game(game)
        logger.info(f"Game started in channel {channel_id}.")
        return game

    async def _create_game_thread(self, channel: discord.TextChannel, game: TicTacToeGame, thread_name: str = None,
                                  private: bool = False) -> discord.Thread:
        """Create a discussion thread for the game in the specified channel."""
        try:
            if not thread_name:
                thread_name = f"ttt-discussion-{channel.id}"

            if private:
                thread = await channel.create_thread(
                    name=thread_name,
                    auto_archive_duration=60,
                    type=discord.ChannelType.private_thread,
                    invitable=True,
                    reason="TicTacToe: Create private discussion thread",
                )
            else:
                # For new channels, create a standalone public thread
                thread = await channel.create_thread(
                    name=thread_name,
                    type=discord.ChannelType.public_thread,
                    auto_archive_duration=60,
                    reason="TicTacToe: Create public discussion thread",
                )

            # Store thread details in the game state
            game.thread_id = int(thread.id)
            await self._persist_update(game)

            # Send a welcome message to the thread
            await thread.send("Discussion thread created. Discuss the game here and keep the channel clean!")

            logger.info(f"Created discussion thread: {thread.name} ({thread.id}) in channel {channel.id}")
            return thread

        except Exception as e:
            logger.error(f"Failed to create discussion thread in channel {channel.id}: {e}")
            raise

    async def _resolve_channel(self, channel_id: int, guild_id: Optional[int] = None) -> Optional[discord.abc.GuildChannel]:
        """Robustly resolve a channel by ID using multiple strategies."""
        ch: Optional[discord.abc.GuildChannel] = None
        # Prefer the cog's bot, but fall back to the globally imported bot if needed
        candidate_bot: Optional[commands.Bot] = self.bot
        try:
            if candidate_bot is None:
                try:
                    candidate_bot = globals().get("bot", None)  # type: ignore[assignment]
                except Exception:
                    candidate_bot = None

            if candidate_bot:
                # Local cache lookup
                ch = candidate_bot.get_channel(channel_id)  # type: ignore[attr-defined]
                if ch:
                    return ch

                # Try per-guild cache if we know the guild
                if guild_id:
                    g = candidate_bot.get_guild(int(guild_id))  # type: ignore[attr-defined]
                    if g:
                        ch = g.get_channel(channel_id)
                        if ch:
                            return ch

                # Fetch from API (requires View Channel perm)
                try:
                    ch = await candidate_bot.fetch_channel(channel_id)  # type: ignore[attr-defined]
                    if ch:
                        return ch
                except Exception as e:
                    logger.warning(f"fetch_channel failed for {channel_id}: {e}")

                # Try guild-scoped fetch if we know guild
                if guild_id:
                    g = candidate_bot.get_guild(int(guild_id))  # type: ignore[attr-defined]
                    if g:
                        try:
                            ch = await g.fetch_channel(channel_id)  # type: ignore[attr-defined]
                            if ch:
                                return ch
                        except Exception as e:
                            logger.warning(f"guild.fetch_channel failed for {channel_id}: {e}")
            else:
                logger.warning(f"_resolve_channel: No bot instance available to resolve channel {channel_id}")
        except Exception as e:
            logger.error(f"_resolve_channel unexpected error for {channel_id}: {e}", exc_info=True)
        return None

    async def _schedule_cleanup(self, channel_id: int, delay_seconds: int = 30):
        """Schedule a cleanup task to run after a delay."""
        # Cancel any existing cleanup task for this channel
        if channel_id in self.cleanup_queue:
            self.cleanup_queue[channel_id].cancel()

        async def delayed_cleanup():
            try:
                logger.info(f"Starting delayed cleanup for channel {channel_id}")
                await asyncio.sleep(delay_seconds)
                await self.cleanup_game_and_channel(channel_id, delete_channel=True)
                logger.info(f"Completed delayed cleanup for channel {channel_id}")
            except asyncio.CancelledError:
                logger.info(f"Cleanup task for channel {channel_id} was cancelled")
                raise
            except Exception as e:
                logger.error(f"Error during delayed cleanup for channel {channel_id}: {e}")
            finally:
                # Remove from queue
                self.cleanup_queue.pop(channel_id, None)

        # Create and store the cleanup task
        task = asyncio.create_task(delayed_cleanup())
        self.cleanup_queue[channel_id] = task
        logger.info(f"Scheduled cleanup for channel {channel_id} in {delay_seconds} seconds")

    async def cleanup_game_and_channel(self, channel_id: int, delete_channel: bool = True):
        """Remove a game and optionally delete the channel if it was created for the game."""
        try:
            logger.info(f"Starting cleanup_game_and_channel for {channel_id}")

            # Resolve channel robustly (use stored guild_id if available)
            game = self.games.get(channel_id)
            guild_id = getattr(game, "guild_id", None)
            channel = await self._resolve_channel(channel_id, guild_id=guild_id)

            # First, purge game state from cache and DB
            logger.info(f"Purging game state for channel {channel_id}")
            await self._purge_game_state(channel_id)

            # Then cleanup in-memory game
            logger.info(f"Cleaning up in-memory game for channel {channel_id}")
            await self.cleanup_game(channel_id)

            # Delete the channel if requested and it exists
            if delete_channel:
                if isinstance(channel, discord.TextChannel):
                    # Check if this channel was created for a game (has the right category and naming pattern)
                    if (hasattr(channel, 'category') and
                            channel.category and
                            channel.category.id == config.game_category_id and
                            channel.name.startswith('tictactoe-')):

                        try:
                            logger.info(f"Attempting to delete game channel: {channel.name} ({channel_id})")
                            await channel.delete(reason="TicTacToe: Game ended, cleaning up channel")
                            logger.info(f"Successfully deleted game channel: {channel.name} ({channel_id})")
                        except Exception as e:
                            logger.error(f"Failed to delete game channel {channel_id}: {e}")
                    else:
                        logger.info(f"Channel {channel_id} doesn't match deletion criteria - skipping channel deletion")
                else:
                    # As a last resort, try HTTP delete if we have permissions and only the ID
                    candidate_bot: Optional[commands.Bot] = self.bot
                    if candidate_bot is None:
                        try:
                            candidate_bot = globals().get("bot", None)  # type: ignore[assignment]
                        except Exception:
                            candidate_bot = None

                    if candidate_bot and hasattr(candidate_bot, "http"):
                        try:
                            logger.info(f"Attempting HTTP delete for channel {channel_id}")
                            await candidate_bot.http.delete_channel(channel_id)  # type: ignore[attr-defined]
                            logger.info(f"HTTP delete issued for channel {channel_id}")
                        except Exception as e:
                            logger.warning(f"HTTP delete failed for channel {channel_id}: {e}")
                    else:
                        logger.warning(f"Channel {channel_id} could not be resolved and no HTTP client available for deletion")
            else:
                logger.info(f"Channel deletion disabled for {channel_id}")

        except Exception as e:
            logger.error(f"Error during game and channel cleanup for {channel_id}: {e}", exc_info=True)

    async def get_game(self, channel_id: int) -> Optional[TicTacToeGame]:
        """Retrieve a game by its channel ID, loading from cache if needed."""
        game = self.games.get(channel_id)
        if game:
            return game
        # Attempt to hydrate from MasterCache
        hydrated = await self._load_game_from_cache(channel_id)
        if hydrated:
            self.games[channel_id] = hydrated
        return hydrated

    async def cleanup_game(self, channel_id: int):
        """Remove a game from the active game list."""
        if channel_id in self.games:
            del self.games[channel_id]
            logger.info(f"Game in channel {channel_id} has been cleaned up from memory.")

    async def check_game_status(self, interaction: discord.Interaction, game: TicTacToeGame):
        """Check the current game status for a win or tie and handle outcomes."""
        winner = game.check_for_winner()
        if winner:
            await self._handle_winner(interaction, game)
            return

        if game.is_full():
            await self._handle_tie(interaction, game)

        # Persist ongoing state after status check
        await self._persist_update(game)

    async def _handle_winner(self, interaction: discord.Interaction, game: TicTacToeGame):
        winner_user = None
        loser_user = None

        # The winner symbol was set; the "turn" has already flipped after the last move,
        # so the winner is the previous player (1 - game.turn)
        winner_id = game.players[1 - game.turn]
        loser_id = game.players[game.turn]

        if interaction.guild:
            winner_user = interaction.guild.get_member(winner_id) if winner_id else None
            loser_user = interaction.guild.get_member(loser_id) if loser_id else None

        if not winner_user and winner_id:
            winner_user = interaction.user if interaction.user.id == winner_id else None

        # Update Leaderboard using MasterCache (wins/losses)
        if winner_id:
            await _record_win(winner_id)
        if loser_id:
            await _record_loss(loser_id)

        embed = discord.Embed(
            title="Game Over: We Have a Winner! 🎉",
            description=f"{winner_user.mention if winner_user else f'<@{winner_id}>'} wins by getting three in a row!\n\n{game.format_board()}\n\n*This channel will be deleted in 30 seconds.*",
            color=discord.Color.gold(),
        )
        view = await make_view(game, disable_buttons=True)

        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
            else:
                await interaction.followup.send(embed=embed, view=view, ephemeral=False)
        except Exception as e:
            logger.error(f"Failed to send winner embed: {e}")

        # Also notify discussion thread if exists
        try:
            if game.thread_id and interaction.guild:
                thread = interaction.guild.get_thread(game.thread_id)
                if thread:
                    await thread.send(
                        f"Winner: {winner_user.mention if winner_user else f'<@{winner_id}>'} 🎉\n*Channel will be deleted shortly.*")
        except Exception:
            pass

        # Post a match result embed to the designated feed channel (include final board, no channel link)
        try:
            guild_id = game.guild_id or (interaction.guild.id if interaction.guild else None)
            feed_channel = await self._resolve_channel(config.win_feed_channel_id, guild_id=guild_id) if guild_id else None
            if isinstance(feed_channel, discord.TextChannel):
                winner_mention = winner_user.mention if winner_user else (f"<@{winner_id}>" if winner_id else "Unknown")
                loser_mention = (
                    loser_user.mention if loser_user else (f"<@{loser_id}>" if loser_id else "Unknown")
                )
                feed_embed = discord.Embed(
                    title="Tic-Tac-Toe: Match Result",
                    description=f"{winner_mention} won🎉\n"
                                f"{loser_mention}, better luck next time\n\n"
                                f"{game.format_board()}",
                    color=discord.Color.gold(),
                )
                # Keep non-link context like difficulty
                try:
                    if game.difficulty:
                        feed_embed.add_field(
                            name="Difficulty",
                            value="3x3" if game.difficulty == "easy" else "5x5",
                            inline=True
                        )
                except Exception:
                    pass

                await feed_channel.send(embed=feed_embed)
            else:
                logger.warning(f"Win feed channel {config.win_feed_channel_id} could not be resolved")
        except Exception as e:
            logger.warning(f"Failed to send win feed embed to channel {config.win_feed_channel_id}: {e}")

        # Schedule cleanup in the background (don't block interaction)
        logger.info(f"Scheduling cleanup for winner game in channel {game.channel_id}")
        await self._schedule_cleanup(game.channel_id, 30)


    async def _handle_tie(self, interaction: discord.Interaction, game: TicTacToeGame):
        # Update Leaderboard for both players (ties)
        for player_id in game.players:
            if player_id:
                await _record_tie(player_id)

        embed = discord.Embed(
            title="Tic-Tac-Toe",
            description=f"It's a tie! 🤝\n\n"
                        f"{game.format_board()}\n\n"
                        f"*This channel will be deleted in 30 seconds.*",
            color=discord.Color.gold(),
        )
        view = await make_view(game, disable_buttons=True)

        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
            else:
                await interaction.followup.send(embed=embed, view=view, ephemeral=False)
        except Exception as e:
            logger.error(f"Failed to handle tie: {e}")

        # Notify discussion thread
        try:
            if game.thread_id and interaction.guild:
                thread = interaction.guild.get_thread(game.thread_id)
                if thread:
                    await thread.send("Game ended in a tie 🤝\n*Channel will be deleted shortly.*")
        except Exception:
            pass

        # Post a tie result embed to the designated feed channel (include final board, no channel link)
        try:
            p1_id, p2_id = (game.players + [None, None])[:2]
            p1_user = interaction.guild.get_member(p1_id) if interaction.guild and p1_id else None
            p2_user = interaction.guild.get_member(p2_id) if interaction.guild and p2_id else None

            p1_mention = p1_user.mention if p1_user else (f"<@{p1_id}>" if p1_id else "Unknown")
            p2_mention = p2_user.mention if p2_user else (f"<@{p2_id}>" if p2_id else "Unknown")

            guild_id = game.guild_id or (interaction.guild.id if interaction.guild else None)
            feed_channel = await self._resolve_channel(config.win_feed_channel_id, guild_id=guild_id) if guild_id else None
            if isinstance(feed_channel, discord.TextChannel):
                feed_embed = discord.Embed(
                    title="Tic-Tac-Toe: Match Tied",
                    description=f"{p1_mention} & {p2_mention} tied 🤝\n\n"
                                f"{game.format_board()}",
                    color=discord.Color.blurple(),
                )
                # Keep non-link context like difficulty
                try:
                    if game.difficulty:
                        feed_embed.add_field(
                            name="Difficulty",
                            value="3x3" if game.difficulty == "easy" else "5x5",
                            inline=True
                        )
                except Exception:
                    pass

                await feed_channel.send(embed=feed_embed)
            else:
                logger.warning(f"Tie feed channel {config.win_feed_channel_id} could not be resolved")
        except Exception as e:
            logger.warning(f"Failed to send tie feed embed to channel {config.win_feed_channel_id}: {e}")

        # Schedule cleanup in the background (don't block interaction)
        logger.info(f"Scheduling cleanup for tie game in channel {game.channel_id}")
        await self._schedule_cleanup(game.channel_id, 30)

    async def cleanup_inactive_games(self):
        """Periodic cleanup for inactive games."""
        now = discord.utils.utcnow()
        inactive_games = [
            channel_id for channel_id, game in self.games.items()
            if (now - game.last_interaction) > self.INACTIVITY_TIMEOUT
        ]
        for channel_id in inactive_games:
            await self.cleanup_game(channel_id)

    async def _purge_game_state(self, channel_id: int):
        """
        Hard-delete the game's document from the database and clean up memory/cache.
        Evict from cache first to prevent any subsequent flush from re-creating the document.
        """
        logger.info(f"Starting _purge_game_state for channel {channel_id}")

        # 1) Evict from cache (best-effort) to avoid re-flush resurrecting the document
        cache = self._get_cache()
        try:
            if cache is not None:
                # If MasterCache exposes internals, clear them explicitly
                try:
                    getattr(cache, "_state_cache", {}).pop(channel_id, None)  # type: ignore[arg-type]
                    getattr(cache, "_state_meta", {}).pop(channel_id, None)   # type: ignore[arg-type]
                    dirty: set = getattr(cache, "_dirty_states", set())
                    if channel_id in dirty:
                        dirty.discard(channel_id)
                    logger.info(f"Evicted channel {channel_id} from MasterCache in-memory state")
                except Exception as e:
                    logger.debug(f"Direct cache eviction failed for {channel_id}: {e}")

                if hasattr(cache, "delete_state"):
                    try:
                        await cache.delete_state(channel_id)  # type: ignore[attr-defined]
                        logger.info(f"Called cache.delete_state for channel {channel_id}")
                    except Exception as e:
                        logger.debug(f"cache.delete_state raised for {channel_id}: {e}")
            else:
                logger.warning(f"No cache available for purging channel {channel_id}")
        except Exception as e:
            logger.error(f"Failed to evict state from cache for channel {channel_id}: {e}", exc_info=True)

        # 2) Delete from DB (one document per channel id) using an available collection
        try:
            collection = None
            if self.state is not None:
                collection = self.state
            elif cache is not None and hasattr(cache, "state_collection"):
                collection = getattr(cache, "state_collection")

            if collection is not None:
                logger.info(f"Attempting to delete DB document for channel {channel_id}")
                result = await collection.delete_one({"_id": str(channel_id)})
                if result and getattr(result, "deleted_count", 0) > 0:
                    logger.info(f"Successfully deleted TicTacToe entry for channel {channel_id} from DB")
                else:
                    logger.info(f"No TicTacToe entry found in DB to delete for channel {channel_id}")
            else:
                logger.warning(f"No state collection available for deleting channel {channel_id}")
        except Exception as e:
            logger.error(f"Failed to delete game document from DB for channel {channel_id}: {e}", exc_info=True)

        # 3) Cleanup in-memory references
        try:
            self.games.pop(channel_id, None)
            self.known_channel_ids.discard(int(channel_id))
            logger.info(f"Cleaned up in-memory references for channel {channel_id}")
        except Exception as e:
            logger.error(f"Failed to cleanup in-memory references for channel {channel_id}: {e}")

    async def cog_load(self):
        logger.info("TicTacToeGameManager cog_load() starting...")
        startup = {
            "mongo_connections": 0,
            "collections_bound": 0,
            "cache_started": False,
            "states_preloaded": 0,
            "hydrated_games": 0,
        }

        try:
            # Database connections
            logger.info("Connecting to MongoDB...")
            self.db_client = AsyncIOMotorClient(MONGO_URI)
            logger.info("✅ MongoDB client(s) initialized")

            # Bind collections for TicTacToe
            self.state = self.db_client["Game-State"]["TicTacToe"]
            self.leaderboard = self.db_client["LeaderBoard"]["Tic-Tac-Toe"]
            startup["collections_bound"] = 2
            logger.info("✅ Collections bound (state, leaderboard)")

            # Start MasterCache
            logger.info("Starting MasterCache for TicTacToe...")
            with PerformanceLogger(logger, "MasterCache_initialization_TTT"):
                self.ttt_cache = MasterCache(
                    state_collection=self.state,
                    leaderboard_collection=self.leaderboard,
                    flush_interval=10.0,
                    state_ttl=5.0,
                    leaderboard_ttl=5.0,
                )
                await self.ttt_cache.start()
                startup["cache_started"] = True
            logger.info("✅ MasterCache started")

            # Wire cache into the module so the manager uses it
            set_tictactoe_cache(self.ttt_cache)

            # Set it in the bot as well (if bot is available)
            if self.bot:
                self.bot.ttt_cache = self.ttt_cache  # type: ignore[attr-defined]

            logger.info(
                f"Cache verification after set_tictactoe_cache: manager.cache={tictactoe_game_manager.cache is not None}, global={master_cache is not None}"
            )

            # Discover channel ids from a state collection if not provided
            if not self.known_channel_ids:
                try:
                    logger.info("Discovering unfinished TicTacToe games from DB...")
                    cursor = self.state.find({}, {"_id": 1})
                    channel_ids: list[int] = []
                    async for doc in cursor:
                        try:
                            channel_ids.append(int(doc.get("_id", "0")))
                        except Exception:
                            pass
                    self.known_channel_ids = set([cid for cid in channel_ids if cid])
                    logger.info(f"Found {len(self.known_channel_ids)} channels with persisted games")
                except Exception as e:
                    logger.error(f"Failed to discover channel ids: {e}", exc_info=True)

            # Preload states and leaderboard into a cache
            try:
                if self.known_channel_ids:
                    await self.ttt_cache.preload_states(self.known_channel_ids)
                    startup["states_preloaded"] = len(self.known_channel_ids)
                await self.ttt_cache.preload_leaderboard()
                logger.info("✅ Cache preloaded (states/leaderboard)")
            except Exception as e:
                logger.error(f"Failed to preload cache: {e}", exc_info=True)

            # Hydrate unfinished games into the manager memory for faster access
            hydrated = 0
            for cid in self.known_channel_ids:
                try:
                    game = await self.get_game(cid)
                    if game:
                        hydrated += 1
                except Exception:
                    pass
            startup["hydrated_games"] = hydrated
            if hydrated:
                logger.info(f"✅ Hydrated {hydrated} unfinished TicTacToe game(s) into memory")

            logger.info(f"🎉 TicTacToeGameManager cog loaded successfully. Final cache check: {self._get_cache() is not None}")
            logger.info(f"Startup summary: {startup}")

        except Exception as e:
            logger.critical(f"CRITICAL: Failed to initialize TicTacToeGameManager cog: {e}", exc_info=True)
            logger.error(f"Startup summary (failed): {startup}")
            raise

    async def cog_unload(self):
        logger.info("TicTacToeGameManager cog_unload() starting...")
        try:
            # Cancel all pending cleanup tasks
            for channel_id, task in self.cleanup_queue.items():
                if not task.done():
                    task.cancel()
                    logger.info(f"Cancelled cleanup task for channel {channel_id}")
            self.cleanup_queue.clear()

            # Shutdown cache
            if self.ttt_cache:
                logger.info("Shutting down MasterCache for TicTacToe...")
                await self.ttt_cache.shutdown()
                logger.info("✅ MasterCache shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down MasterCache: {e}", exc_info=True)
        finally:
            # Close DB connections
            if self.db_client:
                self.db_client.close()
            logger.info("🏁 TicTacToeGameManager cog unloaded.")


class TicTacToeButton(Button):
    def __init__(self, x, y):
        super().__init__(style=discord.ButtonStyle.secondary, label=" ", row=y)
        self.x = x
        self.y = y

    async def callback(self, interaction: discord.Interaction):
        # Enforce that the game runs only under the configured category
        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel) or not await tictactoe_game_manager._ensure_category(ch):
            await interaction.response.send_message(
                "This game can only be played in the designated games category.", ephemeral=True
            )
            return

        game = await tictactoe_game_manager.get_game(interaction.channel.id)
        if not game:
            await interaction.response.send_message("No active game in this channel.", ephemeral=True)
            return

        # Ensure we persist guild_id for reliable cleanup later
        try:
            if interaction.guild and not game.guild_id:
                game.guild_id = int(interaction.guild.id)
                await tictactoe_game_manager._persist_update(game)
        except Exception:
            pass

        # Ensure a discussion thread and disable channel chat
        await tictactoe_game_manager._ensure_discussion_thread(interaction, game)

        if game.winner:
            await interaction.response.send_message("The game is already over! 🎉", ephemeral=True)
            return

        if interaction.user.id != game.players[game.turn]:
            await interaction.response.send_message("It's not your turn to play.", ephemeral=True)
            return

        if game.board[self.y][self.x] != " ":
            await interaction.response.send_message("This spot is already taken!", ephemeral=True)
            return

        if game.place_move(game.turn, self.x, self.y):
            logger.info(f"Player {interaction.user.id} placed their move at ({self.x}, {self.y})")
        else:
            await interaction.response.send_message("Invalid move!", ephemeral=True)
            return

        # Persist state after the move
        await tictactoe_game_manager._persist_update(game)

        embed = discord.Embed(
            title="Tic-Tac-Toe",
            description=game.format_board_with_turn(interaction.guild.members),
            color=discord.Color.blue(),
        )
        view = await make_view(game)

        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
            else:
                # If already responded, try to send an update in the discussion thread
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
            logger.error("Failed to edit the interaction; it was already responded to.")
        finally:
            await tictactoe_game_manager.check_game_status(interaction, game)


async def make_view(game: TicTacToeGame, disable_buttons=False, members=None) -> View:
    """
    Generate a view with buttons representing the game board.
    Includes member information for turn tracking.
    """
    view = View()
    for y, row in enumerate(game.board):
        for x, value in enumerate(row):
            button = TicTacToeButton(x, y)
            button.label = value if value != " " else "•"
            button.disabled = disable_buttons or value != " "
            view.add_item(button)
    return view


# Global instance of the game manager (now also a Cog)
tictactoe_game_manager = TicTacToeGameManager()


# ---------------- Setup to register the Cog ----------------

async def setup(bot: commands.Bot):
    # Ensure the global manager knows about the bot before being added as a Cog
    tictactoe_game_manager.bot = bot
    await bot.add_cog(tictactoe_game_manager)