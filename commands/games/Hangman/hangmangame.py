import os
import datetime
import asyncio
from typing import Optional, Dict, Any, List, Set

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

from commands.games.MasterCache import MasterCache
from storage.config_system import config
from utilities.bot import bot
from utilities.logger_setup import get_logger, PerformanceLogger

logger = get_logger("HangmanGameManager")

# ---------------- Environment ----------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_URI2 = os.getenv("MONGO_URI2")

# Global cache reference
master_cache: Optional[MasterCache] = None


def set_hangman_cache(cache: MasterCache):
    global master_cache
    master_cache = cache
    hangman_game_manager.cache = cache
    logger.info(
        f"Hangman MasterCache is set. Global: {master_cache is not None}, Manager: {hangman_game_manager.cache is not None}"
    )


def _now_ts() -> float:
    return discord.utils.utcnow().timestamp()


def _serialize_progress(progress: List[str]) -> List[str]:
    return [c for c in progress]


def _deserialize_progress(data: Any) -> List[str]:
    if not isinstance(data, list):
        return []
    return [str(c) for c in data]


async def _record_win(user_id: int):
    if not master_cache:
        logger.warning("MasterCache not set; skipping leaderboard update (win)")
        return
    try:
        await master_cache.leaderboard_collection.update_one(  # type: ignore[attr-defined]
            {"_id": str(user_id)},
            {
                "$inc": {"wins": 1},
                "$setOnInsert": {"losses": 0},
            },
            upsert=True,
        )
    except Exception as e:
        logger.error(f"Failed to update leaderboard wins for user {user_id}: {e}", exc_info=True)


async def _record_loss(user_id: int):
    if not master_cache:
        logger.warning("MasterCache not set; skipping leaderboard update (loss)")
        return
    try:
        await master_cache.leaderboard_collection.update_one(  # type: ignore[attr-defined]
            {"_id": str(user_id)},
            {
                "$inc": {"losses": 1},
                "$setOnInsert": {"wins": 0},
            },
            upsert=True,
        )
    except Exception as e:
        logger.error(f"Failed to update leaderboard losses for user {user_id}: {e}", exc_info=True)


class HangmanGame:
    """
    Represents a single Hangman game.
    One guesser (player) tries to guess the hidden word within max attempts.
    """

    DEFAULT_MAX_ATTEMPTS = 6

    def __init__(self, channel_id: int, word: str, max_attempts: int = DEFAULT_MAX_ATTEMPTS):
        self.channel_id = channel_id
        self.secret_word = word.lower()
        self.progress: List[str] = ["_" if ch.isalpha() else ch for ch in self.secret_word]
        self.guessed_letters: Set[str] = set()
        self.wrong_guesses: int = 0
        self.max_attempts = max_attempts
        self.player_id: Optional[int] = None
        self.winner: Optional[int] = None  # set to player_id if solved
        self.loser: Optional[int] = None   # set to player_id if failed
        self.last_interaction = discord.utils.utcnow()
        self.thread_id: Optional[int] = None
        self.root_message_id: Optional[int] = None
        self.guild_id: Optional[int] = None

    @classmethod
    def from_state(cls, state: Dict[str, Any]) -> "HangmanGame":
        channel_id = int(state.get("_id", "0"))
        word = str(state.get("secret_word", ""))
        max_attempts = int(state.get("max_attempts", cls.DEFAULT_MAX_ATTEMPTS))
        game = cls(channel_id, word, max_attempts=max_attempts)
        game.progress = _deserialize_progress(state.get("progress"))
        game.guessed_letters = set([str(x) for x in state.get("guessed_letters", [])])
        game.wrong_guesses = int(state.get("wrong_guesses", 0))
        game.player_id = int(state["player_id"]) if state.get("player_id") is not None else None
        game.winner = int(state["winner"]) if state.get("winner") is not None else None
        game.loser = int(state["loser"]) if state.get("loser") is not None else None
        _ = float(state.get("last_interaction_ts", _now_ts()))
        game.last_interaction = discord.utils.utcnow()
        tid = state.get("thread_id")
        game.thread_id = int(tid) if tid is not None else None
        rm = state.get("root_message_id")
        game.root_message_id = int(rm) if rm is not None else None
        gid = state.get("guild_id")
        game.guild_id = int(gid) if gid is not None else None
        return game

    def to_state(self) -> Dict[str, Any]:
        return {
            "_id": str(self.channel_id),
            "secret_word": self.secret_word,
            "progress": _serialize_progress(self.progress),
            "guessed_letters": sorted(list(self.guessed_letters)),
            "wrong_guesses": self.wrong_guesses,
            "max_attempts": self.max_attempts,
            "player_id": self.player_id,
            "winner": self.winner,
            "loser": self.loser,
            "last_interaction_ts": _now_ts(),
            "thread_id": self.thread_id,
            "root_message_id": self.root_message_id,
            "guild_id": self.guild_id,
        }

    def format_board(self) -> str:
        display_word = " ".join(self.progress)
        guessed = ", ".join(sorted(self.guessed_letters)) if self.guessed_letters else "None"
        remaining = max(0, self.max_attempts - self.wrong_guesses)
        return (
            "```\n"
            f"Word: {display_word}\n"
            f"Guessed: {guessed}\n"
            f"Wrong: {self.wrong_guesses}/{self.max_attempts} (Remaining: {remaining})\n"
            "```"
        )

    def is_solved(self) -> bool:
        return "_" not in self.progress

    def is_failed(self) -> bool:
        return self.wrong_guesses >= self.max_attempts

    def guess_letter(self, ch: str) -> bool:
        if not ch or not ch.isalpha() or len(ch) != 1:
            return False
        ch = ch.lower()
        if ch in self.guessed_letters:
            return False

        self.guessed_letters.add(ch)
        hit = False
        for idx, orig in enumerate(self.secret_word):
            if orig == ch:
                self.progress[idx] = ch
                hit = True

        if not hit:
            self.wrong_guesses += 1

        self.last_interaction = discord.utils.utcnow()
        return True


class HangmanGuessButton(Button):
    def __init__(self, letter: str):
        super().__init__(style=discord.ButtonStyle.secondary, label=letter.upper(), row=0)
        self.letter = letter.lower()

    async def callback(self, interaction: discord.Interaction):
        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel) or not await hangman_game_manager._ensure_category(ch):
            await interaction.response.send_message(
                "This game can only be played in the designated games category.", ephemeral=True
            )
            return

        game = await hangman_game_manager.get_game(interaction.channel.id)
        if not game:
            await interaction.response.send_message("No active hangman game in this channel.", ephemeral=True)
            return

        # Persist guild id for cleanup
        try:
            if interaction.guild and not game.guild_id:
                game.guild_id = int(interaction.guild.id)
                await hangman_game_manager._persist_update(game)
        except Exception:
            pass

        # Ensure discussion thread
        await hangman_game_manager._ensure_discussion_thread(interaction, game)

        # Only the registered player can guess
        if game.player_id and interaction.user.id != game.player_id:
            await interaction.response.send_message("Only the current player can make guesses.", ephemeral=True)
            return

        # If no player set yet, claim the seat
        if not game.player_id:
            game.player_id = interaction.user.id

        # If game already ended
        if game.winner or game.loser:
            await interaction.response.send_message("This hangman game has ended.", ephemeral=True)
            return

        valid = game.guess_letter(self.letter)
        if not valid:
            await interaction.response.send_message("Invalid or duplicate guess.", ephemeral=True)
            return

        # Persist after guess
        await hangman_game_manager._persist_update(game)

        # Check status and respond
        if game.is_solved():
            game.winner = game.player_id
            await hangman_game_manager._persist_update(game)
            await hangman_game_manager._handle_win(interaction, game)
            return

        if game.is_failed():
            game.loser = game.player_id
            await hangman_game_manager._persist_update(game)
            await hangman_game_manager._handle_loss(interaction, game)
            return

        embed = discord.Embed(
            title="Hangman",
            description=game.format_board(),
            color=discord.Color.blue(),
        )
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


async def make_view(game: HangmanGame, disable_inputs: bool = False) -> View:
    """
    Build a letter grid (A-Z) as buttons for guessing.
    Disable already guessed letters.
    """
    view = View()
    letters = [chr(c) for c in range(ord('a'), ord('z') + 1)]
    # Arrange in rows of up to 5-7 to avoid overflow; Discord max 25 components per view
    row = 0
    per_row = 7
    for i, letter in enumerate(letters):
        btn = HangmanGuessButton(letter)
        btn.disabled = disable_inputs or (letter in game.guessed_letters)
        # Discord's UI assigns rows automatically by attribute on Button; set row via init not exposed,
        # but we can rely on auto placement; to keep minimal, we won't force rows beyond defaults.
        view.add_item(btn)
        # Limit total buttons to 25 (Discord cap)
        if len(view.children) >= 25:
            break
    return view


class HangmanGameManager(commands.Cog, name="HangmanGame"):
    """
    Manages multiple Hangman games, including creation, state, and cleanup.
    """
    INACTIVITY_TIMEOUT = datetime.timedelta(seconds=15)

    def __init__(self, bot: Optional[commands.Bot] = None):
        super().__init__()
        self.bot: Optional[commands.Bot] = bot
        self.games: Dict[int, HangmanGame] = {}
        self.cache: Optional[MasterCache] = None

        # DB/Cache lifecycle
        self.db_client: Optional[AsyncIOMotorClient] = None
        self.db_client2: Optional[AsyncIOMotorClient] = None
        self.state = None
        self.leaderboard = None
        self.hm_cache: Optional[MasterCache] = None
        self.known_channel_ids: set[int] = set()

        # Delayed cleanup
        self.cleanup_queue: Dict[int, asyncio.Task] = {}

    def _get_cache(self) -> Optional[MasterCache]:
        cache = self.cache or master_cache
        if cache is None:
            try:
                bot_cache = getattr(bot, "hm_cache", None)
                if bot_cache is not None:
                    cache = bot_cache
                    self.cache = bot_cache
            except Exception:
                pass
        if cache is not None and master_cache is None:
            try:
                set_hangman_cache(cache)
            except Exception:
                pass
        return cache

    async def _persist_new_game(self, game: HangmanGame):
        cache = self._get_cache()
        if not cache:
            logger.warning("MasterCache not set; cannot persist new hangman game")
            return
        await cache.replace_state(game.channel_id, game.to_state())

    async def _persist_update(self, game: HangmanGame):
        cache = self._get_cache()
        if not cache:
            logger.warning("MasterCache not set; cannot persist hangman game update")
            return
        await cache.update_state(game.channel_id, game.to_state())

    async def _load_game_from_cache(self, channel_id: int) -> Optional[HangmanGame]:
        cache = self._get_cache()
        if not cache:
            return None
        try:
            state = await cache.get_state(channel_id)
            if not state:
                return None
            return HangmanGame.from_state(state)
        except Exception:
            return None

    async def _ensure_category(self, channel: discord.abc.GuildChannel) -> bool:
        try:
            cat_id = getattr(channel.category, "id", None)
        except Exception:
            cat_id = None
        return cat_id == config.game_category_id

    async def _ensure_discussion_thread(self, interaction: discord.Interaction, game: HangmanGame):
        ch = interaction.channel
        guild = interaction.guild
        if not isinstance(ch, discord.TextChannel) or not guild:
            return

        try:
            default_role = guild.default_role
            overwrites = ch.overwrites_for(default_role)
            if overwrites.send_messages is not False:
                overwrites.send_messages = False
                await ch.set_permissions(default_role, overwrite=overwrites, reason="Hangman: Use thread for chat")
        except Exception as e:
            logger.warning(f"Failed to update channel permissions for no-chat in {ch.id}: {e}")

        root_message = None
        try:
            if interaction.message:
                root_message = interaction.message
                game.root_message_id = int(root_message.id)
        except Exception:
            root_message = None

        if not game.thread_id:
            try:
                thread_name = f"hm-discussion-{ch.id}"
                if root_message:
                    thread = await root_message.create_thread(name=thread_name, auto_archive_duration=60)
                else:
                    thread = await ch.create_thread(
                        name=thread_name,
                        type=discord.ChannelType.public_thread,
                        auto_archive_duration=60
                    )
                game.thread_id = int(thread.id)
                await self._persist_update(game)
                try:
                    await thread.send("Thread created. Discuss the game here and keep the channel clean!")
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"Failed to create discussion thread in channel {ch.id}: {e}")

    async def start_game(
        self,
        channel_id: int,
        word: str,
        max_attempts: int = HangmanGame.DEFAULT_MAX_ATTEMPTS,
        guild: discord.Guild = None,
        game_name: str = None
    ) -> HangmanGame:
        cache = self._get_cache()
        logger.info(f"hangman.start_game called: channel={channel_id}, cache_available={cache is not None}")

        if cache is None:
            logger.error("MasterCache not configured; refusing to start hangman game.")
            raise RuntimeError("Hangman storage is not ready. Please try again shortly.")

        if channel_id in self.games:
            raise ValueError("A game is already active in this channel.")

        if guild:
            try:
                category = guild.get_channel(config.game_category_id)
                if not category or not isinstance(category, discord.CategoryChannel):
                    raise ValueError("Game category not found or invalid.")
                if not game_name:
                    game_name = f"hangman-{discord.utils.utcnow().strftime('%Y%m%d-%H%M%S')}"
                new_channel = await guild.create_text_channel(
                    name=game_name,
                    category=category,
                    reason="Hangman: New game channel",
                    overwrites={guild.default_role: discord.PermissionOverwrite(send_messages=False)}
                )
                channel_id = new_channel.id
                logger.info(f"Created new hangman game channel: {new_channel.name} ({channel_id})")
            except Exception as e:
                logger.error(f"Failed to create new hangman game channel: {e}")
                raise RuntimeError("Failed to create new game channel.")

        game = HangmanGame(channel_id, word, max_attempts=max_attempts)
        try:
            if guild:
                game.guild_id = int(guild.id)
        except Exception:
            pass

        self.games[channel_id] = game
        await self._persist_new_game(game)
        logger.info(f"Hangman game started in channel {channel_id}.")
        return game

    async def _resolve_channel(self, channel_id: int, guild_id: Optional[int] = None) -> Optional[discord.abc.GuildChannel]:
        ch: Optional[discord.abc.GuildChannel] = None
        candidate_bot: Optional[commands.Bot] = self.bot
        try:
            if candidate_bot is None:
                try:
                    candidate_bot = globals().get("bot", None)  # type: ignore[assignment]
                except Exception:
                    candidate_bot = None

            if candidate_bot:
                ch = candidate_bot.get_channel(channel_id)  # type: ignore[attr-defined]
                if ch:
                    return ch
                if guild_id:
                    g = candidate_bot.get_guild(int(guild_id))  # type: ignore[attr-defined]
                    if g:
                        ch = g.get_channel(channel_id)
                        if ch:
                            return ch
                try:
                    ch = await candidate_bot.fetch_channel(channel_id)  # type: ignore[attr-defined]
                    if ch:
                        return ch
                except Exception:
                    pass
                if guild_id:
                    g = candidate_bot.get_guild(int(guild_id))  # type: ignore[attr-defined]
                    if g:
                        try:
                            ch = await g.fetch_channel(channel_id)  # type: ignore[attr-defined]
                            if ch:
                                return ch
                        except Exception:
                            pass
        except Exception as e:
            logger.error(f"_resolve_channel unexpected error for {channel_id}: {e}", exc_info=True)
        return None

    async def _schedule_cleanup(self, channel_id: int, delay_seconds: int = 30):
        if channel_id in self.cleanup_queue:
            self.cleanup_queue[channel_id].cancel()

        async def delayed_cleanup():
            try:
                logger.info(f"Hangman delayed cleanup for channel {channel_id}")
                await asyncio.sleep(delay_seconds)
                await self.cleanup_game_and_channel(channel_id, delete_channel=True)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error during hangman delayed cleanup for channel {channel_id}: {e}")
            finally:
                self.cleanup_queue.pop(channel_id, None)

        task = asyncio.create_task(delayed_cleanup())
        self.cleanup_queue[channel_id] = task

    async def cleanup_game_and_channel(self, channel_id: int, delete_channel: bool = True):
        try:
            game = self.games.get(channel_id)
            guild_id = getattr(game, "guild_id", None)
            channel = await self._resolve_channel(channel_id, guild_id=guild_id)

            await self._purge_game_state(channel_id)
            await self.cleanup_game(channel_id)

            if delete_channel:
                if isinstance(channel, discord.TextChannel):
                    if (hasattr(channel, "category") and channel.category and
                            channel.category.id == config.game_category_id and
                            channel.name.startswith("hangman-")):
                        try:
                            await channel.delete(reason="Hangman: Game ended, cleaning up channel")
                        except Exception as e:
                            logger.error(f"Failed to delete hangman game channel {channel_id}: {e}")
                else:
                    candidate_bot: Optional[commands.Bot] = self.bot
                    if candidate_bot and hasattr(candidate_bot, "http"):
                        try:
                            await candidate_bot.http.delete_channel(channel_id)  # type: ignore[attr-defined]
                        except Exception:
                            pass
        except Exception as e:
            logger.error(f"Error during hangman game/channel cleanup for {channel_id}: {e}", exc_info=True)

    async def get_game(self, channel_id: int) -> Optional[HangmanGame]:
        game = self.games.get(channel_id)
        if game:
            return game
        hydrated = await self._load_game_from_cache(channel_id)
        if hydrated:
            self.games[channel_id] = hydrated
        return hydrated

    async def cleanup_game(self, channel_id: int):
        if channel_id in self.games:
            del self.games[channel_id]
            logger.info(f"Hangman game in channel {channel_id} cleaned from memory.")

    async def _handle_win(self, interaction: discord.Interaction, game: HangmanGame):
        winner_id = game.player_id
        if winner_id:
            await _record_win(winner_id)

        embed = discord.Embed(
            title="Hangman: You Win! 🎉",
            description=f"You guessed the word: `{game.secret_word}`\n\n{game.format_board()}\n\n*This channel will be deleted in 30 seconds.*",
            color=discord.Color.green(),
        )
        view = await make_view(game, disable_inputs=True)

        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
            else:
                await interaction.followup.send(embed=embed, view=view, ephemeral=False)
        except Exception as e:
            logger.error(f"Failed to send win embed: {e}")

        try:
            if game.thread_id and interaction.guild:
                thread = interaction.guild.get_thread(game.thread_id)
                if thread:
                    await thread.send(f"Winner: <@{winner_id}> 🎉\nThe word was `{game.secret_word}`.")
        except Exception:
            pass

        # Optional: post to a win feed channel (reusing config.win_feed_channel_id)
        try:
            guild_id = game.guild_id or (interaction.guild.id if interaction.guild else None)
            feed_channel = await self._resolve_channel(config.win_feed_channel_id, guild_id=guild_id) if guild_id else None
            if isinstance(feed_channel, discord.TextChannel):
                mention = f"<@{winner_id}>" if winner_id else "Unknown"
                feed_embed = discord.Embed(
                    title="Hangman: Match Result",
                    description=f"Winner: {mention} 🎉\nWord: `{game.secret_word}`\n\n{game.format_board()}",
                    color=discord.Color.green(),
                )
                await feed_channel.send(embed=feed_embed)
        except Exception:
            pass

        await self._schedule_cleanup(game.channel_id, 30)

    async def _handle_loss(self, interaction: discord.Interaction, game: HangmanGame):
        loser_id = game.player_id
        if loser_id:
            await _record_loss(loser_id)

        embed = discord.Embed(
            title="Hangman: You Lost 💀",
            description=f"The word was: `{game.secret_word}`\n\n{game.format_board()}\n\n*This channel will be deleted in 30 seconds.*",
            color=discord.Color.red(),
        )
        view = await make_view(game, disable_inputs=True)

        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
            else:
                await interaction.followup.send(embed=embed, view=view, ephemeral=False)
        except Exception as e:
            logger.error(f"Failed to send loss embed: {e}")

        try:
            if game.thread_id and interaction.guild:
                thread = interaction.guild.get_thread(game.thread_id)
                if thread:
                    await thread.send(f"Game over. <@{loser_id}> ran out of attempts. Word: `{game.secret_word}`.")
        except Exception:
            pass

        # Optional: post to a feed channel as well
        try:
            guild_id = game.guild_id or (interaction.guild.id if interaction.guild else None)
            feed_channel = await self._resolve_channel(config.win_feed_channel_id, guild_id=guild_id) if guild_id else None
            if isinstance(feed_channel, discord.TextChannel):
                mention = f"<@{loser_id}>" if loser_id else "Unknown"
                feed_embed = discord.Embed(
                    title="Hangman: Match Result",
                    description=f"{mention} failed to guess the word `{game.secret_word}` 💀\n\n{game.format_board()}",
                    color=discord.Color.red(),
                )
                await feed_channel.send(embed=feed_embed)
        except Exception:
            pass

        await self._schedule_cleanup(game.channel_id, 30)

    async def cleanup_inactive_games(self):
        now = discord.utils.utcnow()
        inactive = [
            channel_id for channel_id, game in self.games.items()
            if (now - game.last_interaction) > self.INACTIVITY_TIMEOUT
        ]
        for channel_id in inactive:
            await self.cleanup_game(channel_id)

    async def _purge_game_state(self, channel_id: int):
        logger.info(f"Hangman _purge_game_state for channel {channel_id}")
        cache = self._get_cache()
        try:
            if cache is not None:
                try:
                    getattr(cache, "_state_cache", {}).pop(channel_id, None)  # type: ignore[arg-type]
                    getattr(cache, "_state_meta", {}).pop(channel_id, None)   # type: ignore[arg-type]
                    dirty: set = getattr(cache, "_dirty_states", set())
                    if channel_id in dirty:
                        dirty.discard(channel_id)
                except Exception:
                    pass
                if hasattr(cache, "delete_state"):
                    try:
                        await cache.delete_state(channel_id)  # type: ignore[attr-defined]
                    except Exception:
                        pass
        except Exception as e:
            logger.error(f"Failed to evict hangman state from cache for {channel_id}: {e}", exc_info=True)

        try:
            collection = None
            if self.state is not None:
                collection = self.state
            elif cache is not None and hasattr(cache, "state_collection"):
                collection = getattr(cache, "state_collection")

            if collection is not None:
                try:
                    await collection.delete_one({"_id": str(channel_id)})
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Failed to delete hangman document from DB for {channel_id}: {e}", exc_info=True)

        try:
            self.games.pop(channel_id, None)
            self.known_channel_ids.discard(int(channel_id))
        except Exception:
            pass

    async def cog_load(self):
        logger.info("HangmanGameManager cog_load() starting...")
        startup = {
            "mongo_connections": 0,
            "collections_bound": 0,
            "cache_started": False,
            "states_preloaded": 0,
            "hydrated_games": 0,
        }

        try:
            logger.info("Connecting to MongoDB...")
            self.db_client = AsyncIOMotorClient(MONGO_URI)
            self.db_client2 = AsyncIOMotorClient(MONGO_URI2) if MONGO_URI2 else None
            startup["mongo_connections"] = 1 + (1 if self.db_client2 else 0)

            # Bind collections for Hangman
            self.state = self.db_client["Game-State"]["Hangman"]
            self.leaderboard = self.db_client["LeaderBoard"]["Hangman"]
            startup["collections_bound"] = 2

            logger.info("Starting MasterCache for Hangman...")
            with PerformanceLogger(logger, "MasterCache_initialization_HM"):
                self.hm_cache = MasterCache(
                    state_collection=self.state,
                    leaderboard_collection=self.leaderboard,
                    flush_interval=10.0,
                    state_ttl=5.0,
                    leaderboard_ttl=5.0,
                )
                await self.hm_cache.start()
                startup["cache_started"] = True

            set_hangman_cache(self.hm_cache)
            if self.bot:
                self.bot.hm_cache = self.hm_cache  # type: ignore[attr-defined]

            # Discover known channels
            if not self.known_channel_ids:
                try:
                    cursor = self.state.find({}, {"_id": 1})
                    channel_ids: list[int] = []
                    async for doc in cursor:
                        try:
                            channel_ids.append(int(doc.get("_id", "0")))
                        except Exception:
                            pass
                    self.known_channel_ids = set([cid for cid in channel_ids if cid])
                except Exception as e:
                    logger.error(f"Failed to discover hangman channel ids: {e}", exc_info=True)

            # Preload states/leaderboard
            try:
                if self.known_channel_ids:
                    await self.hm_cache.preload_states(self.known_channel_ids)
                    startup["states_preloaded"] = len(self.known_channel_ids)
                await self.hm_cache.preload_leaderboard()
            except Exception as e:
                logger.error(f"Failed to preload cache (hangman): {e}", exc_info=True)

            # Hydrate unfinished games
            hydrated = 0
            for cid in self.known_channel_ids:
                try:
                    game = await self.get_game(cid)
                    if game:
                        hydrated += 1
                except Exception:
                    pass
            startup["hydrated_games"] = hydrated

            logger.info(f"HangmanGameManager cog loaded. Startup summary: {startup}")

        except Exception as e:
            logger.critical(f"CRITICAL: Failed to initialize HangmanGameManager cog: {e}", exc_info=True)
            logger.error(f"Startup summary (failed): {startup}")
            raise

    async def cog_unload(self):
        logger.info("HangmanGameManager cog_unload() starting...")
        try:
            for channel_id, task in self.cleanup_queue.items():
                if not task.done():
                    task.cancel()
            self.cleanup_queue.clear()

            if self.hm_cache:
                await self.hm_cache.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down Hangman MasterCache: {e}", exc_info=True)
        finally:
            if self.db_client:
                self.db_client.close()
            if self.db_client2:
                self.db_client2.close()
            logger.info("HangmanGameManager cog unloaded.")


# Global instance
hangman_game_manager = HangmanGameManager()


# ---------------- Setup to register the Cog ----------------
async def setup(bot: commands.Bot):
    hangman_game_manager.bot = bot
    await bot.add_cog(hangman_game_manager)