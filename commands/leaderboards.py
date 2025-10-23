# Python
from typing import Optional, Dict, List, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from commands.games.MasterCache import MasterCache
from utilities.logger_setup import get_simple_logger, log_performance, log_context

logger = get_simple_logger("leaderboards")


def _paginate(items: List[Tuple[str, int]], page: int, page_size: int = 10) -> List[Tuple[str, int]]:
    start = (page - 1) * page_size
    end = start + page_size
    return items[start:end]


def _build_embed(
    page_items: List[Tuple[str, int]],
    guild: Optional[discord.Guild],
    page: int,
    total_pages: int,
) -> discord.Embed:
    lines: List[str] = []
    base_index = (page - 1) * len(page_items)
    for idx, (user_id_str, score) in enumerate(page_items, start=1):
        uid = int(user_id_str)
        display = f"<@{uid}>"
        if guild:
            member = guild.get_member(uid)
            if member:
                display = member.mention
        lines.append(f"{base_index + idx}. {display} — {score}")

    description = "\n".join(lines) if lines else "No entries on this page."
    embed = discord.Embed(
        title="Counting Leaderboard",
        description=description,
        color=discord.Color.gold(),
    )
    if total_pages > 1:
        embed.set_footer(text=f"Page {page}/{total_pages}")
    return embed


def _build_ttt_embed(
    page_items: List[Tuple[str, Dict[str, int]]],
    guild: Optional[discord.Guild],
    page: int,
    total_pages: int,
    sort_label: str,
) -> discord.Embed:
    lines: List[str] = []
    base_index = (page - 1) * len(page_items)
    for idx, (user_id_str, stats) in enumerate(page_items, start=1):
        uid = int(user_id_str)
        display = f"<@{uid}>"
        if guild:
            member = guild.get_member(uid)
            if member:
                display = member.mention
        wins = int(stats.get("wins", 0))
        losses = int(stats.get("losses", 0))
        ties = int(stats.get("ties", 0))
        played = wins + losses
        win_rate = (wins / played) if played > 0 else 0.0
        lines.append(f"{base_index + idx}. {display} — W {wins} | L {losses} | T {ties} | WR {win_rate:.0%}")

    description = "\n".join(lines) if lines else "No entries on this page."
    embed = discord.Embed(
        title="Tic-Tac-Toe Leaderboard",
        description=description,
        color=discord.Color.gold(),
    )
    if total_pages > 1:
        embed.set_footer(text=f"Sort: {sort_label} • Page {page}/{total_pages}")
    else:
        embed.set_footer(text=f"Sort: {sort_label}")
    return embed


class LeaderboardPaginator(discord.ui.View):
    def __init__(
        self,
        items: List[Tuple[str, int]],
        interaction: discord.Interaction,
        timeout: float = 60.0,
        page_size: int = 10,
    ):
        super().__init__(timeout=timeout)
        self.items = items
        self.page_size = page_size
        self.page = 1
        self.total_pages = max(1, (len(items) + page_size - 1) // page_size)
        self.guild = interaction.guild

        # Disable buttons initially if not needed
        self.prev_button.disabled = self.page <= 1
        self.next_button.disabled = self.page >= self.total_pages

    async def update_message(self, interaction: discord.Interaction):
        page_items = _paginate(self.items, self.page, self.page_size)
        embed = _build_embed(page_items, self.guild, self.page, self.total_pages)
        await interaction.response.edit_message(embed=embed, view=self)
        # Update buttons state
        self.prev_button.disabled = self.page <= 1
        self.next_button.disabled = self.page >= self.total_pages

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.page > 1:
            self.page -= 1
        await self.update_message(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.page < self.total_pages:
            self.page += 1
        await self.update_message(interaction)

    async def on_timeout(self):
        # Disable buttons on timeout to prevent further interaction
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True


class TTTLeaderboardPaginator(discord.ui.View):
    def __init__(
        self,
        items: List[Tuple[str, Dict[str, int]]],
        interaction: discord.Interaction,
        sort_label: str,
        timeout: float = 60.0,
        page_size: int = 10,
    ):
        super().__init__(timeout=timeout)
        self.items = items
        self.page_size = page_size
        self.page = 1
        self.total_pages = max(1, (len(items) + page_size - 1) // page_size)
        self.guild = interaction.guild
        self.sort_label = sort_label

        self.prev_button.disabled = self.page <= 1
        self.next_button.disabled = self.page >= self.total_pages

    async def update_message(self, interaction: discord.Interaction):
        start = (self.page - 1) * self.page_size
        end = start + self.page_size
        page_items = self.items[start:end]
        embed = _build_ttt_embed(page_items, self.guild, self.page, self.total_pages, self.sort_label)
        await interaction.response.edit_message(embed=embed, view=self)
        self.prev_button.disabled = self.page <= 1
        self.next_button.disabled = self.page >= self.total_pages

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.page > 1:
            self.page -= 1
        await self.update_message(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, _: discord.ui.Button):
        if self.page < self.total_pages:
            self.page += 1
        await self.update_message(interaction)

    async def on_timeout(self):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True


class Leaderboards(commands.Cog, name="Leaderboards"):
    """
    Slash commands for leaderboards.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        logger.debug("Leaderboards cog initialized")

    def _get_cache(self) -> Optional[MasterCache]:
        """
        Retrieve the MasterCache from the CountingGame cog.
        """
        counting_cog = self.bot.get_cog("CountingGame")
        if counting_cog and getattr(counting_cog, "cache", None):
            logger.debug("CountingGame cache found")
            return counting_cog.cache  # type: ignore[return-value]
        logger.warning("CountingGame cache not available")
        return None

    def _get_ttt_collection(self):
        """
        Retrieve the TicTacToe leaderboard collection from the TicTacToeGame cog.
        Avoid truthiness checks on Motor collections.
        """
        ttt_cog = self.bot.get_cog("TicTacToeGame")
        if ttt_cog is None:
            logger.warning("TicTacToeGame cog not available")
            return None
        if not hasattr(ttt_cog, "leaderboard"):
            logger.warning("TicTacToe leaderboard attribute not present on cog")
            return None
        lb = getattr(ttt_cog, "leaderboard")
        if lb is None:
            logger.warning("TicTacToe leaderboard collection is None")
            return None
        logger.debug("TicTacToe leaderboard collection found")
        return lb

    leaderboard = app_commands.Group(
        name="leaderboard",
        description="View leaderboards",
        guild_only=True,
    )

    @leaderboard.command(name="counting", description="Show the counting leaderboard")
    @app_commands.checks.cooldown(1, 3.0, key=lambda i: i.guild_id or i.user.id)
    @app_commands.describe(
        top="How many users to display (1-50)",
    )
    @log_performance("leaderboard.counting")
    async def counting(
            self,
            interaction: discord.Interaction,
            top: app_commands.Range[int, 1, 50] = 10,
    ):
        async def respond_error(msg: str):
            try:
                if interaction.response.is_done():
                    await interaction.edit_original_response(content=msg, embed=None, view=None)
                else:
                    await interaction.response.send_message(msg, ephemeral=True)
            except Exception:
                logger.exception("Failed to send error response")

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)
        except Exception:
            logger.debug("Defer failed or already responded")

        with log_context(logger, f"leaderboard.counting top={top}"):
            try:
                # Get the counting game cog instance
                counting_cog = interaction.client.get_cog("CountingGame")
                if not counting_cog:
                    logger.error("CountingGame cog not found")
                    await respond_error("Counting game is not available. Please try again later.")
                    return

                # Direct database query for top users - much more efficient!
                top_users = await counting_cog.get_top_users(limit=top)

                # Convert to the expected format: list of (user_id_str, count) tuples
                top_items = [(str(user_id), count) for user_id, count in top_users]

                logger.info("Fetched top %d users directly from database", len(top_items))

            except Exception:
                logger.exception("Failed to read leaderboard")
                await respond_error("Failed to read leaderboard. Please try again later.")
                return

            if not top_items:
                logger.info("Empty leaderboard")
                empty_msg = "The counting leaderboard is empty."
                if interaction.response.is_done():
                    await interaction.edit_original_response(content=empty_msg, embed=None, view=None)
                else:
                    await interaction.response.send_message(empty_msg, ephemeral=True)
                return

            logger.info("Preparing leaderboard response with top=%d", len(top_items))

            if len(top_items) <= 10:
                # Single page
                embed = _build_embed(top_items, interaction.guild, page=1, total_pages=1)
                if interaction.response.is_done():
                    await interaction.edit_original_response(content=None, embed=embed, view=None)
                else:
                    await interaction.response.send_message(embed=embed)
                logger.debug("Leaderboard response sent (single page)")
                return

            # Paged response
            view = LeaderboardPaginator(items=top_items, interaction=interaction, timeout=120.0, page_size=10)
            first_page_items = _paginate(top_items, page=1, page_size=10)
            embed = _build_embed(first_page_items, interaction.guild, page=1, total_pages=view.total_pages)

            if interaction.response.is_done():
                await interaction.edit_original_response(content=None, embed=embed, view=view)
            else:
                await interaction.response.send_message(embed=embed, view=view)

            logger.debug("Leaderboard response sent (paginated)")

    # ---------- TicTacToe Leaderboard ----------

    _TTT_SORT_OPTIONS: List[Tuple[str, str]] = [
        ("wins_desc", "Most Wins"),
        ("wins_asc", "Least Wins"),
        ("losses_desc", "Most Losses"),
        ("losses_asc", "Least Losses"),
        ("ties_desc", "Most Ties"),
        ("ties_asc", "Least Ties"),
        ("win_rate_desc", "Highest Win Rate"),
        ("win_rate_asc", "Lowest Win Rate"),
        ("net_wins_desc", "Best Net Wins (W-L)"),
        ("net_wins_asc", "Worst Net Wins (W-L)"),
    ]

    async def _ttt_sort_autocomplete(
        self,
        interaction: discord.Interaction,
        current: str,
    ) -> List[app_commands.Choice[str]]:
        # Autofill sorting options
        options = self._TTT_SORT_OPTIONS
        if current:
            options = [o for o in options if current.lower() in (o[0] + " " + o[1]).lower()]
        return [app_commands.Choice(name=label, value=key) for key, label in options[:25]]

    @leaderboard.command(name="tictactoe", description="Show the Tic-Tac-Toe leaderboards")
    @app_commands.checks.cooldown(1, 3.0, key=lambda i: i.guild_id or i.user.id)
    @app_commands.describe(
        top="How many users to display (1-50)",
        sort="Sort order (autocomplete supported)",
    )
    @app_commands.autocomplete(sort=_ttt_sort_autocomplete)
    @log_performance("leaderboard.tictactoe")
    async def tictactoe(
        self,
        interaction: discord.Interaction,
        top: app_commands.Range[int, 1, 50] = 10,
        sort: Optional[str] = "wins_desc",
    ):
        async def respond_error(msg: str):
            try:
                if interaction.response.is_done():
                    await interaction.edit_original_response(content=msg, embed=None, view=None)
                else:
                    await interaction.response.send_message(msg, ephemeral=True)
            except Exception:
                logger.exception("Failed to send error response")

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)
        except Exception:
            logger.debug("Defer failed or already responded")

        with log_context(logger, f"leaderboard.tictactoe top={top} sort={sort}"):
            collection = self._get_ttt_collection()
            # Motor Collection objects do not support truthiness; compare explicitly to None
            if collection is None:
                await respond_error("Tic-Tac-Toe leaderboard is not available yet. Please try again shortly.")
                return

            try:
                cursor = collection.find({}, {"_id": 1, "wins": 1, "losses": 1, "ties": 1})
                raw: Dict[str, Dict[str, int]] = {}
                async for doc in cursor:
                    uid = str(doc.get("_id"))
                    raw[uid] = {
                        "wins": int(doc.get("wins", 0)),
                        "losses": int(doc.get("losses", 0)),
                        "ties": int(doc.get("ties", 0)),
                    }
                logger.debug("Fetched TTT leaderboard entries: %d", len(raw))
            except Exception:
                logger.exception("Failed to read Tic-Tac-Toe leaderboard")
                await respond_error("Failed to read Tic-Tac-Toe leaderboard. Please try again later.")
                return

            if not raw:
                empty_msg = "The Tic-Tac-Toe leaderboard is empty."
                if interaction.response.is_done():
                    await interaction.edit_original_response(content=empty_msg, embed=None, view=None)
                else:
                    await interaction.response.send_message(empty_msg, ephemeral=True)
                return

            def key_funcs(k: str, v: Dict[str, int]):
                wins = int(v.get("wins", 0))
                losses = int(v.get("losses", 0))
                ties = int(v.get("ties", 0))
                played = wins + losses
                win_rate = (wins / played) if played > 0 else 0.0
                net = wins - losses
                mapping = {
                    "wins_desc": (-wins, losses, -ties, int(k)),
                    "wins_asc": (wins, losses, ties, int(k)),
                    "losses_desc": (-losses, -wins, -ties, int(k)),
                    "losses_asc": (losses, wins, ties, int(k)),
                    "ties_desc": (-ties, -wins, losses, int(k)),
                    "ties_asc": (ties, wins, losses, int(k)),
                    "win_rate_desc": (-win_rate, -wins, losses, int(k)),
                    "win_rate_asc": (win_rate, wins, losses, int(k)),
                    "net_wins_desc": (-(net), -wins, losses, int(k)),
                    "net_wins_asc": ((net), wins, losses, int(k)),
                }
                return mapping.get(sort or "wins_desc", mapping["wins_desc"])

            # Build and sort list
            items: List[Tuple[str, Dict[str, int]]] = list(raw.items())
            items.sort(key=lambda kv: key_funcs(kv[0], kv[1]))
            top_items = items[:top]

            # Single vs paged
            if len(top_items) <= 10:
                embed = _build_ttt_embed(
                    top_items,
                    interaction.guild,
                    page=1,
                    total_pages=1,
                    sort_label=dict(self._TTT_SORT_OPTIONS).get(sort or "wins_desc", "Most Wins"),
                )
                if interaction.response.is_done():
                    await interaction.edit_original_response(content=None, embed=embed, view=None)
                else:
                    await interaction.response.send_message(embed=embed)
                logger.debug("TTT Leaderboard response sent (single page)")
                return

            # Paged response
            sort_label = dict(self._TTT_SORT_OPTIONS).get(sort or "wins_desc", "Most Wins")
            view = TTTLeaderboardPaginator(
                items=top_items,
                interaction=interaction,
                sort_label=sort_label,
                timeout=120.0,
                page_size=10,
            )
            first_page = top_items[:10]
            embed = _build_ttt_embed(first_page, interaction.guild, page=1, total_pages=view.total_pages, sort_label=sort_label)

            if interaction.response.is_done():
                await interaction.edit_original_response(content=None, embed=embed, view=view)
            else:
                await interaction.response.send_message(embed=embed, view=view)

            logger.debug("TTT Leaderboard response sent (paginated)")


async def setup(bot: commands.Bot):
    await bot.add_cog(Leaderboards(bot))