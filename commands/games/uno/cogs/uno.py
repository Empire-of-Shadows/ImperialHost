import asyncio
from typing import Optional
import discord
from discord import app_commands
from discord.ext import commands
from discord.ext.commands import BucketType
from discord.ui import View
from commands.games.uno.core.game import UnoGameManager
from utilities.cooldown import (cooldown_enforcer, uno_play_cd, uno_start_cd, uno_quit_cd, uno_call_cd, uno_draw_cd,
                                uno_pass_cd, uno_declare_cd, uno_cancel_cd, uno_new_cd_user, uno_new_cd_guild)
from utilities.logger_setup import get_logger

logger = get_logger("UnoCog")

async def autocomplete_card(interaction: discord.Interaction, current: str):
    """
    Dynamic autocomplete for the /uno_play command based on the player's hand.
    """
    logger.info(f"Processing autocomplete for user {interaction.user} in channel {interaction.channel.id}.")
    await asyncio.sleep(0.5)
    game = UnoGameManager.uno_games.get(interaction.channel.id)
    if not game or not game.get("started", False):
        return []
    player = next((p for p in game["players"] if p.user == interaction.user), None)
    if not player:
        return []

    last_card = game.get("last_played_card", "")
    last_parts = last_card.split() if last_card else []
    last_color = last_parts[0] if len(last_parts) > 0 else None
    last_type = last_parts[1] if len(last_parts) > 1 else None

    if len(player.hand) >= 26:
        filtered_cards = [card for card in player.hand if (last_color and last_color in card) or (last_type and last_type in card)]
    else:
        filtered_cards = player.hand

    return [
        app_commands.Choice(name=card, value=card)
        for card in filtered_cards
        if current.lower() in card.lower()
    ][:25]


class UnoCog(commands.GroupCog, name="uno"):
    """Cog for managing UNO commands and interactions."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        super().__init__()

    @app_commands.command(name="new_game", description="Start a new UNO game.")
    @app_commands.check(cooldown_enforcer(uno_new_cd_user, BucketType)())
    async def new_game(self, interaction: discord.Interaction):
        """
        Start a new UNO game by selecting a game mode.
        """
        if interaction.channel.id != 1406642649997508758:
            await interaction.response.send_message("❌ This command can only be used in <#1406642649997508758>.", ephemeral=True)
            return

        class GameModeSelectView(View):
            def __init__(self):
                super().__init__(timeout=30)
                self.value: Optional[str] = None

            @discord.ui.select(
                placeholder="Choose a game mode...",
                options=[
                    discord.SelectOption(label="Text Thread", value="thread"),
                    discord.SelectOption(label="Voice Channel", value="voice"),
                ]
            )
            async def select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
                self.value = select.values[0]
                await interaction.response.defer()
                self.stop()

        view = GameModeSelectView()
        await interaction.response.send_message("Please select the game mode:", view=view, ephemeral=True)
        await view.wait()

        if not view.value:
            await interaction.followup.send("⏳ You did not select a game mode. Please try again.", ephemeral=True)
            return

        game_manager = UnoGameManager(self.bot)
        await game_manager.create_game(interaction, mode=view.value)

    @app_commands.command(name="start", description="Start the UNO game.")
    @app_commands.check(cooldown_enforcer(uno_start_cd, BucketType)())
    async def start(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await UnoGameManager.start_game(interaction)
        except Exception as e:
            logger.error(f"Error starting UNO game: {e}")
            await interaction.followup.send("An error occurred while starting the game. Please try again.", ephemeral=True)

    @app_commands.command(name="play", description="Play a card in UNO.")
    @app_commands.autocomplete(card=autocomplete_card)
    @app_commands.describe(card="The card you want to play.")
    @app_commands.check(cooldown_enforcer(uno_play_cd, BucketType)())
    async def play(self, interaction: discord.Interaction, card: str):
        try:
            await UnoGameManager.play_card(interaction, card)
        except ValueError as ve:
            await interaction.response.send_message(f"Invalid card: {ve}. Please choose a valid card.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error playing card {card}: {e}")
            await interaction.response.send_message("An error occurred while playing the card. Please try again.", ephemeral=True)

    @app_commands.command(name="draw", description="Draw a card from the deck.")
    @app_commands.check(cooldown_enforcer(uno_draw_cd, BucketType)())
    async def draw(self, interaction: discord.Interaction):
        try:
            await UnoGameManager.draw_card(interaction)
        except Exception as e:
            logger.error(f"Error drawing card: {e}")
            await interaction.response.send_message("An error occurred while drawing a card. Please try again.", ephemeral=True)

    @app_commands.command(name="pass", description="Pass your turn.")
    @app_commands.check(cooldown_enforcer(uno_pass_cd, BucketType)())
    async def pass_turn(self, interaction: discord.Interaction):
        try:
            await UnoGameManager.pass_turn(interaction)
        except Exception as e:
            logger.error(f"Error passing turn: {e}")
            await interaction.response.send_message("An error occurred while passing your turn. Please try again.", ephemeral=True)

    @app_commands.command(name="call", description="Call UNO on a player.")
    @app_commands.describe(player="The player to call UNO on.")
    @app_commands.check(cooldown_enforcer(uno_call_cd, BucketType)())
    async def call_uno(self, interaction: discord.Interaction, player: discord.Member):
        try:
            await UnoGameManager.call_uno(interaction, player)
        except Exception as e:
            logger.error(f"Error calling UNO on {player}: {e}")
            await interaction.response.send_message("An error occurred while calling UNO. Please try again.", ephemeral=True)

    @app_commands.command(name="declare", description="Declare UNO when you have one card.")
    @app_commands.check(cooldown_enforcer(uno_declare_cd, BucketType)())
    async def declare_uno(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            await UnoGameManager.declare_uno(interaction)
        except Exception as e:
            logger.error(f"Error declaring UNO: {e}")
            await interaction.followup.send("An error occurred while declaring UNO. Please try again.", ephemeral=True)

    @app_commands.command(name="quit", description="Quit the current game.")
    @app_commands.check(cooldown_enforcer(uno_quit_cd, BucketType)())
    async def quit_game(self, interaction: discord.Interaction):
        try:
            await UnoGameManager.quit_game(self, interaction)
        except Exception as e:
            logger.error(f"Error quitting the game: {e}")
            await interaction.response.send_message("An error occurred while quitting the game. Please try again.", ephemeral=True)

    @app_commands.command(name="cancel", description="Cancel the current game.")
    @app_commands.check(cooldown_enforcer(uno_cancel_cd, BucketType)())
    async def cancel_game(self, interaction: discord.Interaction):
        try:
            await UnoGameManager.cancel_game(self, interaction)
        except PermissionError:
            await interaction.response.send_message("You don't have permission to cancel this game.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error canceling the game: {e}")
            await interaction.response.send_message("An error occurred while canceling the game. Please try again.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(UnoCog(bot))