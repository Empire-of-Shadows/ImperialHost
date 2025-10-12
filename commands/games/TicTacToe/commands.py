import discord
from discord import app_commands
from discord.ext import commands
from discord.ui import Button, View, button
from typing import cast
from commands.games.TicTacToe.tictactoegame import make_view, tictactoe_game_manager
from storage.config_system import config
from utilities.logger_setup import get_logger

logger = get_logger("TicTacToeCommandCog")


def _ctx_extra(
		*,
		interaction: discord.Interaction | None = None,
		channel: discord.abc.GuildChannel | None = None,
		thread: discord.Thread | None = None,
		message: discord.Message | None = None,
		opponent: discord.Member | None = None,
		data: dict | None = None,
) -> dict:
	# Build a structured logging "extra" context for consistent diagnostics
	extra = {}
	try:
		if interaction:
			extra["user_id"] = getattr(interaction.user, "id", None)
			extra["guild_id"] = getattr(interaction.guild, "id", None) if interaction.guild else None
			extra["channel_id"] = getattr(interaction.channel, "id", None) if interaction.channel else None
			try:
				extra["command"] = interaction.command.qualified_name if interaction.command else None
			except Exception:
				extra["command"] = None
		if channel:
			extra["channel_id"] = getattr(channel, "id", extra.get("channel_id"))
			extra["guild_id"] = getattr(getattr(channel, "guild", None), "id", extra.get("guild_id"))
		if thread:
			extra["thread_id"] = getattr(thread, "id", None)
		if message:
			extra["message_id"] = getattr(message, "id", None)
		if opponent:
			extra["opponent_id"] = getattr(opponent, "id", None)
		if data:
			# Shallow merge limited keys only to avoid huge payloads
			for k, v in data.items():
				if k in {
					"difficulty",
					"who_starts",
					"visibility",
					"private",
					"invited_id",
					"game_turn",
					"game_players",
					"reason",
					"error",
				}:
					extra[k] = v
	except Exception:
		# Never let logging context building break execution
		pass
	return {"event": "tictactoe", **extra}


class JoinGameButton(Button):
	def __init__(self, game, invited_user_id: int | None = None, target_channel: discord.TextChannel = None):
		super().__init__(label="Join Game", style=discord.ButtonStyle.success)
		self.game = game
		self.invited_user_id = invited_user_id
		self.target_channel = target_channel

	async def callback(self, interaction: discord.Interaction):
		# Only allow join if slot open
		if self.game.players[1] is not None:
			logger.info(
				"Join denied: game already full",
				extra=_ctx_extra(interaction=interaction, data={"reason": "full"}),
			)
			await interaction.response.send_message("The game is already full!", ephemeral=True)
			return

		# If an invitation was set, only that user may join
		if self.invited_user_id is not None and interaction.user.id != self.invited_user_id:
			logger.info(
				"Join denied: invite-only mismatch",
				extra=_ctx_extra(interaction=interaction, data={"reason": "invite_only"}),
			)
			await interaction.response.send_message("This lobby is invite-only.", ephemeral=True)
			return

		# Assign player 2
		self.game.players[1] = interaction.user.id
		logger.info(
			"Second player joined lobby",
			extra=_ctx_extra(
				interaction=interaction,
				data={
					"game_players": [self.game.players[0], self.game.players[1]],
					"game_turn": self.game.turn,
				},
			),
		)

		# Update lobby message
		await interaction.response.edit_message(
			content=f"{interaction.user.mention} has joined the game!\nGame is starting in {self.target_channel.mention}...",
			embed=None,
			view=None,
		)

		# Push to cache
		await tictactoe_game_manager._persist_update(self.game)

		# Start game UI in the TARGET CHANNEL (not current channel)
		embed = discord.Embed(
			title="Tic-Tac-Toe",
			description=self.game.format_board_with_turn(interaction.guild.members),
			color=discord.Color.blue(),
		)
		view = await make_view(self.game)

		# Post the game board in the new channel
		await self.target_channel.send(embed=embed, view=view)

		# Also notify players in the game channel
		player1 = interaction.guild.get_member(self.game.players[0])
		player2 = interaction.guild.get_member(self.game.players[1])
		await self.target_channel.send(
			f"🎮 **Game Started!**\n"
			f"{player1.mention if player1 else 'Player 1'} vs {player2.mention if player2 else 'Player 2'}\n"
			f"{'🎯 ' + player1.mention if self.game.turn == 0 else '🎯 ' + player2.mention if player2 else 'Player 2'} goes first!"
		)


class CancelLobbyButton(Button):
	def __init__(self, game, owner_id: int, target_channel: discord.TextChannel = None):
		super().__init__(label="Cancel Lobby", style=discord.ButtonStyle.danger)
		self.game = game
		self.owner_id = owner_id
		self.target_channel = target_channel

	async def callback(self, interaction: discord.Interaction):
		if interaction.user.id != self.owner_id:
			logger.info(
				"Cancel denied: non-owner tried to cancel lobby",
				extra=_ctx_extra(interaction=interaction, data={"reason": "not_owner"}),
			)
			await interaction.response.send_message("Only the creator can cancel this lobby.", ephemeral=True)
			return

		try:
			await interaction.response.edit_message(content="Lobby canceled by creator.", embed=None, view=None)
		except Exception as e:
			logger.debug(
				"Edit lobby message failed during cancel",
				extra=_ctx_extra(interaction=interaction, data={"error": str(e)}),
			)

		logger.info(
			"Lobby canceled by owner; cleaning up game and channel",
			extra=_ctx_extra(interaction=interaction, data={"game_players": self.game.players}),
		)

		# Clean up the game and delete the channel since it was created but game never started
		await tictactoe_game_manager.cleanup_game_and_channel(self.game.channel_id, delete_channel=True)


class StartWithOptionsView(View):
	def __init__(self, game, owner_id: int, invited_user_id: int | None, target_channel: discord.TextChannel):
		super().__init__(timeout=600)
		self.add_item(JoinGameButton(game, invited_user_id=invited_user_id, target_channel=target_channel))
		self.add_item(CancelLobbyButton(game, owner_id, target_channel=target_channel))


class TicTacToeCommandCog(commands.GroupCog, name="tictactoe"):
	"""Tic-Tac-Toe slash commands with options."""

	def __init__(self, bot: commands.Bot):
		self.bot = bot
		super().__init__()

	@app_commands.command(name="start", description="Start a new Tic-Tac-Toe game with options.")
	@app_commands.describe(
		difficulty="Board size preset",
		opponent="Invite a specific opponent (optional)",
		who_starts="Who goes first",
		visibility="Where discussion happens",
	)
	@app_commands.choices(
		difficulty=[
			app_commands.Choice(name="Easy (3x3)", value="easy"),
			app_commands.Choice(name="Medium (5x5)", value="medium"),
		],
		who_starts=[
			app_commands.Choice(name="Me", value="me"),
			app_commands.Choice(name="Opponent", value="opponent"),
			app_commands.Choice(name="Random", value="random"),
		],
		visibility=[
			app_commands.Choice(name="Public Thread", value="public"),
			app_commands.Choice(name="Private Thread (invited users only)", value="private"),
		],
	)
	async def start(
			self,
			interaction: discord.Interaction,
			difficulty: app_commands.Choice[str],
			opponent: discord.Member | None = None,
			who_starts: app_commands.Choice[str] | None = None,
			visibility: app_commands.Choice[str] | None = None,
	):
		"""Starts a new Tic-Tac-Toe game by creating a new channel with a discussion thread."""
		guild = interaction.guild
		current_channel = interaction.channel

		logger.info(
			"Start command invoked",
			extra=_ctx_extra(
				interaction=interaction,
				opponent=opponent,
				data={
					"difficulty": getattr(difficulty, "value", None),
					"who_starts": getattr(who_starts, "value", None),
					"visibility": getattr(visibility, "value", None),
				},
			),
		)

		if not guild:
			await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
			return

		# RESTRICTION: Command can only be used in the game lobby
		if not isinstance(current_channel, discord.TextChannel) or current_channel.id != config.game_lobby_channel_id:
			lobby_channel = guild.get_channel(config.game_lobby_channel_id)
			lobby_mention = lobby_channel.mention if lobby_channel else f"<#{config.game_lobby_channel_id}>"

			logger.info(
				"Start command denied: wrong channel",
				extra=_ctx_extra(
					interaction=interaction,
					channel=current_channel,
					data={"reason": "not_lobby_channel"},
				),
			)
			await interaction.response.send_message(
				f"Tic-Tac-Toe games can only be started in {lobby_mention}!", ephemeral=True
			)
			return

		diff_value = difficulty.value if difficulty else "easy"
		if diff_value not in ("easy", "medium"):
			logger.warning(
				"Invalid difficulty choice",
				extra=_ctx_extra(interaction=interaction, data={"difficulty": diff_value}),
			)
			await interaction.response.send_message("Invalid difficulty choice.", ephemeral=True)
			return

		# Category enforcement for new channel creation
		category = guild.get_channel(config.game_category_id)
		if not category or not isinstance(category, discord.CategoryChannel):
			await interaction.response.send_message(
				"The games category is not available. Cannot create new game channel.", ephemeral=True
			)
			return

		# Generate a unique game name
		opponent_name = opponent.display_name[:10] if opponent else "open"
		game_name = f"tictactoe-{diff_value}-{interaction.user.display_name[:10]}-vs-{opponent_name}"

		# Create the game with a new channel
		try:
			game = await tictactoe_game_manager.start_game(0, diff_value, guild=guild,
														   game_name=game_name)  # 0 as placeholder
			target_channel = guild.get_channel(game.channel_id)
			logger.info(
				"Game created with new channel",
				extra=_ctx_extra(
					interaction=interaction,
					channel=target_channel,
					data={"difficulty": diff_value, "new_channel_id": game.channel_id},
				),
			)
		except Exception as e:
			logger.exception("Failed to create game with new channel", extra=_ctx_extra(interaction=interaction))
			await interaction.response.send_message("Failed to create the game due to an internal error.",
													ephemeral=True)
			return

		if not target_channel:
			await interaction.response.send_message("Failed to get game channel.", ephemeral=True)
			return

		# Assign the first slot to the creator
		game.players[0] = interaction.user.id

		# Handle "who starts"
		if who_starts:
			if who_starts.value == "me":
				game.turn = 0
			elif who_starts.value == "opponent":
				game.turn = 1
			else:
				game.turn = 0 if (interaction.id % 2 == 0) else 1
			logger.debug(
				"First move decided",
				extra=_ctx_extra(
					interaction=interaction,
					data={"who_starts": who_starts.value, "game_turn": game.turn},
				),
			)

		# Persist early state
		await tictactoe_game_manager._persist_update(game)
		logger.debug(
			"Early game state persisted",
			extra=_ctx_extra(interaction=interaction, data={"game_players": game.players}),
		)

		# Create discussion thread
		try:
			private = bool(visibility and visibility.value == "private")
			thread = await tictactoe_game_manager._create_game_thread(
				target_channel,
				game,
				private=private
			)

			# Invite opponent to private thread if set
			if private and opponent:
				try:
					await thread.add_user(opponent)
					logger.debug(
						"Opponent added to private thread",
						extra=_ctx_extra(interaction=interaction, thread=thread, opponent=opponent),
					)
				except Exception as e:
					logger.warning(
						"Failed to add opponent to private thread",
						extra=_ctx_extra(interaction=interaction, thread=thread, opponent=opponent,
										 data={"error": str(e)}),
					)

			logger.info(
				"Discussion thread created",
				extra=_ctx_extra(interaction=interaction, thread=thread, data={"private": private}),
			)
		except Exception as e:
			logger.error(
				"Failed to create discussion thread",
				extra=_ctx_extra(interaction=interaction, channel=target_channel, data={"error": str(e)}),
			)

		# Get the lobby channel for posting the lobby message
		lobby_channel = guild.get_channel(config.game_lobby_channel_id)
		if not lobby_channel:
			await interaction.response.send_message("Game lobby channel not found.", ephemeral=True)
			return

		# Build lobby embed for the lobby channel
		embed = discord.Embed(
			title="🎮 Tic-Tac-Toe Lobby",
			description=f"**Game Channel:** {target_channel.mention}\n**Creator:** {interaction.user.mention}\n\nWaiting for an opponent to join. Once someone joins, the game will start in the game channel.",
			color=discord.Color.blurple(),
		)
		embed.add_field(name="Difficulty", value=("3x3" if diff_value == "easy" else "5x5"), inline=True)
		embed.add_field(name="Game Channel", value=target_channel.mention, inline=True)
		if who_starts:
			embed.add_field(
				name="First Move",
				value={"me": interaction.user.mention, "opponent": opponent.mention if opponent else "Opponent",
					   "random": "Random"}[who_starts.value],
				inline=True,
			)
		if opponent:
			embed.add_field(name="Invited Player", value=opponent.mention, inline=True)

		# Create lobby view with reference to target channel
		invited_id = opponent.id if opponent else None
		# If opponent is provided, only they can join; otherwise it's open to everyone
		view = StartWithOptionsView(
			game,
			owner_id=interaction.user.id,
			invited_user_id=invited_id,
			target_channel=target_channel,
		)

		# Post lobby message in the GAME_LOBBY_ID channel
		if current_channel.id == config.game_lobby_channel_id:
			# If we're already in the lobby channel, respond normally
			await interaction.response.send_message(embed=embed, view=view)
			lobby_msg = await interaction.original_response()
		else:
			# If somehow we got here from another channel, post in lobby and respond ephemerally
			lobby_msg = await lobby_channel.send(embed=embed, view=view)
			await interaction.response.send_message(f"Lobby created in {lobby_channel.mention}!", ephemeral=True)

		# Store lobby message reference
		game.root_message_id = int(lobby_msg.id)
		await tictactoe_game_manager._persist_update(game)

		logger.info(
			"Lobby message posted in game lobby",
			extra=_ctx_extra(interaction=interaction, message=lobby_msg,
							 data={"invited_id": invited_id, "new_channel_id": target_channel.id}),
		)

		# Send welcome message to the new game channel
		try:
			await target_channel.send(
				f"🎮 **Tic-Tac-Toe Game Channel Created!**\n"
				f"Creator: {interaction.user.mention}\n"
				f"{'Invited: ' + opponent.mention if opponent else 'Open to anyone'}\n"
				f"Difficulty: {diff_value.title()}\n\n"
				f"⏳ Waiting for opponent to join the lobby in {lobby_channel.mention}...\n"
				f"💬 Use the thread for discussion once the game starts!"
			)
		except Exception as e:
			logger.warning(f"Failed to send welcome message to new channel: {e}")

		# If an opponent was specified, ping them in the lobby channel
		if opponent:
			try:
				await lobby_channel.send(
					f"🔔 {opponent.mention} you've been invited to join a Tic-Tac-Toe game! Use the button above to join.")

				# Also ping in the game channel
				await target_channel.send(
					f"🔔 {opponent.mention} you've been invited to this game! Please join using the lobby in {lobby_channel.mention}.")

				logger.debug(
					"Opponent pings sent to both channels",
					extra=_ctx_extra(interaction=interaction, opponent=opponent),
				)
			except Exception as e:
				logger.debug(
					"Failed to send opponent pings",
					extra=_ctx_extra(interaction=interaction, opponent=opponent, data={"error": str(e)}),
				)

	@app_commands.command(name="cancel", description="Cancel the active Tic-Tac-Toe lobby/game in this channel.")
	async def cancel(self, interaction: discord.Interaction):
		channel = interaction.channel
		logger.info("Cancel command invoked", extra=_ctx_extra(interaction=interaction, channel=channel))

		if not isinstance(channel, discord.TextChannel):
			logger.info(
				"Cancel denied: not a text channel",
				extra=_ctx_extra(interaction=interaction, channel=channel, data={"reason": "not_text_channel"}),
			)
			await interaction.response.send_message("This command can only be used in text channels.", ephemeral=True)
			return

		game = await tictactoe_game_manager.get_game(channel.id)
		if not game:
			logger.info(
				"Cancel denied: no active game",
				extra=_ctx_extra(interaction=interaction, channel=channel),
			)
			await interaction.response.send_message("No active Tic-Tac-Toe game in this channel.", ephemeral=True)
			return

		# Only a participant or a user with Manage Channels can cancel
		am_participant = interaction.user.id in [pid for pid in game.players if pid]
		has_manage = interaction.user.guild_permissions.manage_channels  # type: ignore[union-attr]
		if not am_participant and not has_manage:
			logger.info(
				"Cancel denied: not participant or moderator",
				extra=_ctx_extra(
					interaction=interaction,
					channel=channel,
					data={"game_players": game.players, "reason": "insufficient_permissions"},
				),
			)
			await interaction.response.send_message("Only a participant or a moderator can cancel this game.",
													ephemeral=True)
			return

		# Clean up game and delete the channel
		await tictactoe_game_manager.cleanup_game_and_channel(channel.id, delete_channel=True)
		logger.info("Game canceled and cleaned up with channel deletion",
					extra=_ctx_extra(interaction=interaction, channel=channel))
		await interaction.response.send_message("Game canceled, cleaned up, and channel will be deleted.",
												ephemeral=True)


async def setup(bot: commands.Bot):
	await bot.add_cog(TicTacToeCommandCog(bot))