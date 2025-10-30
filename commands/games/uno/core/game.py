import asyncio
import json
import random
from datetime import datetime, timedelta
import discord
from discord.ui import Button, View
from commands.games.uno.utils.valid_card_check import can_play_card
from storage.config_system import config
from utilities.timers import get_10_min_countdown_timestamp, get_1_min_countdown_timestamp
from commands.games.uno.core.cards import CardDeck
from commands.games.uno.core.player import Player
from commands.games.uno.utils.permissions import setup_game_permissions
from utilities.bot import dispatcher
from utilities.logger_setup import get_logger

logger = get_logger("UNO_GAME_MANAGER")
current_time_utc = datetime.now().astimezone().replace(tzinfo=None)

class UnoGameManager:
    uno_games = {}

    def __init__(self, bot):
        self.bot = bot

    async def create_game(self, interaction: discord.Interaction, mode: str):
        """
        Creates a new UNO game with the mode selected by the user.
        Options: `thread` or `voice`.
        """
        logger.info(f"User {interaction.user} initiated game creation with mode '{mode}'.")

        def serialize_discord_object(obj):
            """
            Convert Discord objects into string representations.
            Used for logging non-serializable data.
            """
            try:
                if isinstance(obj, discord.Guild):
                    return f"Guild(id={obj.id}, name={obj.name})"
                if isinstance(obj, discord.TextChannel):
                    category = obj.category.name if obj.category else "None"
                    return f"TextChannel(id={obj.id}, name={obj.name}, category={category})"
                if isinstance(obj, discord.Member):
                    return f"Member(id={obj.id}, name={obj.name}, bot={obj.bot}, nick={obj.nick})"
                if isinstance(obj, discord.VoiceChannel):
                    category = obj.category.name if obj.category else "None"
                    return f"VoiceChannel(id={obj.id}, name={obj.name}, category={category})"
                if hasattr(obj, "__dict__"):  # For generic objects with attributes
                    return obj.__dict__
            except Exception as e:
                logger.warning(f"Error serializing object: {e}")
            return str(obj)  # Fallback for anything else

        # Fetch the games category from the guild by ID
        try:
            games_category = await interaction.guild.fetch_channel(config.game_category_id)
            if not isinstance(games_category, discord.CategoryChannel):
                logger.error(f"Channel with ID {config.game_category_id} is not a category channel.")
                return await interaction.followup.send("❌ The configured games category ID is not a valid category!", ephemeral=True)
        except discord.NotFound:
            logger.error(f"Games category with ID {config.game_category_id} not found.")
            return await interaction.followup.send("❌ The games category does not exist!", ephemeral=True)
        except discord.Forbidden:
            logger.error(f"No permission to access category with ID {config.game_category_id}.")
            return await interaction.followup.send("❌ Bot doesn't have permission to access the games category!", ephemeral=True)
        except discord.HTTPException as e:
            logger.error(f"Error fetching category {config.game_category_id}: {e}")
            return await interaction.followup.send("❌ Error accessing the games category!", ephemeral=True)

        # Check if the user already has an active game
        existing_game = next((game for game in UnoGameManager.uno_games.values() if game["owner"] == interaction.user),
                             None)
        if existing_game:
            existing_channel = existing_game["channel"]
            if not interaction.guild.get_channel(existing_channel.id):
                # If the game channel is deleted, clean up the game
                logger.warning(
                    f"Detected stale game for {interaction.user} in deleted channel {existing_channel.id}. Cleaning up.")
                del UnoGameManager.uno_games[existing_channel.id]
                return await interaction.followup.send(
                    "❌ Your previous game channel was deleted. The game has been removed. Please try again.",
                    ephemeral=True,
                )
            else:
                logger.info(
                    f"User {interaction.user} must finish or cancel their current game in {existing_channel.name}.")
                return await interaction.followup.send(
                    "❌ You already have an active game. Please finish or cancel it before starting a new one.",
                    ephemeral=True,
                )

        # Notify the user of the progress
        await interaction.followup.send("Setting up the game...", ephemeral=True)
        try:
            # Create a new text channel in the "Games" category
            logger.info(f"Creating game channel for user {interaction.user}.")
            channel_name = f"{interaction.user.name.lower()}-uno"
            overwrites = setup_game_permissions(interaction.guild, interaction.user)

            channel = await interaction.guild.create_text_channel(
                name=channel_name, overwrites=overwrites, category=games_category
            )
            logger.info(f"Successfully created game channel '{channel.name}' (ID: {channel.id}).")

            # Initialize game state
            uno_games = {
                "guild": interaction.guild,
                "players": [Player(interaction.user)],  # Add the user as the first player
                "deck": CardDeck(),
                "skips": 0,
                "turn_index": 0,
                "last_played_card": None,
                "discard_pile": [],
                "channel": channel,
                "game_id": channel.id,
                "thread": None,
                "voice_channel": None,
                "owner": interaction.user,
                "sticky_message": None,
                "direction": 1,
                "started": False,
                "turn_timeout_task": None,
            }
            UnoGameManager.uno_games[channel.id] = uno_games
            logger.info(f"UNO game created. Game ID: {uno_games['game_id']}, Owner: {uno_games['owner'].name}.")
            logger.debug("Game state:")
            logger.debug(json.dumps(uno_games, default=serialize_discord_object, indent=4))

            timestamp = get_10_min_countdown_timestamp()
            if mode == "thread":
                thread_message = await channel.send(f"Use the thread below for discussion.")
                await dispatcher.submit(channel.send(
                    content=(
                        f"{interaction.user.mention} has started a new UNO game! Click the button below to join.\n"
                        f"Cleans up {timestamp} if not started."
                    )
                ))
                thread = await thread_message.create_thread(name=f"Game Discussion", auto_archive_duration=10080)
                uno_games["thread"] = thread
                await thread.send("Welcome to the game thread! Only players can chat here. 🎮")
                logger.info("Game thread created successfully.")

            elif mode == "voice":
                voice_channel_name = f"Uno Voice - {interaction.user.name}"
                voice_manager = self.bot.get_cog("VoiceManager")
                voice_channel = await voice_manager.create_game_voice_channel(
                    guild=interaction.guild,
                    category=games_category,
                    game_channel=channel,
                    players=[interaction.user]  # Allow only the game owner initially
                )
                uno_games["voice_channel"] = voice_channel
                await channel.send(f"A temporary voice channel was created: {voice_channel.mention} 🎤")
                logger.info(f"Voice channel '{voice_channel.name}' created successfully (ID: {voice_channel.id}).")

            # Add the join game button
            join_view = JoinGameView(self, channel.id)  # Pass the UnoGameManager instance and channel ID
            join_message = await channel.send(
                content="Click the button below to join the game!",
                view=join_view
            )
            uno_games["join_message"] = join_message

            pinned_message = await channel.send(
                "**🎮 UNO Game Setup Complete!**\n\n"
                "**🃏 Game Commands:**\n"
                "> `/new_uno_game` – Start a new UNO game\n"
                "> `/start_game` – Begin the match\n"
                "> `/play_card` – Play your card\n"
                "> `/draw_card` – Draw a new card\n"
                "> `/pass_turn` – Skip your turn\n"
                "> `/call_uno` – Call UNO when you’re down to 2 cards\n"
                "> `/declare_uno` – Declare UNO\n"
                "> `/cancel_game` – Cancel the ongoing game\n\n"
                "**📜 Game Rules:**\n"
                "🔹 Match cards by **color** or **value**.\n"
                "🔹 You **must call UNO** when you’re down to **2 cards**.\n"
                "🔹 First to get rid of all their cards wins!\n\n"
                "**🎉 Have fun and play fair!**\n"
                "-# Remember to report issues"
            )
            await pinned_message.pin()

            # Notify the user
            if mode == "thread":
                await interaction.followup.send(
                    f"Game created! Navigate to the channel: {uno_games['channel'].mention}\n"
                    f"A new thread was created for the game. {uno_games['thread'].mention}"
                )
            elif mode == "voice":
                await interaction.followup.send(
                    f"Game created! Navigate to the text channel: {uno_games['channel'].mention}.\n"
                    f"A voice channel was also created. {uno_games['voice_channel'].mention}"
                )

            # Schedule expiration task
            self.bot.loop.create_task(self.expire_game_after_timeout(channel.id, uno_games, 600))  # 10-minute timeout
            logger.info(f"Expiration task scheduled for the game in channel {channel.id}.")

        except Exception as e:
            logger.exception(f"Error during game creation for user {interaction.user}: {e}")
            await interaction.followup.send("❌ An error occurred while creating the game. Please try again later.",
                                            ephemeral=True)

    async def expire_game_after_timeout(self, channel_id, uno_games, timeout):
        """
        Expires a game if it hasn't started within the given timeout period.
        """
        try:
            logger.info(
                f"Starting expiration countdown for game in channel {channel_id} with timeout of {timeout} seconds.")
            expiration_time = discord.utils.utcnow() + timedelta(seconds=timeout)
            await discord.utils.sleep_until(expiration_time)

            # Check if the game is still active and hasn't started
            active_game = UnoGameManager.uno_games.get(channel_id)
            if active_game:
                if not active_game["started"]:
                    logger.info(
                        f"Game in channel {channel_id} has not started within {timeout} seconds. Preparing to clean up.")
                    # Notify the game creator and clean up
                    await active_game["channel"].send(
                        "⏳ The game was not started within 10 minutes and has been canceled. Cleaning up..."
                    )
                    await self.end_game(active_game)
                    logger.info(f"Game in channel {channel_id} has been successfully expired and cleaned up.")
                else:
                    logger.info(f"Game in channel {channel_id} has already started. Expiration task canceled.")
            else:
                logger.warning(f"No active game found for channel {channel_id}. Expiration task skipped.")

        except Exception as e:
            logger.error(f"An error occurred while expiring the game for channel {channel_id}: {e}", exc_info=True)

    async def join_game_button_callback(self, interaction: discord.Interaction):
        """
        Handles the interaction for the Join Game button.
        Ensures the player joins the game and updates permissions for thread/voice if needed.
        """
        logger.info(f"User {interaction.user} clicked the Join Game button in channel {interaction.channel.id}.")

        # Retrieve the game instance from the game manager
        game = UnoGameManager.uno_games.get(interaction.channel.id)

        if not game:
            logger.warning(
                f"No active game found in channel {interaction.channel.id} when {interaction.user} tried to join.")
            if interaction.response.is_done():
                await interaction.followup.send("❌ No active game in this channel.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ No active game in this channel.", ephemeral=True)
            return

        # Restrict joining if the game has already started
        if game["started"]:
            logger.info(
                f"User {interaction.user} tried to join an already started game in channel {interaction.channel.id}.")
            if interaction.response.is_done():
                await interaction.followup.send("❌ The game has already started!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ The game has already started!", ephemeral=True)
            return

        # Check if the player is already part of the game
        player = Player(interaction.user)
        if player in game["players"]:
            logger.info(f"User {interaction.user} is already part of the game in channel {interaction.channel.id}.")
            if interaction.response.is_done():
                await interaction.followup.send("❌ You're already in the game!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ You're already in the game!", ephemeral=True)
            return

        # Check if the game is full
        if len(game["players"]) >= 4:
            logger.info(f"User {interaction.user} tried to join a full game in channel {interaction.channel.id}.")
            if interaction.response.is_done():
                await interaction.followup.send("❌ This game is full!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ This game is full!", ephemeral=True)
            return

        # Add the player to the game
        game["players"].append(player)
        logger.info(
            f"User {interaction.user} joined the game in channel {interaction.channel.id}. Total players: {len(game['players'])}.")

        # Retrieve thread and voice_channel from the game
        thread = game.get("thread")
        voice = game.get("voice_channel")
        logger.debug(f"Thread: {thread}, Voice: {voice}, Channel: {game.get('channel')}")

        # Update thread permissions if it exists
        if thread is not None:
            try:
                await thread.add_user(interaction.user)
                await thread.send(
                    f"👋 Welcome {interaction.user.mention}! Feel free to chat here until the game begins.")
                logger.info(f"User {interaction.user} added to thread {thread.id}.")
            except discord.HTTPException as e:
                logger.error(f"Failed to add {interaction.user} to thread {thread.id}: {e}")

        # Update voice channel permissions if it exists
        if voice is not None:
            try:
                overwrites = setup_game_permissions(
                    guild=interaction.guild,
                    creator=game["owner"],
                    players=game["players"],
                    is_voice=True
                )
                await voice.edit(overwrites=overwrites)
                logger.info(f"Voice channel permissions updated for {voice.id} to include {interaction.user}.")
            except discord.HTTPException as e:
                logger.error(f"Failed to update permissions for voice channel {voice.id}: {e}")

        # Build a response message to confirm the player has joined
        followup_message = f"✅ {interaction.user.mention}, you joined the game!"
        if thread is not None:
            followup_message += f"\nYou can now join {thread.mention} to chat with your opponents."
        if voice is not None:
            followup_message += f"\nYou can now join {voice.mention} to communicate with your opponents."

        # Send the response
        if not interaction.response.is_done():
            await interaction.response.send_message(followup_message, ephemeral=True)
        else:
            await interaction.followup.send(followup_message, ephemeral=True)
        logger.info(f"Notification sent to {interaction.user} confirming they joined the game.")

        # Announce in the game channel that the player has joined
        await game["channel"].send(f"{interaction.user.mention} has joined the game!")
        logger.info(f"Announcement made in the game channel {game['channel'].id} about {interaction.user} joining.")

        # Update the sticky display to reflect new player
        try:
            await UnoGameManager.update_sticky_display(game)
            logger.info(f"Sticky display updated for game in channel {interaction.channel.id}.")
        except Exception as e:
            logger.error(f"Error while updating sticky display for game in channel {interaction.channel.id}: {e}",
                         exc_info=True)

    @staticmethod
    async def start_game(interaction: discord.Interaction):
        """
        Starts the game and updates permissions.
        """
        import random  # To enable random selection of the starting player

        logger.info(f"User {interaction.user} is attempting to start a game in channel {interaction.channel.id}.")

        # Retrieve the game instance from the UnoGameManager
        uno_game = UnoGameManager.uno_games.get(interaction.channel.id)

        # Validate if a game exists in the current channel
        if not uno_game:
            logger.warning(f"No active game found in channel {interaction.channel.id} for {interaction.user}.")
            return await interaction.followup.send("❌ No active game in this channel.", ephemeral=True)

        # Validate the interaction occurred in the correct game channel
        if interaction.channel.id != uno_game.get("game_id"):
            logger.warning(
                f"Interaction channel {interaction.channel.id} does not match game channel {uno_game.get('game_id')} for {interaction.user}.")
            return await interaction.followup.send("❌ No active game in this channel.", ephemeral=True)

        # Check that only the game owner can start the game
        if interaction.user != uno_game["owner"]:
            logger.warning(
                f"User {interaction.user} tried to start the game but is not the owner in channel {interaction.channel.id}.")
            return await interaction.followup.send("❌ Only the game owner can start the game.", ephemeral=True)

        # Validate that there are enough players to start the game
        if len(uno_game["players"]) < 2:
            logger.info(
                f"User {interaction.user} attempted to start a game with less than 2 players in channel {interaction.channel.id}.")
            return await interaction.followup.send("❌ At least 2 players are required to start the game.",
                                                   ephemeral=True)

        # Check if the game has already started
        if uno_game["started"]:
            logger.info(
                f"User {interaction.user} tried to start a game that is already in progress in channel {interaction.channel.id}.")
            return await interaction.followup.send("❌ The game has already started!", ephemeral=True)

        # Additional validation for the game's state
        if not uno_game["game_id"]:
            logger.error(f"Game in channel {interaction.channel.id} has an invalid or missing game_id.")
            return await interaction.followup.send("❌ Invalid game state. Please try again.", ephemeral=True)

        try:
            # Update permissions for all players
            logger.info(f"Updating channel permissions for game in channel {interaction.channel.id}.")
            overwrites = setup_game_permissions(interaction.guild, uno_game["owner"], uno_game["players"])
            await uno_game["channel"].edit(overwrites=overwrites)

            # Shuffle the players randomly and select the starting player
            random.shuffle(uno_game["players"])
            starting_player = uno_game["players"][0]  # The first player will go first
            uno_game["turn_index"] = 0  # Set turn index to the starting player
            logger.info(
                f"Players shuffled in channel {interaction.channel.id}. Starting player: {starting_player.user}.")

            # Deal 7 cards to each player
            logger.info(f"Dealing 7 cards to each player in channel {interaction.channel.id}.")
            try:
                for player in uno_game["players"]:
                    for _ in range(7):
                        if not uno_game["deck"].deck:  # Deck is empty
                            logger.warning("Deck ran out of cards while dealing. Reshuffling...")
                            uno_game["deck"].deck = random.shuffle(uno_game["discard_pile"][:-1])  # Leave one card on the pile
                            uno_game["discard_pile"] = [uno_game["discard_pile"][-1]]  # Reset discard pile
                        player.add_card(uno_game["deck"].draw())
            except Exception as e:
                logger.error(f"Error occurred while dealing cards: {e}")
                raise

            # Set the first card for the game
            logger.info("Dealing the first card for the game...")
            while True:
                uno_game["last_played_card"] = uno_game["deck"].draw()
                card = uno_game["last_played_card"]
                card_value = card.split(" ")[1] if " " in card else card

                if card_value in ["Wild", "+4"]:  # Wild cards cannot start
                    uno_game["deck"].deck.insert(0, card)  # Put it back into the deck
                    logger.debug(f"Card '{card}' cannot start the game. Drawing a new card...")
                    continue
                break

            # Initialize a discard pile with the first card
            uno_game["discard_pile"] = [uno_game["last_played_card"]]

            logger.info(
                f"Game in channel {interaction.channel.id} started successfully. First card: {uno_game['last_played_card']}."
            )
            # Flag the game as started
            uno_game["started"] = True
            logger.info(f"Game in channel {interaction.channel.id} flagged as started.")

            # Notify players in the game channel
            await interaction.followup.send("✅ The game has started!")
            await uno_game["channel"].send(f"🎮 **The UNO game has started!**")
            await uno_game["channel"].send(
                f"🎉 `{starting_player.user.display_name}` will go first! The first card is `{uno_game['last_played_card']}`."
            )
            logger.info(f"Game start messages sent in channel {interaction.channel.id}.")

            # Update the sticky display to show the current game state
            await UnoGameManager.update_sticky_display(uno_game)
            logger.info(f"Sticky display updated for game in channel {interaction.channel.id}.")
        except Exception as e:
            logger.error(f"An error occurred while starting the game in channel {interaction.channel.id}: {e}",
                         exc_info=True)
            await interaction.followup.send("❌ An error occurred while starting the game. Please try again later.",
                                            ephemeral=True)

    @staticmethod
    async def update_sticky_display(game):
        """
        Updates the sticky display showing the game state in an embed format.
        """
        logger.info(f"Updating the sticky display for game in channel {game['channel'].id}.")

        try:
            # Check that the channel and players exist
            if not game.get("channel") or not game.get("players"):
                logger.error("Game channel or players are missing.")
                return

            # Create game embed
            started = game.get("started", False)
            embed = discord.Embed(
                title="🎮 UNO Game Information",
                color=discord.Color.green(),
                description="Game is currently ongoing. Here's the current state:",
            )

            # Add game details
            try:
                embed.add_field(
                    name="Current Player",
                    value=f"{game['players'][game['turn_index']].user.mention}" if game.get("players") else "None",
                    inline=False,
                )
            except IndexError:
                embed.add_field(name="Current Player", value="None (No players)", inline=False)

            embed.add_field(
                name="Last Played Card",
                value=game.get("last_played_card", "None"),
                inline=False,
            )
            embed.add_field(
                name="Player Hands",
                value="\n".join(
                    [
                        f"{p.user.mention}: {len(p.hand)} cards | ⏭️ AFK: {getattr(p, 'skips', 0)}"
                        for p in game["players"]
                    ]
                ),
                inline=False,
            )

            # Add turn expiration if the game has started
            if started:
                timestamp = get_1_min_countdown_timestamp()
                embed.add_field(
                    name=f"Current Turn Expiration",
                    value=f"{timestamp}",
                    inline=False,
                )
            # Delete the current sticky message if it exists
            sticky_deleted = False
            if game.get("sticky_message"):
                try:
                    await game["sticky_message"].delete()
                    sticky_deleted = True
                    logger.info("Deleted the previous sticky message.")
                except discord.HTTPException as e:
                    logger.error(f"Failed to delete the previous sticky message: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error during sticky deletion: {e}")

            # If the sticky message was successfully deleted (or didn't exist), send a new one
            if sticky_deleted or not game.get("sticky_message"):
                game["sticky_message"] = await game["channel"].send(embed=embed)
                logger.info(
                    f"Sticky display updated successfully in channel {game['channel'].id}."
                )
                if started:
                    # Cancel the previous turn timeout task if it exists
                    if game.get("turn_timeout_task") and not game["turn_timeout_task"].done():
                        game["turn_timeout_task"].cancel()
                        logger.info(
                            f"Canceled previous timeout task for game in channel {game['channel'].id}."
                        )

                    # Start a new timeout task for this turn
                    turn_index = game["turn_index"]
                    logger.info(
                        f"Starting timeout task for player {game['players'][turn_index].user} in channel {game['channel'].id}."
                    )
                    game["turn_timeout_task"] = asyncio.create_task(
                        wait_for_turn_timeout(game, turn_index)
                    )



        except Exception as e:
            logger.error(
                f"Error occurred while updating sticky display for game in channel {game['channel'].id}: {e}",
                exc_info=True,
            )

    @staticmethod
    async def end_game(game):
        """
        Cleans up the game and ends it.
        """
        # Cancel previous turn timeout if exists
        if game.get("turn_timeout_task") and not game["turn_timeout_task"].done():
            game["turn_timeout_task"].cancel()

        # Notify the game channel about cleanup
        await game["channel"].send("🧹 Cleaning up and ending the game...")

        # Remove the game from active tracking
        if game["channel"].id in UnoGameManager.uno_games:
            del UnoGameManager.uno_games[game["channel"].id]
            logger.info(">< UNO Game deleted.")

        if game["voice_channel"]:
            logger.debug(f"Attempting to delete voice channel: {game['voice_channel'].id}.")
            try:
                await game["voice_channel"].delete()
                logger.debug(f"Voice channel deleted: {game['voice_channel'].id}.")
            except discord.HTTPException as e:
                logger.error(f"Failed to delete voice channel: {e}")
        else:
            logger.debug("No voice channel to delete.")
        # Delete the text channel
        try:
            logger.debug(f"Attempting to delete text channel: {game['channel'].id}.")
            await game["channel"].delete()
            logger.debug(f"Text channel deleted: {game['channel'].id}.")
        except discord.HTTPException as e:
            logger.error(f"Failed to delete text channel: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during game cleanup: {e}")

    @staticmethod
    async def play_card(interaction: discord.Interaction, card: str):
        """
        Handles a player attempting to play a card.
        """
        logger.info(f"User {interaction.user} attempted to play card '{card}' in channel {interaction.channel.id}.")

        try:
            await interaction.response.defer(ephemeral=False)

            game = UnoGameManager.uno_games.get(interaction.channel.id)

            # Validation: Ensure the game exists and has started
            if not game:
                logger.warning(f"No active game found in channel {interaction.channel.id}.")
                return await interaction.followup.send("Play Card: ❌ No active game in this channel.", ephemeral=False)
            if not game["started"]:
                logger.warning(f"Game in channel {interaction.channel.id} hasn't started yet.")
                return await interaction.followup.send("❌ The game hasn't started yet!", ephemeral=False)

            # Validate the player and their turn
            player = next((p for p in game["players"] if p.user == interaction.user), None)
            if not player:
                logger.warning(f"User {interaction.user} is not part of the game in channel {interaction.channel.id}.")
                return await interaction.followup.send("❌ You are not part of this game.", ephemeral=False)
            if game["players"][game["turn_index"]] != player:
                logger.info(
                    f"User {interaction.user} attempted to play out of turn in channel {interaction.channel.id}.")
                return await interaction.followup.send("❌ It's not your turn!", ephemeral=False)

            # Ensure the card is in the player's hand and it's playable
            if card not in player.hand:
                logger.warning(f"User {interaction.user} tried playing a card they do not have: '{card}'.")
                return await interaction.followup.send("❌ You do not have this card.", ephemeral=False)
            if not UnoGameManager.is_valid_card(card, game["last_played_card"]):
                logger.info(
                    f"Invalid card '{card}' played by {interaction.user}. Last played card: {game['last_played_card']}.")
                await asyncio.sleep(.5)
                return await interaction.followup.send("❌ Invalid card played.", ephemeral=False)

            # Remove the card and update the last played card
            player.remove_card(card)
            game["last_played_card"] = card
            logger.info(f"User {interaction.user} played '{card}' successfully in channel {interaction.channel.id}.")

            # Handle Reverse card
            if "Reverse" in card:
                if len(game["players"]) == 2:
                    logger.info("Reverse card played in a 2-player game; turn direction remains the same.")
                    await interaction.followup.send(f"🔄 {interaction.user.mention} played {card}!")
                else:
                    game["direction"] *= -1  # Reverse the direction
                    logger.info(f"Turn direction reversed. New direction: {game['direction']}.")

                    game["current_player_index"] = (game["current_player_index"] + game["direction"]) % len(
                        game["players"])

                    await asyncio.sleep(.5)
                    await interaction.followup.send(
                        f"🔄 {interaction.user.mention} played {card}! The turn order is now reversed."
                    )

            # Handle Skip card
            elif "Skip" in card:
                skipped_player_index = (game["turn_index"] + game["direction"]) % len(game["players"])
                skipped_player = game["players"][skipped_player_index]
                game["turn_index"] = (game["turn_index"] + game["direction"] * 2) % len(game["players"])
                logger.info(f"{interaction.user} played {card}. {skipped_player.user}'s turn is skipped.")
                await asyncio.sleep(.5)
                await interaction.followup.send(
                    f"⏩ {interaction.user.mention} played {card}! {skipped_player.user.mention}'s turn is skipped."
                )

            # Handle +2 card
            elif "+2" in card:
                next_player_index = (game["turn_index"] + game["direction"]) % len(game["players"])
                next_player = game["players"][next_player_index]
                for _ in range(2):
                    next_player.add_card(game["deck"].draw())
                game["turn_index"] = (game["turn_index"] + game["direction"] * 2) % len(game["players"])
                logger.info(f"{interaction.user} played {card}. {next_player.user} drew 2 cards.")
                await asyncio.sleep(.5)
                await interaction.followup.send(
                    f"➕ {interaction.user.mention} played {card}! {next_player.user.mention} draws 2 cards and their turn is skipped."
                )

            # Handle Wild +4 card
            elif "+4" in card:
                next_player_index = (game["turn_index"] + game["direction"]) % len(game["players"])
                next_player = game["players"][next_player_index]

                # Apply the +4 draw to the next player
                for _ in range(4):
                    next_player.add_card(game["deck"].draw())
                logger.info(f"{interaction.user} played {card}. {next_player.user} drew 4 cards.")

                # Prompt user to select a color
                game["pending_wild"] = player  # Flag the current player
                selected_color = await UnoGameManager.prompt_color_selection(interaction, player)

                if not selected_color:
                    # If no color was selected (timeout), end the current turn or handle accordingly
                    logger.warning(f"{interaction.user} did not select a color in time!")
                    return

                # Apply the selected color to the game state
                game["last_played_card"] = f"{selected_color} +4"  # Set the new color
                game["pending_wild"] = None  # Clear the pending wild flag
                logger.info(f"{interaction.user} set the color to: {selected_color}.")
                game["turn_index"] = (game["turn_index"] + game["direction"] * 2) % len(game["players"])
                await asyncio.sleep(.5)
                await interaction.followup.send(
                    f"🎨 {interaction.user.mention} successfully changed the color to **{selected_color}** and applied **+4**!"
                )

            # Handle Wild card
            elif "Wild" in card:
                game["pending_wild"] = player  # Flag the current player pending color selection
                selected_color = await UnoGameManager.prompt_color_selection(interaction, player)

                if not selected_color:
                    # If no color was selected (timeout), end the current turn or handle accordingly
                    logger.warning(f"{interaction.user} did not select a color in time!")
                    return

                # Apply the selected color to the game state
                game["last_played_card"] = f"{selected_color} Wild"  # Set the new color
                game["pending_wild"] = None  # Clear the pending wild flag
                await asyncio.sleep(.5)
                logger.info(f"{interaction.user} set the color to: {selected_color}.")
                await interaction.followup.send(
                    f"🎨 {interaction.user.mention} successfully changed the color to **{selected_color}**!"
                )
                game["turn_index"] = (game["turn_index"] + game["direction"]) % len(game["players"])
            # Standard Cards
            else:
                logger.info(f"User {interaction.user} played a standard card '{card}'.")
                await asyncio.sleep(.5)
                await interaction.followup.send(f"♦️ {interaction.user.mention} played {card}.")
                game["turn_index"] = (game["turn_index"] + game["direction"]) % len(game["players"])


            # Check if the player has won
            if len(player.hand) == 0:
                logger.info(f"User {interaction.user} has won the game in channel {interaction.channel.id}.")
                await interaction.followup.send(f"🎉 {interaction.user.mention} has won the game! 🎉")
                await asyncio.sleep(15)  # Give some time for players to see the message
                return await UnoGameManager.end_game(game)

            # Check if a player needs to declare UNO
            if len(player.hand) == 1 and not getattr(player, "declared_uno", False):
                game["pending_uno_penalty"] = player
                logger.warning(f"User {interaction.user} has 1 card but did not declare UNO.")
                await interaction.followup.send(
                    f"⚠️ {interaction.user.mention}, you forgot to declare UNO! Other players can now call UNO on you."
                )

            # Update the sticky display
            await UnoGameManager.update_sticky_display(game)
            logger.info(f"Sticky display updated for game in channel {interaction.channel.id}.")

        except Exception as e:
            logger.error(
                f"Error occurred while processing play_card for {interaction.user} in channel {interaction.channel.id}: {e}",
                exc_info=True)
            await interaction.followup.send("❌ An error occurred while processing your move. Please try again.",
                                            ephemeral=True)

    @staticmethod
    def is_valid_card(card, last_card):
        """
        Checks if a card can legally be played based on the last played card.
        """
        logger.info(f"Validating card '{card}' against last played card '{last_card}'.")

        try:
            # Any card can be played if there's no last card on the pile
            if not last_card:
                logger.debug("No last card on the pile. Any card can be played.")
                return True

            # Special cases for Wild and +4 cards
            if "Wild" in card or "+4" in card:
                logger.debug(f"Card '{card}' is a wild card or +4 and is always valid.")
                return True

            # Match either color or value for regular cards
            card_color, card_value = card.split(" ", 1)
            last_color, last_value = last_card.split(" ", 1)

            is_valid = card_color == last_color or card_value == last_value
            logger.debug(f"Validation result for card '{card}' against '{last_card}': {is_valid}")
            return is_valid

        except ValueError:
            # Handle cases where the card format is invalid
            logger.error(f"Invalid card format for '{card}' or '{last_card}'.")
            return False

    @staticmethod
    async def advance_turn(game):
        """
        Moves the turn to the next player, considering the current turn direction.
        """
        try:
            previous_turn_index = game["turn_index"]
            player_count = len(game["players"])

            if player_count <= 0:
                logger.error("No players available in the game to advance the turn.")
                return

            # Update turn index based on direction
            game["turn_index"] = (previous_turn_index + game["direction"]) % player_count
            logger.info(
                f"Turn advanced from player index {previous_turn_index} to {game['turn_index']}"
            )

        except Exception as e:
            logger.error(f"Error occurred while advancing the turn: {e}", exc_info=True)

    @staticmethod
    async def draw_card(interaction: discord.Interaction):
        """
        Handles a player drawing a card.
        """
        logger.info(f"User {interaction.user} attempted to draw a card in channel {interaction.channel.id}.")

        try:
            # Retrieve the game from the manager
            game = UnoGameManager.uno_games.get(interaction.channel.id)

            # Validation: Check if there's an active game and if the game has started
            if not game:
                logger.warning(f"No active game found in channel {interaction.channel.id}.")
                return await interaction.response.send_message(
                    "Draw Card: ❌ No active game in this channel.", ephemeral=True
                )
            if not game["started"]:
                logger.warning(f"Game in channel {interaction.channel.id} has not started yet.")
                return await interaction.response.send_message("❌ The game hasn't started yet!", ephemeral=True)

            # Validate that the user is part of the game
            player = next((p for p in game["players"] if p.user == interaction.user), None)
            if not player:
                logger.warning(f"User {interaction.user} is not part of the game in channel {interaction.channel.id}.")
                return await interaction.response.send_message("❌ You are not part of this game.", ephemeral=True)
            if game["players"][game["turn_index"]] != player:
                logger.info(
                    f"User {interaction.user} attempted to draw out of turn in channel {interaction.channel.id}.")
                return await interaction.response.send_message("❌ It's not your turn!", ephemeral=True)

            # Draw a card from the deck
            drawn_card = game["deck"].draw()
            if not drawn_card:
                logger.warning(f"The deck is empty in channel {interaction.channel.id}. No card can be drawn.")
                return await interaction.response.send_message("❌ The deck is empty. No card to draw.", ephemeral=True)

            # Add the card to the player's hand
            player.add_card(drawn_card)
            logger.info(f"User {interaction.user} drew card '{drawn_card}' in channel {interaction.channel.id}.")

            # Reset UNO declaration if applicable
            if getattr(player, "declared_uno", True):
                setattr(player, "declared_uno", False)

            # Inform the player of the drawn card and its playability
            if UnoGameManager.is_valid_card(drawn_card, game["last_played_card"]):
                await interaction.response.send_message(
                    f"🎴 You drew {drawn_card}. You can play this card or use `/pass_turn` to skip your turn.",
                    ephemeral=True,
                )
                logger.info(f"Card '{drawn_card}' is valid for play by {interaction.user}.")
            else:
                await interaction.response.send_message(
                    f"🎴 You drew {drawn_card}, but it cannot be played. Your turn will be skipped.",
                    ephemeral=True,
                )
                logger.info(
                    f"Card '{drawn_card}' drawn by {interaction.user} is not valid for play. Turn will be skipped."
                )
                # Advance the turn
                await UnoGameManager.advance_turn(game)

            # Update the sticky display for the game
            await UnoGameManager.update_sticky_display(game)
            logger.info(
                f"Sticky display updated after {interaction.user} drew a card in channel {interaction.channel.id}.")

        except Exception as e:
            logger.error(f"An error occurred while {interaction.user} attempted to draw a card: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while drawing your card. Please try again.",
                                                    ephemeral=True)

    @staticmethod
    async def call_uno(interaction: discord.Interaction, target_player: discord.Member):
        """
        Handles the UNO call-out action. Can be used to call a player who forgot to declare UNO.
        """
        logger.info(
            f"User {interaction.user} is attempting to call UNO on {target_player} in channel {interaction.channel.id}.")

        try:
            # Retrieve the game from the manager
            game = UnoGameManager.uno_games.get(interaction.channel.id)

            # Validation: Check if there's an active game and if the game has started
            if not game:
                logger.warning(f"No active game found in channel {interaction.channel.id}.")
                return await interaction.response.send_message("Call Uno: ❌ No active game in this channel.",
                                                               ephemeral=True)
            if not game["started"]:
                logger.warning(f"Game in channel {interaction.channel.id} has not started yet.")
                return await interaction.response.send_message("❌ The game hasn't started yet!", ephemeral=True)

            # Find the target player in the game
            called_player = next((p for p in game["players"] if p.user == target_player), None)
            if not called_player:
                logger.warning(f"User {target_player} is not part of the game in channel {interaction.channel.id}.")
                return await interaction.response.send_message(f"❌ {target_player.mention} is not part of this game.",
                                                               ephemeral=True)

            # Check if there's a pending penalty and if the called player is the one who forgot
            if not game.get("pending_uno_penalty"):
                logger.info(f"No penalty is pending for any player in channel {interaction.channel.id}.")
                return await interaction.response.send_message(
                    f"❌ No player can be penalized for missing UNO right now.", ephemeral=True)
            if game["pending_uno_penalty"] != called_player:
                logger.info(
                    f"{target_player} was not the player who forgot to call UNO in channel {interaction.channel.id}.")
                return await interaction.response.send_message(f"❌ {target_player.mention} did not forget to call UNO.",
                                                               ephemeral=True)

            # Apply the penalty: Add penalty cards to the player's hand
            penalty_cards = 2
            for _ in range(penalty_cards):
                called_player.add_card(game["deck"].draw())
            logger.info(
                f"{target_player} received a penalty of {penalty_cards} cards in channel {interaction.channel.id}.")

            # Notify the game channel
            await interaction.channel.send(
                f"🔔 {interaction.user.mention} called UNO on {target_player.mention}! "
                f"{target_player.mention} forgot to call UNO and draws {penalty_cards} cards."
            )

            # Clear the pending penalty state
            game["pending_uno_penalty"] = None

            # Update the sticky display
            await UnoGameManager.update_sticky_display(game)
            logger.info(
                f"Sticky display updated after UNO was called on {target_player} in channel {interaction.channel.id}.")

            # Notify the caller
            await interaction.response.send_message(f"✅ You successfully called UNO on {target_player.mention}!",
                                                    ephemeral=True)

        except Exception as e:
            logger.error(f"An error occurred while calling UNO on {target_player}: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while calling UNO. Please try again.",
                                                    ephemeral=True)

    @staticmethod
    async def declare_uno(interaction: discord.Interaction):
        """
        Allows a player to declare UNO when they have two cards left.
        """
        logger.info(f"User {interaction.user} is attempting to declare UNO in channel {interaction.channel.id}.")

        try:
            # Retrieve the game from the manager
            game = UnoGameManager.uno_games.get(interaction.channel.id)

            # Validation: Check if there's an active game and if the game has started
            if not game:
                logger.warning(f"No active game found in channel {interaction.channel.id}.")
                return await interaction.followup.send("Declare Uno: ❌ No active game in this channel.",
                                                       ephemeral=True)
            if not game["started"]:
                logger.warning(f"Game in channel {interaction.channel.id} has not started yet.")
                return await interaction.followup.send("❌ The game hasn't started yet!", ephemeral=True)

            # Find the player in the game
            player = next((p for p in game["players"] if p.user == interaction.user), None)
            if not player or player.user is None:
                logger.error(
                    f"User {interaction.user} does not have a valid Player object in channel {interaction.channel.id}.")
                return await interaction.followup.send("❌ You are not part of this game.", ephemeral=True)

            # Validate the number of cards in the player's hand
            if len(player.hand) != 2:
                logger.info(f"User {interaction.user} attempted to declare UNO with {len(player.hand)} cards in hand.")
                return await interaction.followup.send(
                    "❌ You can only call UNO when you have exactly 2 cards left!", ephemeral=True
                )

            # Check if the player has already declared UNO
            if getattr(player, "declared_uno", False):
                logger.info(f"User {interaction.user} has already declared UNO in channel {interaction.channel.id}.")
                return await interaction.followup.send("❌ You have already declared UNO!", ephemeral=True)

            # Mark the player as having declared UNO
            setattr(player, "declared_uno", True)
            logger.info(f"User {interaction.user} successfully declared UNO in channel {interaction.channel.id}.")

            # Remove any pending penalty for failing to declare UNO
            pending_penalty_player = game.get("pending_uno_penalty")

            if pending_penalty_player is not None:
                # Check if pending penalty matches current player
                if pending_penalty_player == player:
                    game["pending_uno_penalty"] = None
                    logger.info(
                        f"Pending UNO penalty removed for {interaction.user} in channel {interaction.channel.id}.")
                else:
                    logger.debug(f"Pending UNO penalty does not match {interaction.user}.")

            # Notify the user and the game channel
            await interaction.followup.send(f"🃏 {interaction.user.mention} has declared UNO!")

            # Update the sticky display
            await UnoGameManager.update_sticky_display(game)
            logger.info(
                f"Sticky display updated after {interaction.user} declared UNO in channel {interaction.channel.id}.")

        except Exception as e:
            logger.error(f"An error occurred while declaring UNO for {interaction.user}: {e}", exc_info=True)
            await interaction.followup.send("❌ An error occurred while declaring UNO. Please try again.",
                                            ephemeral=True)

    @staticmethod
    async def update_declare_uno(player):
        """
        Marks the player as having declared UNO.
        """
        try:
            setattr(player, "declared_uno", True)
            logger.info(f"Player {player.user} has been marked as having declared UNO.")
        except Exception as e:
            logger.error(f"An error occurred while updating declare UNO status for {player.user}: {e}", exc_info=True)

    @staticmethod
    async def pass_turn(interaction: discord.Interaction):
        """
        Allows a player to pass their turn.
        """
        logger.info(f"User {interaction.user} is attempting to pass their turn in channel {interaction.channel.id}.")

        try:
            # Retrieve the game from the manager
            game = UnoGameManager.uno_games.get(interaction.channel.id)

            # Validation: Ensure the game exists and has started
            if not game:
                logger.warning(f"No active game found in channel {interaction.channel.id}.")
                return await interaction.response.send_message("Pass Turn: ❌ No active game in this channel.",
                                                               ephemeral=True)
            if not game["started"]:
                logger.warning(f"Game in channel {interaction.channel.id} has not started yet.")
                return await interaction.response.send_message("❌ The game hasn't started yet!", ephemeral=True)

            # Ensure the user is part of the game
            player = next((p for p in game["players"] if p.user == interaction.user), None)
            if not player:
                logger.warning(f"User {interaction.user} is not part of the game in channel {interaction.channel.id}.")
                return await interaction.response.send_message("❌ You are not part of this game.", ephemeral=True)

            # Ensure it is the user's turn
            if game["players"][game["turn_index"]] != player:
                logger.info(
                    f"User {interaction.user} attempted to pass out of turn in channel {interaction.channel.id}.")
                return await interaction.response.send_message("❌ It's not your turn!", ephemeral=True)
            # Check if the player can play a card
            top_card = game["discard_pile"][-1] if game["discard_pile"] else game["last_played_card"]
            if any(can_play_card(card, top_card) for card in player.hand):
                # Player has a valid card but still decided to pass
                logger.warning(
                    f"User {interaction.user} could play a card but chose to pass in channel {interaction.channel.id}.")

                # Penalize by giving 2 cards
                for _ in range(2):
                    player.add_card(game["deck"].draw())
                await interaction.response.send_message(
                    "❌ You could have played but passed anyway. You received 2 penalty cards.")
            else:
                # Player has no playable card, passing is valid but still shouldn't
                for _ in range(1):
                    player.add_card(game["deck"].draw())
                logger.info(
                    f"User {interaction.user} passed their turn legitimately in channel {interaction.channel.id}.")
                await interaction.response.send_message("↩️ You passed your turn.")

            # Pass the turn to the next player
            await UnoGameManager.advance_turn(game)

            logger.info(f"Turn successfully passed by {interaction.user} in channel {interaction.channel.id}.")

            # Update the sticky display
            await UnoGameManager.update_sticky_display(game)
            logger.info(
                f"Sticky display updated after {interaction.user} passed their turn in channel {interaction.channel.id}.")

            # Notify the user
            await interaction.response.send_message(f"↩️ You passed your turn.")
            return None

        except Exception as e:
            logger.error(f"An error occurred while {interaction.user} attempted to pass their turn: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while passing your turn. Please try again.",
                                                    ephemeral=True)

    @staticmethod
    async def prompt_color_selection(interaction: discord.Interaction, player):
        """
        Prompts the player to select a color after playing a Wild or +4 card.
        """

        class ColorSelectView(discord.ui.View):
            def __init__(self, authorized_player_id: int):
                super().__init__(timeout=30)  # Timeout after 30 seconds
                self.result = None  # Store the selected emoji here
                self.authorized_player_id = authorized_player_id

            @discord.ui.select(
                placeholder="Select a color...",
                options=[
                    discord.SelectOption(label="Red", value="🔴", emoji="🔴"),
                    discord.SelectOption(label="Green", value="🟢", emoji="🟢"),
                    discord.SelectOption(label="Blue", value="🔵", emoji="🔵"),
                    discord.SelectOption(label="Yellow", value="🟡", emoji="🟡"),
                ],
            )
            async def select_callback(self, interaction: discord.Interaction, select: discord.ui.Select):
                # Check if the user is authorized to make this selection
                if interaction.user.id != self.authorized_player_id:
                    await interaction.response.send_message(
                        "❌ Only the current player can select a color!", ephemeral=True
                    )
                    return

                self.result = select.values[0]  # Store the selected emoji (e.g., '🔴')
                logger.info(f"{interaction.user} selected color: {self.result}.")
                await interaction.response.defer()  # Acknowledge the interaction without sending another message
                self.stop()  # Stop waiting for further input

        # Create the selection view with the player's ID and send it
        view = ColorSelectView(player.user.id)
        await interaction.followup.send("🎨 Please select a color from the choices below:", view=view, ephemeral=True)

        # Wait for the player to make a selection or timeout
        await view.wait()

        # Check what happened: timeout or valid selection
        if not view.result:
            logger.warning(f"{player.user} did not select a color in time!")
            # Notify the player and return None
            await interaction.followup.send("⏳ You did not select a color in time. Your turn has ended.",
                                            ephemeral=True)
            return None

        # Return the selected color as an emoji (e.g., '🔴')
        return view.result

    @staticmethod
    async def quit_game(self, interaction: discord.Interaction):
        """
        Allow a player to quit a game. If only one player is left, end the game.
        If the owner quits and there are more players, transfer ownership.
        """
        logger.info(f"User {interaction.user} is attempting to quit the game in channel {interaction.channel.id}.")

        try:
            # Retrieve the game
            game = UnoGameManager.uno_games.get(interaction.channel.id)
            if not game:
                logger.warning(f"No active game found in channel {interaction.channel.id}.")
                await interaction.response.send_message("No game found in this channel!", ephemeral=True)
                return

            # Check if the player is part of the game
            player_to_quit = next((p for p in game["players"] if p.user == interaction.user), None)
            if not player_to_quit:
                logger.warning(f"User {interaction.user} tried to quit the game without being a participant.")
                await interaction.response.send_message("❌ You are not in the game!", ephemeral=True)
                return

            # Remove player from the game
            game["players"].remove(player_to_quit)
            await interaction.response.send_message(f"{interaction.user.mention} has quit the game.", ephemeral=True)
            logger.info(f"User {interaction.user} successfully quit the game in channel {interaction.channel.id}.")

            # Handle owner leaving
            if game["owner"] == interaction.user:
                if len(game["players"]) >= 2:
                    # Transfer ownership to the next player
                    game["owner"] = game["players"][0].user
                    await interaction.channel.send(
                        f"The game owner has left. {game['owner'].mention} is now the new owner!"
                    )
                    logger.info(f"Ownership transferred to {game['owner']} in channel {interaction.channel.id}.")
                else:
                    # Not enough players left, end the game
                    await interaction.channel.send("❌ Not enough players left to continue. The game has ended.")
                    await UnoGameManager.end_game(game)
                    logger.info(f"Game ended as only one or no players were left in channel {interaction.channel.id}.")
                    return

            # End the game if fewer than two players are left
            if len(game["players"]) <= 1:
                await interaction.channel.send("❌ Not enough players left to continue. The game has ended.")
                await UnoGameManager.end_game(game)
                logger.info(f"Game ended in channel {interaction.channel.id} after player quit.")
                return

            # Update sticky display after a player quits
            await UnoGameManager.update_sticky_display(game)
            logger.info(
                f"Sticky display updated after {interaction.user} quit the game in channel {interaction.channel.id}.")

        except Exception as e:
            logger.error(f"An error occurred while handling quit_game: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while quitting the game. Please try again.",
                                                    ephemeral=True)

    @staticmethod
    async def cancel_game(self, interaction: discord.Interaction):
        """
        Allow the game owner to cancel the game.
        """
        logger.info(f"User {interaction.user} is attempting to cancel the game in channel {interaction.channel.id}.")

        try:
            # Retrieve the game
            game = UnoGameManager.uno_games.get(interaction.channel.id)
            if not game:
                logger.warning(f"No active game found in channel {interaction.channel.id}.")
                await interaction.response.send_message("Cancel Game: ❌ No active game in this channel.",
                                                        ephemeral=True)
                return

            # Ensure only the game owner can cancel the game
            if interaction.user != game["owner"]:
                logger.warning(f"User {interaction.user} tried to cancel the game without being the owner.")
                await interaction.response.send_message(
                    "❌ Only the game owner can cancel this UNO game!", ephemeral=True
                )
                return

            # Notify players about game cancellation
            await interaction.response.send_message(f"✋ {interaction.user.mention} has canceled the game.",
                                                    ephemeral=True)
            await interaction.channel.send("🚫 The UNO game has been canceled by the owner.")
            logger.info(f"Game in channel {interaction.channel.id} was canceled by the owner {interaction.user}.")

            # Perform cleanup and end the game
            await UnoGameManager.end_game(game)
            logger.info(f"Game successfully ended after cancellation in channel {interaction.channel.id}.")

        except Exception as e:
            logger.error(f"An error occurred while canceling the game: {e}", exc_info=True)
            await interaction.response.send_message("❌ An error occurred while canceling the game. Please try again.",
                                                    ephemeral=True)

    @staticmethod
    async def cleanup_game(game, notify_channel=True, reason=None):
        """
        Cleans up an ongoing game and deletes resources like text/voice channels.

        Arguments:
        - game: The game instance to clean up.
        - notify_channel: Whether to notify the channel about the cleanup.
        - reason: The reason for cleanup, if applicable.
        """
        logger.info(f"Cleaning up game in channel {game.get('channel', 'unknown')}.")

        try:
            if notify_channel and game.get("channel"):
                try:
                    message = "🧹 Cleaning up and ending the game."
                    if reason:
                        message += f" Reason: {reason}"
                    await game["channel"].send(message)
                    logger.info("Cleanup notification sent to the game channel.")
                except Exception as e:
                    logger.error(f"Failed to send cleanup message in channel {game['channel']}: {e}", exc_info=True)

            # Perform actual cleanup - Free game resources and remove it from the games list
            await UnoGameManager.end_game(game)
            logger.info("Game resources have been successfully cleaned up.")

        except Exception as e:
            logger.error(f"An error occurred during game cleanup: {e}", exc_info=True)

    @staticmethod
    async def delete_all(interaction: discord.Interaction):
        """
        Deletes all active Uno games and their associated channels.
        """
        logger.info(f"User {interaction.user} is attempting to delete all active Uno games.")

        try:
            await interaction.response.defer()

            # If there are no active games
            if not UnoGameManager.uno_games:
                logger.info("No active Uno games to delete.")
                await interaction.followup.send("No active games to delete.", ephemeral=True)
                return

            # Iterate through all the active games
            for guild_id, game in list(UnoGameManager.uno_games.items()):
                try:
                    # Attempt to delete the associated text channel
                    if game.get("channel"):
                        await game["channel"].delete()
                        logger.info(f"Deleted text channel {game['channel'].name} for guild {guild_id}.")
                    else:
                        logger.warning(f"No text channel associated with the game in guild {guild_id}.")

                    # Attempt to delete the associated voice channel
                    if game.get("voice_channel"):
                        await game["voice_channel"].delete()
                        logger.info(f"Deleted voice channel {game['voice_channel'].name} for guild {guild_id}.")
                    else:
                        logger.warning(f"No voice channel associated with the game in guild {guild_id}.")

                    # Remove the game from memory
                    await UnoGameManager.cancel_game(interaction)
                    logger.info(f"Cleaned up game data for guild {guild_id}.")

                except Exception as game_error:
                    logger.error(f"Failed to fully delete game resources for guild {guild_id}: {game_error}",
                                 exc_info=True)

            # Notify the user that all active games have been deleted
            await interaction.followup.send("All active Uno games and their channels have been deleted.",
                                            ephemeral=True)
            logger.info("All active Uno games have been successfully deleted.")

        except Exception as e:
            logger.error(f"An error occurred while deleting all games: {e}", exc_info=True)
            await interaction.followup.send("❌ An error occurred while deleting games. Please try again later.",
                                            ephemeral=True)


class JoinGameView(View):
    """
    A view that holds a Join Game button linked to a callback.
    """

    def __init__(self, uno_manager, channel_id):
        super().__init__(timeout=600)  # View times out after 5 minutes (300 seconds)
        self.uno_manager = uno_manager  # Pass the UnoGameManager instance
        self.channel_id = channel_id  # The channel ID where the game resides

    @discord.ui.button(label="Join Game", style=discord.ButtonStyle.success)
    async def join_game_button(self, interaction: discord.Interaction, button: Button):
        """
        Callback for the 'Join Game' button. Invokes the join_game_button_callback.
        """
        await self.uno_manager.join_game_button_callback(interaction)

    async def on_timeout(self):
        """
        Called when the view times out (after the specified timeout duration).
        Used to notify players and/or clean up resources.
        """
        logger.info(f"JoinGameView in channel {self.channel_id} has timed out.")

        try:
            # Check if the channel exists before attempting to notify
            channel = self.uno_manager.get_channel(self.channel_id)
            if channel:
                await channel.send("🕒 The join game prompt has timed out. The game is now closed for new players.")
            else:
                logger.warning(f"Channel with ID {self.channel_id} does not exist. Skipping notification.")
        except Exception as e:
            logger.error(f"Error when trying to notify about view timeout: {e}")
        finally:
            # Stop the view to release resources
            self.stop()


async def wait_for_turn_timeout(game, turn_index):
    """
    Handles turn timeout for a player during the game. Skips the player's turn if they take too long
    and removes them from the game if they reach the maximum skip limit.
    """
    try:
        timeout_duration = 60  # 1-minute timeout
        await asyncio.sleep(timeout_duration)

        # Verify that it's still the player's turn after the timeout
        if game["turn_index"] != turn_index:
            return

        # Get the current player
        player = game["players"][turn_index]
        player.skips += 1

        # Notify about the timeout
        await game["channel"].send(
            f"⏱️ {player.user.mention} took too long and was skipped! (⏭️ {player.skips}/5)"
        )

        # Check if player has exceeded the skip limit
        if player.skips >= 5:  # Assuming 5 is the max skip limit
            await game["channel"].send(
                f"🚫 {player.user.mention} has been removed from the game for being skipped too many times."
            )

            # Check if the player is the owner
            is_owner = player.user == game["owner"]

            # Remove the player from the game
            game["players"].remove(player)

            # Check if enough players remain to continue the game
            if len(game["players"]) < 2:
                await game["channel"].send(
                    "❌ Not enough players to continue the game. The game has ended.\n"
                    "🧹 Cleaning up and ending the game."
                )

                # Perform cleanup
                await cleanup_game(game)
                return

            # Reassign game owner if the removed player was the owner
            if is_owner:
                new_owner = game["players"][0]
                game["owner"] = new_owner.user
                await game["channel"].send(f"👑 {new_owner.user.mention} is now the new game owner.")

            # Adjust turn index if necessary
            if turn_index >= len(game["players"]):
                game["turn_index"] = 0  # Reset to the first player if the index is out of range

        else:
            # Move to the next player if the current player hasn't been removed
            game["turn_index"] = (game["turn_index"] + 1) % len(game["players"])

        # Update the game's UI and state
        await UnoGameManager.update_sticky_display(game)

    except asyncio.CancelledError:
        # Gracefully handle task cancellation
        pass

    except Exception as e:
        # Log unexpected errors
        logger.error(f"Unexpected error in wait_for_turn_timeout: {e}", exc_info=True)


async def cleanup_game(game):
    """
    Cleans up a completed or canceled game by removing channels, resources,
    or other game-related data and freeing memory.
    """
    try:
        logger.info(f"Initiating cleanup for game in channel {game['channel'].id}.")

        # Remove game from active tracking
        UnoGameManager.uno_games.pop(game["channel"].id, None)
        logger.info("Game removed from active games.")

        # Delete the voice channel if exists
        if game.get("voice_channel"):
            try:
                await game["voice_channel"].delete()
                logger.info(f"Voice channel {game['voice_channel'].id} deleted.")
            except discord.HTTPException as e:
                logger.error(f"Failed to delete voice channel: {e}")

        # Delete the text channel
        if game.get("channel"):
            try:
                await game["channel"].delete()
                logger.info(f"Text channel {game['channel'].id} deleted.")
            except discord.HTTPException as e:
                logger.error(f"Failed to delete text channel: {e}")

        # Cancel any active turn timeout task
        if game.get("turn_timeout_task") and not game["turn_timeout_task"].done():
            game["turn_timeout_task"].cancel()
            logger.info("Turn timeout task canceled during cleanup.")

    except Exception as e:
        logger.error(f"Unexpected error during game cleanup: {e}", exc_info=True)

