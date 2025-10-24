# Python
import logging
import os
import asyncio
from asyncio import Lock, Queue
import re
import random
import discord
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from discord.ext import commands

from commands.games.MasterCache import MasterCache
from storage.config_system import config
from utilities.logger_setup import get_logger, log_performance, PerformanceLogger

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

logger = get_logger("CountingGame", level=logging.DEBUG)

ASCII_DIGITS_RE = re.compile(r"^[0-9]+$")


class CountingGame(commands.Cog, name="CountingGame"):
    def __init__(self):
        logger.info("Initializing CountingGame cog...")
        logger.debug("CountingGame.__init__()")
        self.db_client = None

        # Collections (assigned in cog_load)
        self.state = None
        self.LB = None
        self.INVENTORY = None

        # Centralized cache
        self.cache: MasterCache | None = None

        # Multi-channel support: configure one or more counting channel IDs
        self.count_channel_ids = config.count_channel_ids
        logger.info(f"Configured counting channels: {self.count_channel_ids}")

        # Roles
        self.out_of_order_role_id = config.out_of_order_role_id
        self.milestone_role_id = config.milestone_role_id
        self.master_counter_id = config.master_counter_id

        # Tunable rules
        self.double_post_grace_seconds = config.double_post_grace_seconds
        self.idle_grace_seconds = config.idle_grace_seconds
        self.streak_protect_window = config.streak_protect_window
        self.max_digits = config.max_digits

        logger.info(f"Counting rules: double_post={self.double_post_grace_seconds}s, "
                    f"idle={self.idle_grace_seconds}s, streak_window={self.streak_protect_window}, "
                    f"max_digits={self.max_digits}")

        self.user_cooldown = {}  # {channel_id: {user_id: timestamp}}
        self.channel_queues = {}  # {channel_id: asyncio.Queue}
        self.locks = {}  # {channel_id: Lock}

        # Idle/double-post ready reaction schedulers
        self.idle_tasks: dict[int, asyncio.Task] = {}
        self.idle_reactions = ["👀", "⏰", "🕒", "🧮", "🤖", "✨", "🔔", "🫠"]
        self.double_post_ready_tasks: dict[int, asyncio.Task] = {}

        # Performance optimizations
        self._leaderboard_cache = None
        self._last_lb_update = 0
        self._lb_cache_ttl = 30.0  # Cache leaderboard for 30 seconds

        self.auto_verify_task = None
        self.auto_verify_interval = config.auto_verify_interval

        logger.debug("CountingGame initialization completed")

    @log_performance("cog_load")
    async def cog_load(self):
        logger.info("CountingGame.cog_load() starting...")
        startup_metrics = {
            'mongo_connections': 0,
            'collections_bound': 0,
            'cache_started': False,
            'states_preloaded': 0,
            'indexes_created': False
        }

        try:
            # Database connections
            logger.info("Connecting to MongoDB...")
            self.db_client = AsyncIOMotorClient(MONGO_URI)
            startup_metrics['mongo_connections'] = 1
            logger.info("✅ MongoDB clients initialized successfully")

            # Collections
            logger.info("Binding collections...")
            self.state = self.db_client["Game-State"]["Counting"]
            self.LB = self.db_client["LeaderBoard"]["Counting"]
            startup_metrics['collections_bound'] = 2
            logger.info("✅ Collections bound successfully")

            # Initialize cache and background flush
            logger.info("Starting MasterCache...")
            with PerformanceLogger(logger, "MasterCache_initialization"):
                self.cache = MasterCache(
                    state_collection=self.state,
                    leaderboard_collection=self.LB,
                    flush_interval=10.0,
                    state_ttl=5.0,
                    leaderboard_ttl=5.0,
                )
                await self.cache.start()
                startup_metrics['cache_started'] = True
            logger.info("✅ MasterCache started successfully")

            # Preload data
            logger.info("Preloading cache data...")
            try:
                with PerformanceLogger(logger, "cache_preload"):
                    await self.cache.preload_states(self.count_channel_ids)
                    startup_metrics['states_preloaded'] = len(self.count_channel_ids)
                    logger.info(
                        f"✅ States preloaded for {len(self.count_channel_ids)} channels: {self.count_channel_ids}")

                    await self.cache.preload_leaderboard()
                    logger.info("✅ Leaderboard preloaded successfully")
            except Exception as e:
                logger.error(f"❌ Failed to preload cache: {e}", exc_info=True)

            # Cache dump
            try:
                with PerformanceLogger(logger, "initial_cache_dump"):
                    p = await self.cache.dump_to_file("cache/counting_live_cache.json", include_meta=True)
                    logger.info(f"✅ Initial cache dump written to {p}")
            except Exception as e:
                logger.error(f"❌ Failed to dump cache to file: {e}", exc_info=True)

            # Create indexes
            await self.create_indexes()
            startup_metrics['indexes_created'] = True

            # Start automatic verification
            await self.start_auto_verification()

            logger.info("🎉 CountingGame cog loaded successfully!")
            logger.info(f"Startup metrics: {startup_metrics}")

        except Exception as e:
            logger.critical(f"💥 CRITICAL: Failed to initialize CountingGame cog: {e}", exc_info=True)
            logger.error(f"Startup failed with metrics: {startup_metrics}")
            raise

    @log_performance("create_indexes")
    async def create_indexes(self):
        logger.info("Creating database indexes...")
        try:
            indexes_created = []

            # Leaderboard indexes for new structure (one doc per user)
            await self.LB.create_index([("user_id", 1)], unique=True)
            indexes_created.append("user_id(unique)")

            await self.LB.create_index([("count", -1)])
            indexes_created.append("count(desc)")

            logger.info(f"✅ Successfully created {len(indexes_created)} indexes: {', '.join(indexes_created)}")
        except Exception as e:
            logger.error(f"❌ Error creating indexes: {e}", exc_info=True)
            raise

    async def cog_unload(self):
        logger.info("CountingGame.cog_unload() starting...")
        cleanup_metrics = {
            'idle_tasks_cancelled': 0,
            'double_post_tasks_cancelled': 0,
            'cache_shutdown': False,
            'db_connections_closed': 0
        }

        try:
            # Cancel idle timers
            logger.info("Cancelling idle tasks...")
            for ch_id, task in list(self.idle_tasks.items()):
                if not task.done():
                    logger.debug(f"Cancelling idle task for channel {ch_id}")
                    task.cancel()
                    cleanup_metrics['idle_tasks_cancelled'] += 1
            self.idle_tasks.clear()

            # Cancel double-post timers
            logger.info("Cancelling double-post tasks...")
            for ch_id, task in list(self.double_post_ready_tasks.items()):
                if not task.done():
                    logger.debug(f"Cancelling double-post-ready task for channel {ch_id}")
                    task.cancel()
                    cleanup_metrics['double_post_tasks_cancelled'] += 1
            self.double_post_ready_tasks.clear()

            # Clear cache
            self._leaderboard_cache = None

            # Shutdown cache
            if self.cache:
                logger.info("Shutting down cache...")
                await self.cache.shutdown()
                cleanup_metrics['cache_shutdown'] = True
                logger.info("✅ Cache shutdown completed")

        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}", exc_info=True)
        finally:
            # Close database connections
            if self.db_client:
                self.db_client.close()
                cleanup_metrics['db_connections_closed'] += 1
                logger.debug("Primary MongoDB client closed")

            logger.info(f"🏁 CountingGame cog unloaded. Cleanup metrics: {cleanup_metrics}")

    async def get_cached_state(self, channel_id: int):
        logger.debug(f"get_cached_state(channel_id={channel_id})")
        try:
            state = await self.cache.get_state(channel_id)
            logger.debug(f"Retrieved state for channel {channel_id}: last_number={state.get('last_number', 'N/A')}")
            return state
        except Exception as e:
            logger.error(f"❌ Failed to get cached state for channel {channel_id}: {e}", exc_info=True)
            raise

    async def save_cached_state(self, channel_id: int, partial_update: dict | None = None):
        logger.debug(
            f"save_cached_state(channel_id={channel_id}, keys={list(partial_update.keys()) if partial_update else []})")
        try:
            if partial_update:
                await self.cache.update_state(channel_id, partial_update)
                logger.debug(f"✅ State updated for channel {channel_id}")
        except Exception as e:
            logger.error(f"❌ Failed to save cached state for channel {channel_id}: {e}", exc_info=True)
            raise

    # =========================================================================
    # ADD VERIFICATION AND CORRECTION METHODS HERE (after existing methods)
    # =========================================================================
    @log_performance("verify_counting_state")
    async def verify_counting_state(self, channel_id: int) -> dict:
        """
        Verify that the counting state matches the sum of all counts in leaderboard.
        Returns verification results with discrepancies found.
        """
        logger.info(f"🔍 Verifying counting state for channel {channel_id}")

        try:
            # Get current state
            state = await self.get_cached_state(channel_id)
            current_number = state.get("last_number", 0)

            # Calculate total counts from leaderboard
            lb = await self.get_cached_leaderboard()
            total_counts = sum(lb.values())

            # Calculate expected current number (starts from 0, so total_counts should equal current_number)
            expected_number = total_counts

            discrepancy = current_number - expected_number
            is_correct = discrepancy == 0

            result = {
                "channel_id": channel_id,
                "current_number": current_number,
                "total_counts": total_counts,
                "expected_number": expected_number,
                "discrepancy": discrepancy,
                "is_correct": is_correct,
                "user_counts": len(lb),
                "verification_time": discord.utils.utcnow().isoformat()
            }

            logger.info(
                f"📊 Verification result for channel {channel_id}: "
                f"current={current_number}, expected={expected_number}, "
                f"discrepancy={discrepancy}, correct={is_correct}"
            )

            return result

        except Exception as e:
            logger.error(f"❌ Verification failed for channel {channel_id}: {e}", exc_info=True)
            raise

    @log_performance("correct_counting_state")
    async def correct_counting_state(self, channel_id: int, announcement_channel: discord.TextChannel = None) -> dict:
        """
        Correct the counting state to match the total leaderboard counts.
        Returns correction details.
        """
        logger.info(f"🔧 Correcting counting state for channel {channel_id}")

        try:
            # Verify first to get the discrepancy
            verification = await self.verify_counting_state(channel_id)

            if verification["is_correct"]:
                logger.info(f"✅ Channel {channel_id} is already correct, no correction needed")
                return {
                    **verification,
                    "corrected": False,
                    "correction_applied": 0,
                    "message": "No correction needed - state is already correct"
                }

            # Calculate correction
            total_counts = verification["total_counts"]
            correction_needed = verification["discrepancy"]

            # Apply correction by updating the state to match total counts
            await self.save_cached_state(channel_id, {
                "last_number": total_counts,
                "last_user_id": verification.get("last_user_id"),  # Preserve last user if possible
                "correction_applied": True,
                "corrected_from": verification["current_number"],
                "corrected_to": total_counts,
                "correction_time": discord.utils.utcnow().isoformat()
            })

            # Force immediate cache flush to ensure correction is persisted
            if self.cache:
                await self.cache.flush_once()

            # Verify correction was applied
            post_correction = await self.verify_counting_state(channel_id)

            result = {
                **post_correction,
                "corrected": True,
                "correction_applied": correction_needed,
                "previous_number": verification["current_number"],
                "message": f"Corrected counting state from {verification['current_number']} to {total_counts}"
            }

            logger.info(
                f"✅ Correction applied to channel {channel_id}: "
                f"{verification['current_number']} → {total_counts} "
                f"(adjustment: {correction_needed})"
            )

            # Send announcement to the counting channel itself so everyone sees it
            # Get the bot instance to fetch the channel
            from utilities.bot import bot  # Adjust import based on your bot structure
            counting_channel = bot.get_channel(channel_id)
            if counting_channel:
                await self._announce_correction(counting_channel, result)
                logger.info(f"📢 Correction announced in counting channel {channel_id}")
            else:
                logger.warning(f"❌ Could not find counting channel {channel_id} for announcement")

            # Also send to admin channel if provided and it's different from counting channel
            if announcement_channel and announcement_channel.id != channel_id:
                await self._announce_correction(announcement_channel, result, is_admin=True)
                logger.info(f"📢 Correction announced in admin channel {announcement_channel.id}")

            return result

        except Exception as e:
            logger.error(f"❌ Correction failed for channel {channel_id}: {e}", exc_info=True)
            raise

    async def _announce_correction(self, channel: discord.TextChannel, correction_data: dict, is_admin: bool = False):
        """Announce state correction to a channel - different message for counting channel vs admin"""
        try:
            if is_admin:
                # Admin announcement - detailed technical info
                embed = discord.Embed(
                    title="🔧 Counting State Correction Applied",
                    color=discord.Color.orange(),
                    timestamp=discord.utils.utcnow()
                )

                embed.add_field(
                    name="Correction Details",
                    value=(
                        f"**Previous Number:** `{correction_data['previous_number']}`\n"
                        f"**Corrected Number:** `{correction_data['current_number']}`\n"
                        f"**Adjustment:** `{correction_data['correction_applied']}`\n"
                        f"**Total User Counts:** `{correction_data['total_counts']}`"
                    ),
                    inline=False
                )

                embed.add_field(
                    name="Verification",
                    value=(
                        f"**Users in Leaderboard:** `{correction_data['user_counts']}`\n"
                        f"**Status:** `{'✅ Corrected' if correction_data['corrected'] else '❌ Failed'}`"
                    ),
                    inline=False
                )

                embed.set_footer(text="Automatic counting state verification")

            else:
                # Public announcement in counting channel - simple and clear
                previous = correction_data['previous_number']
                current = correction_data['current_number']
                direction = "increased" if current > previous else "decreased"

                embed = discord.Embed(
                    title="🔧 Counting Game Correction",
                    color=discord.Color.gold(),
                    timestamp=discord.utils.utcnow()
                )

                embed.add_field(
                    name="Game State Updated",
                    value=(
                        f"The counting number has been {direction} to maintain game integrity.\n\n"
                        f"**Previous Number:** `{correction_data['previous_number']}`\n"
                        f"**New Current Number:** `{correction_data['current_number']}`\n\n"
                        f"*Please continue counting from* `{correction_data['current_number'] + 1}`"
                    ),
                    inline=False
                )

                embed.set_footer(text="Automatic system correction applied")

            await channel.send(embed=embed)
            logger.info(f"📢 Correction announced in channel {channel.id} (admin: {is_admin})")

        except Exception as e:
            logger.error(f"❌ Failed to announce correction: {e}")

    @log_performance("verify_and_auto_correct")
    async def verify_and_auto_correct(self, channel_id: int, announcement_channel: discord.TextChannel = None) -> dict:
        """
        Combined verification and auto-correction in one method.
        This is the main method to use for automatic verification.
        """
        logger.info(f"🔄 Verifying and auto-correcting channel {channel_id}")

        try:
            # First verify the state
            verification = await self.verify_counting_state(channel_id)

            # If incorrect, automatically correct it
            if not verification["is_correct"]:
                logger.info(f"🔄 Auto-correcting channel {channel_id} (discrepancy: {verification['discrepancy']})")
                return await self.correct_counting_state(channel_id, announcement_channel)
            else:
                logger.info(f"✅ Channel {channel_id} is correct, no correction needed")
                return {
                    **verification,
                    "corrected": False,
                    "correction_applied": 0,
                    "message": "No correction needed - state is correct"
                }

        except Exception as e:
            logger.error(f"❌ Verify and auto-correct failed for channel {channel_id}: {e}", exc_info=True)
            raise

    @log_performance("verify_all_channels")
    async def verify_all_channels(self, auto_correct: bool = True) -> dict:
        """Verify counting state for all configured channels with optional auto-correction"""
        logger.info(f"🔍 Verifying all {len(self.count_channel_ids)} counting channels (auto_correct: {auto_correct})")

        results = {}
        corrections_applied = []

        for channel_id in self.count_channel_ids:
            try:
                if auto_correct:
                    # Use the combined verify and auto-correct method
                    result = await self.verify_and_auto_correct(channel_id)
                    if result.get("corrected", False):
                        corrections_applied.append({
                            "channel_id": channel_id,
                            "previous": result["previous_number"],
                            "corrected": result["current_number"],
                            "adjustment": result["correction_applied"]
                        })
                else:
                    # Just verify without correcting
                    result = await self.verify_counting_state(channel_id)

                results[channel_id] = result

            except Exception as e:
                results[channel_id] = {
                    "channel_id": channel_id,
                    "error": str(e),
                    "is_correct": False
                }

        # Summary
        correct_channels = [cid for cid, result in results.items() if result.get("is_correct", False)]
        incorrect_channels = [cid for cid, result in results.items() if
                              not result.get("is_correct", False) and "error" not in result]
        error_channels = [cid for cid, result in results.items() if "error" in result]

        summary = {
            "total_channels": len(self.count_channel_ids),
            "correct_channels": len(correct_channels),
            "incorrect_channels": len(incorrect_channels),
            "error_channels": len(error_channels),
            "corrections_applied": corrections_applied,
            "auto_correct": auto_correct,
            "results": results
        }

        logger.info(
            f"📊 All channels verification complete: "
            f"{len(correct_channels)} correct, {len(incorrect_channels)} incorrect, "
            f"{len(error_channels)} errors, {len(corrections_applied)} corrections applied"
        )

        return summary

    async def start_auto_verification(self):
        """Start automatic periodic verification with auto-correction"""
        if self.auto_verify_task and not self.auto_verify_task.done():
            self.auto_verify_task.cancel()

        async def auto_verify_loop():
            while True:
                try:
                    await asyncio.sleep(self.auto_verify_interval)
                    logger.info("🔄 Running automatic counting state verification with auto-correction")
                    summary = await self.verify_all_channels(auto_correct=True)

                    # Log summary for monitoring
                    incorrect_count = summary['incorrect_channels'] + summary['error_channels']
                    corrections_count = len(summary['corrections_applied'])

                    if corrections_count > 0:
                        logger.warning(
                            f"⚠️ Auto-verification corrected {corrections_count} channels: "
                            f"{', '.join(str(c['channel_id']) for c in summary['corrections_applied'])}"
                        )
                    elif incorrect_count > 0:
                        logger.warning(
                            f"⚠️ Auto-verification found {incorrect_count} problematic channels "
                            f"(but auto-correction may have been disabled)"
                        )

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"❌ Auto-verification failed: {e}")

        self.auto_verify_task = asyncio.create_task(auto_verify_loop())

    async def check_number(self, message: discord.Message):
        if message.author.bot:
            return
        channel_id = message.channel.id

        # Enhanced logging with message context
        logger.debug(
            f"check_number(channel={channel_id}, author={message.author.id}, "
            f"author_name='{message.author.display_name}', msg_id={message.id}, "
            f"content='{message.content[:50]}{'...' if len(message.content) > 50 else ''}')"
        )

        if channel_id not in self.channel_queues:
            self.channel_queues[channel_id] = Queue()
            logger.info(f"📊 New counting channel activated: {channel_id}")

        if channel_id not in self.locks:
            self.locks[channel_id] = Lock()
            logger.debug(f"🔒 Lock created for channel {channel_id}")

        await self.channel_queues[channel_id].put(message)
        queue_size = self.channel_queues[channel_id].qsize()
        if queue_size > 5:
            logger.warning(f"⚠️ High queue size ({queue_size}) for channel {channel_id}")
        else:
            logger.debug(f"Message enqueued (queue_size={queue_size})")

        async with self.locks[channel_id]:
            processed_count = 0
            while not self.channel_queues[channel_id].empty():
                current_message = await self.channel_queues[channel_id].get()
                try:
                    await self.process_message(current_message)
                    processed_count += 1
                except Exception as e:
                    logger.error(f"❌ Error processing message {current_message.id}: {e}", exc_info=True)

            if processed_count > 1:
                logger.info(f"📦 Batch processed {processed_count} messages for channel {channel_id}")

    def _is_valid_number_text(self, text: str) -> bool:
        """Optimized number validation"""
        if not text or len(text.strip()) > self.max_digits:
            return False
        stripped = text.strip()
        return bool(ASCII_DIGITS_RE.fullmatch(stripped))

    async def process_message(self, message: discord.Message):
        channel_id = message.channel.id
        user = message.author
        user_id = user.id
        current_time = message.created_at.timestamp()

        # Enhanced process logging
        logger.debug(
            f"🔄 process_message(ch={channel_id}, user={user_id}:{user.display_name}, "
            f"msg_id={message.id}, content='{message.content}', timestamp={current_time})"
        )

        # Cooldown check with enhanced logging
        cd_until = self.user_cooldown.get(channel_id, {}).get(user_id, 0)
        if cd_until > current_time:
            cooldown_remaining = cd_until - current_time
            logger.info(
                f"⏳ Cooldown violation: User {user.display_name}({user_id}) has {cooldown_remaining:.1f}s remaining")
            await self.delete_message(message, f"❌ {user.mention} Too fast!")
            return

        self.user_cooldown.setdefault(channel_id, {})[user_id] = current_time + 2
        logger.debug(f"⏱️ Cooldown set for user {user.display_name}({user_id}) until {current_time + 2}")

        # Strict number validation
        content = message.content
        if not self._is_valid_number_text(content):
            logger.info(f"❌ Invalid number format from {user.display_name}({user_id}): '{content}'")
            await self.delete_message(message, f"❌ {user.mention} Invalid number format!")
            return

        # State via cache
        state = await self.get_cached_state(channel_id)
        user_number = int(content.strip())
        expected = state["last_number"] + 1

        # Enhanced state logging
        logger.debug(
            f"📊 Game state: last_number={state['last_number']}, expected={expected}, "
            f"user_number={user_number}, last_user_id={state.get('last_user_id')}, "
            f"current_user={user_id}"
        )

        # Double-post grace and streak protection logic
        is_same_user = (user_id == state["last_user_id"])
        last_ts = state.get("last_message_ts", 0.0)
        since_last = max(0.0, current_time - last_ts)

        # Master counter must wait 3x longer before allowed to double post
        mc_role = user.guild.get_role(self.master_counter_id)
        is_master_counter = bool(mc_role and mc_role in user.roles)
        effective_double_post_grace = (
                self.double_post_grace_seconds * 3) if is_master_counter else self.double_post_grace_seconds

        within_double_post_grace = since_last >= effective_double_post_grace or since_last >= self.idle_grace_seconds
        within_streak_protect = current_time <= float(state.get("grace_until", 0.0))

        # Enhanced double-post logging
        logger.debug(
            f"🔄 Double-post analysis: is_same_user={is_same_user}, is_master={is_master_counter}, "
            f"since_last={since_last:.2f}s, eff_grace={effective_double_post_grace}s, "
            f"idle_grace={self.idle_grace_seconds}s, within_grace={within_double_post_grace}, "
            f"within_streak_protect={within_streak_protect}, expected={expected}, got={user_number}"
        )

        # Determine if double-post by same user is allowed right now
        double_post_allowed = (not is_same_user) or within_double_post_grace or (
                within_streak_protect and user_number == expected)

        # Out-of-order check with detailed reasoning
        if user_number != expected or not double_post_allowed:
            if user_number != expected:
                reason = f"Expected `{expected}`, got `{user_number}`"
                logger.warning(
                    f"❌ Wrong number: User {user.display_name}({user_id}) posted {user_number}, expected {expected}")
            else:
                reason = "No double posting yet! Please wait a bit."
                logger.warning(
                    f"❌ Double-post too soon: User {user.display_name}({user_id}) needs to wait {effective_double_post_grace - since_last:.1f}s more")

            logger.info(f"🚫 Out-of-order by user {user_id} in ch {channel_id}: {reason}")
            await self.handle_out_of_order(message, state, channel_id, reason, current_time)
            return

        # Valid count - update state
        logger.info(f"✅ VALID COUNT: #{user_number} by {user.display_name}({user_id}) in channel {channel_id}")

        await self.save_cached_state(channel_id, {
            "last_number": user_number,
            "last_user_id": user_id,
            "last_message_id": message.id,
            "last_message_ts": current_time,
            "out_of_order_user": None,
            "grace_until": 0.0,
        })

        # Kick off fast, non-blocking verification + acknowledgement
        asyncio.create_task(self._post_acceptance_verification(message, user_number))

        # Leaderboard/roles - non-blocking
        asyncio.create_task(self.update_leaderboard(user_id))
        asyncio.create_task(self.remove_out_of_order_role(user))
        asyncio.create_task(self.update_master_counter_role(message.guild))

        # (re)start idle reaction timer
        self._cancel_idle_task(channel_id)
        self._start_idle_task(message.channel, message.id)

        # Schedule a "double-post ready" second emoji for master counter
        if is_master_counter:
            logger.debug(f"🎯 Master counter {user.display_name}({user_id}) posted, scheduling double-post ready timer")
            self._cancel_double_post_task(channel_id)
            self._start_double_post_ready_task(
                channel=message.channel,
                last_message_id=message.id,
                delay=effective_double_post_grace
            )

        # Optional: celebrate milestones
        if user_number % 100 == 0:
            logger.info(f"🎉 MILESTONE: {user_number} reached by {user.display_name}({user_id})")
            await self.assign_milestone_role(user, message.guild)
        elif user_number % 10 == 0:
            logger.debug(f"📈 Mini milestone: {user_number} by {user.display_name}({user_id})")

    # Idle reaction scheduling
    def _cancel_idle_task(self, channel_id: int):
        task = self.idle_tasks.get(channel_id)
        if task and not task.done():
            logger.debug(f"🚫 Cancelling existing idle task for channel {channel_id}")
            task.cancel()
        self.idle_tasks.pop(channel_id, None)

    def _start_idle_task(self, channel: discord.TextChannel, last_message_id: int):
        logger.debug(
            f"⏰ Scheduling idle reaction for ch={channel.id}, msg_id={last_message_id}, delay={self.idle_grace_seconds}s")

        async def runner():
            try:
                await asyncio.sleep(self.idle_grace_seconds)
                state = await self.get_cached_state(channel.id)
                if int(state.get("last_message_id", 0)) != int(last_message_id):
                    logger.debug("⏰ Idle task abort: message no longer latest")
                    return

                try:
                    msg = await channel.fetch_message(last_message_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                    logger.debug(f"⏰ Idle task fetch_message failed: {e}")
                    return

                emoji = random.choice(self.idle_reactions)
                try:
                    await msg.add_reaction(emoji)
                    logger.info(f"⏰ Idle reaction '{emoji}' added to msg {last_message_id} in ch {channel.id}")
                except discord.Forbidden:
                    logger.warning("⏰ Idle reaction forbidden: missing permissions")
                except discord.HTTPException as e:
                    logger.warning(f"⏰ Idle reaction failed: {e}")
            except asyncio.CancelledError:
                logger.debug("⏰ Idle task cancelled")
                return
            except Exception as e:
                logger.error(f"❌ Idle reaction task error for channel {channel.id}: {e}", exc_info=True)
            finally:
                self.idle_tasks.pop(channel.id, None)

        task = asyncio.create_task(runner(), name=f"idle-reaction-{channel.id}")
        self.idle_tasks[channel.id] = task

    # Double-post ready reaction (master counter only)
    def _cancel_double_post_task(self, channel_id: int):
        task = self.double_post_ready_tasks.get(channel_id)
        if task and not task.done():
            logger.debug(f"🚫 Cancelling existing double-post-ready task for channel {channel_id}")
            task.cancel()
        self.double_post_ready_tasks.pop(channel_id, None)

    def _start_double_post_ready_task(self, channel: discord.TextChannel, last_message_id: int, delay: float):
        logger.debug(
            f"🎯 Scheduling double-post-ready reaction for ch={channel.id}, msg_id={last_message_id}, delay={delay}s")

        async def runner():
            try:
                await asyncio.sleep(max(0.0, float(delay)))
                state = await self.get_cached_state(channel.id)
                if int(state.get("last_message_id", 0)) != int(last_message_id):
                    logger.debug("🎯 Double-post-ready abort: message no longer latest")
                    return

                try:
                    msg = await channel.fetch_message(last_message_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                    logger.debug(f"🎯 Double-post-ready fetch_message failed: {e}")
                    return

                existing = {str(r.emoji) for r in msg.reactions}
                candidates = [e for e in self.idle_reactions if e not in existing]
                if not candidates:
                    candidates = list(self.idle_reactions)

                second = random.choice(candidates)
                if second in existing and len(candidates) > 1:
                    alt = [e for e in candidates if e != second]
                    if alt:
                        second = random.choice(alt)

                try:
                    await msg.add_reaction(second)
                    logger.info(
                        f"🎯 Double-post-ready reaction '{second}' added to msg {last_message_id} in ch {channel.id}")
                except discord.Forbidden:
                    logger.warning("🎯 Double-post-ready reaction forbidden: missing permissions")
                except discord.HTTPException as e:
                    logger.warning(f"🎯 Double-post-ready reaction failed: {e}")
            except asyncio.CancelledError:
                logger.debug("🎯 Double-post-ready task cancelled")
                return
            except Exception as e:
                logger.error(f"❌ Double-post-ready task error for channel {channel.id}: {e}", exc_info=True)
            finally:
                self.double_post_ready_tasks.pop(channel.id, None)

        task = asyncio.create_task(runner(), name=f"double-post-ready-{channel.id}")
        self.double_post_ready_tasks[channel.id] = task

    # --- Acceptance verification and user feedback ---

    async def _post_acceptance_verification(self, message: discord.Message, user_number: int):
        """
        Verify that this message was actually accepted (became the latest valid count).
        If verification fails quickly, notify the user to try again.
        Runs out-of-band to keep processing fast.
        """
        try:
            ok = await self._verify_and_acknowledge(message, user_number)
            if not ok:
                await self._notify_verification_failed(message)
        except Exception as e:
            logger.error(f"❌ post-acceptance verification error for msg {message.id}: {e}", exc_info=True)

    async def _verify_and_acknowledge(self, message: discord.Message, user_number: int,
                                      attempts: int = 6, delay: float = 0.05) -> bool:
        """
        Poll the cached state briefly to confirm this message became the latest.
        Success condition: state's last_message_id == this message.id AND last_number == user_number.
        Returns True if verified, else False. No reactions/messages on success.
        """
        ch_id = message.channel.id
        for _ in range(max(1, attempts)):
            try:
                state = await self.get_cached_state(ch_id)
                if int(state.get("last_message_id", 0)) == int(message.id) and \
                        int(state.get("last_number", -1)) == int(user_number):
                    return True
            except Exception as e:
                logger.debug(f"Verification poll failed for ch {ch_id}, msg {message.id}: {e}")
            await asyncio.sleep(max(0.0, float(delay)))
        return False

    async def _notify_verification_failed(self, message: discord.Message):
        """
        Tell the user we couldn't verify acceptance and show the next expected number.
        Message auto-deletes shortly to reduce noise.
        """
        try:
            state = await self.get_cached_state(message.channel.id)
            next_expected = int(state.get("last_number", -1)) + 1
        except Exception:
            next_expected = "the next number"

        try:
            msg = await message.reply(
                f"⚠️ Couldn't verify your count was accepted. Please try again on `{next_expected}`.",
                mention_author=True
            )
            try:
                await msg.delete(delay=7)
            except discord.Forbidden:
                pass
        except (discord.Forbidden, discord.HTTPException) as e:
            logger.debug(f"Failed to send verification-failed notice for msg {message.id}: {e}")

    async def delete_message(self, message: discord.Message, warning: str):
        logger.info(
            f"🗑️ Deleting invalid message {message.id} from {message.author.display_name}({message.author.id}): {warning}")

        try:
            await message.delete()
            logger.debug("✅ Message deleted successfully")
        except discord.Forbidden:
            logger.warning("❌ Failed to delete user message: forbidden")

        try:
            warn_msg = await message.channel.send(warning)
            logger.debug(f"⚠️ Warning message sent: {warn_msg.id}")
            try:
                await warn_msg.delete(delay=5)
            except discord.Forbidden:
                logger.warning("❌ Failed to schedule warning deletion: forbidden")
        except Exception as e:
            logger.error(f"❌ Failed to send warning message: {e}")

        # Also restore context: show the expected/current number so people know where we are
        try:
            state = await self.get_cached_state(message.channel.id)
            expected = int(state.get("last_number", -1)) + 1
            hint = await message.channel.send(f"ℹ️ Current number is `{expected}`. Keep it going!")
            logger.info(f"💡 Posted current-number hint {expected} in ch {message.channel.id}")
            # Keep the hint around a bit longer so folks can see it
            try:
                await hint.delete(delay=20)
            except discord.Forbidden:
                logger.warning("❌ Failed to schedule hint deletion: forbidden")
        except Exception as e:
            logger.error(f"❌ Failed to post current-number hint in ch {message.channel.id}: {e}", exc_info=True)

    @log_performance("update_leaderboard")
    async def update_leaderboard(self, user_id: int):
        """Update leaderboard with new one-document-per-user structure"""
        logger.debug(f"📊 Updating leaderboard for user {user_id}")
        try:
            # Use atomic increment for the new structure
            result = await self.LB.update_one(
                {"user_id": user_id},
                {"$inc": {"count": 1}},
                upsert=True
            )

            # Invalidate cached leaderboard
            self._leaderboard_cache = None

            if result.upserted_id:
                logger.debug(f"✅ Created new leaderboard entry for user {user_id}")
            else:
                logger.debug(f"✅ Incremented leaderboard for user {user_id}")

        except Exception as e:
            logger.error(f"❌ Failed to update leaderboard for user {user_id}: {e}", exc_info=True)

    async def get_cached_leaderboard(self) -> dict:
        """Get leaderboard with local caching - adapted for new structure"""
        now = asyncio.get_event_loop().time()
        if (self._leaderboard_cache is None or
                now - self._last_lb_update > self._lb_cache_ttl):
            try:
                # Query all user documents and convert to old format for compatibility
                cursor = self.LB.find({})  # Get all documents
                leaderboard_data = {}

                async for doc in cursor:
                    if "user_id" in doc and "count" in doc:
                        leaderboard_data[str(doc["user_id"])] = doc["count"]

                self._leaderboard_cache = leaderboard_data
                self._last_lb_update = now
                logger.debug(f"🔄 Leaderboard cache refreshed with {len(leaderboard_data)} users")
            except Exception as e:
                logger.error(f"❌ Failed to refresh leaderboard cache: {e}")
        return self._leaderboard_cache or {}

    async def get_user_count(self, user_id: int) -> int:
        """Get a specific user's count directly from database"""
        try:
            doc = await self.LB.find_one({"user_id": user_id})
            return doc.get("count", 0) if doc else 0
        except Exception as e:
            logger.error(f"❌ Failed to get count for user {user_id}: {e}")
            return 0

    async def get_top_users(self, limit: int = 10) -> list[tuple[int, int]]:
        """Get top users efficiently using the count index"""
        try:
            cursor = self.LB.find({}).sort("count", -1).limit(limit)
            top_users = []

            async for doc in cursor:
                top_users.append((doc["user_id"], doc["count"]))

            return top_users
        except Exception as e:
            logger.error(f"❌ Failed to get top users: {e}")
            return []

    async def handle_out_of_order(self, message: discord.Message, state: dict, channel_id: int, reason="Out of order!",
                                  now_ts: float | None = None):
        logger.info(
            f"🚫 Handling out-of-order: msg_id={message.id}, user={message.author.display_name}({message.author.id}), ch={channel_id}, reason='{reason}'")

        await self.delete_message(message, f"❌ {message.author.mention} {reason}")

        # Apply out-of-order role
        role = message.guild.get_role(self.out_of_order_role_id)
        if role:
            try:
                await message.author.add_roles(role, reason=reason)
                logger.info(f"🏷️ Applied out-of-order role to {message.author.display_name}({message.author.id})")
            except discord.Forbidden:
                logger.warning(f"❌ Failed to apply out-of-order role to {message.author.id}: forbidden")

        # Set streak protection
        now_ts = now_ts if now_ts is not None else message.created_at.timestamp()
        grace_until = float(now_ts + self.streak_protect_window)
        await self.save_cached_state(channel_id, {
            "out_of_order_user": message.author.id,
            "grace_until": grace_until,
        })
        logger.info(
            f"🛡️ Streak protection activated for {message.author.display_name}({message.author.id}) until {grace_until:.2f}")

    async def remove_out_of_order_role(self, user: discord.Member):
        logger.debug(f"🏷️ Checking out-of-order role for {user.display_name}({user.id})")
        role = user.guild.get_role(self.out_of_order_role_id)
        if role and role in user.roles:
            try:
                await user.remove_roles(role, reason="Recovered from mistake")
                logger.info(f"✅ Removed out-of-order role from {user.display_name}({user.id})")
            except discord.Forbidden:
                logger.warning(f"❌ Failed to remove out-of-order role from {user.id}: forbidden")

    async def assign_milestone_role(self, user: discord.Member, guild: discord.Guild):
        logger.info(f"🎉 Assigning milestone role to {user.display_name}({user.id})")
        role = guild.get_role(self.milestone_role_id)
        if role:
            try:
                await user.add_roles(role, reason="Milestone reached")
                logger.info(f"✅ Milestone role assigned to {user.display_name}({user.id})")
            except discord.Forbidden:
                logger.warning(f"❌ Failed to assign milestone role to {user.id}: forbidden")
        else:
            logger.warning(f"❌ Milestone role {self.milestone_role_id} not found in guild")

    @log_performance("update_master_counter_role")
    async def update_master_counter_role(self, guild: discord.Guild):
        """Optimized master counter role update with caching"""
        logger.debug("👑 Updating master counter role...")
        try:
            lb = await self.get_cached_leaderboard()
            if not lb:
                logger.debug("❌ No leaderboard data available")
                return

            sorted_leaderboard = sorted(lb.items(), key=lambda x: x[1], reverse=True)
            if not sorted_leaderboard:
                logger.debug("❌ Empty sorted leaderboard")
                return

            top_user_id = int(sorted_leaderboard[0][0])
            top_member = guild.get_member(top_user_id)
            if not top_member:
                logger.warning(f"❌ Top user {top_user_id} not found in guild")
                return

            role = guild.get_role(self.master_counter_id)
            if not role:
                logger.warning(f"❌ Master counter role {self.master_counter_id} not found")
                return

            # Optimized role assignment - only modify if needed
            current_members = set(member.id for member in role.members)
            should_have_role = {top_user_id}

            # Remove role from members who shouldn't have it
            to_remove = current_members - should_have_role
            for member_id in to_remove:
                member = guild.get_member(member_id)
                if member:
                    try:
                        await member.remove_roles(role, reason="No longer top counter")
                        logger.debug(f"👑 Removed master role from {member.display_name}({member.id})")
                    except discord.Forbidden:
                        logger.warning(f"❌ Failed to remove master role from {member.id}: forbidden")

            # Add role to top member if needed
            if top_user_id not in current_members:
                try:
                    await top_member.add_roles(role, reason="Became top counter")
                    logger.info(f"👑 Master counter role assigned to {top_member.display_name}({top_user_id})")
                except discord.Forbidden:
                    logger.warning(f"❌ Failed to assign master counter role to {top_user_id}: forbidden")
            else:
                logger.debug(f"👑 {top_member.display_name}({top_user_id}) already has master counter role")

        except Exception as e:
            logger.error(f"❌ Error updating master counter role: {e}", exc_info=True)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.channel.id not in self.count_channel_ids:
            return
        logger.debug(
            f"📨 on_message ch={message.channel.id} msg_id={message.id} author={message.author.display_name}({message.author.id})")
        await self.check_number(message)

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        # Edit handling: edits are not allowed to modify counts
        if after.author.bot or after.channel.id not in self.count_channel_ids:
            return
        if before.content != after.content:
            logger.warning(
                f"✏️ Edit detected in counting channel - deleting msg {after.id} by {after.author.display_name}({after.author.id})")
            logger.debug(f"Edit: '{before.content}' -> '{after.content}'")

            try:
                await after.delete()
                logger.info("✅ Edited message deleted successfully")
            except discord.Forbidden:
                logger.warning("❌ Failed to delete edited message: forbidden")

            try:
                warn = await after.channel.send(
                    f"❌ {after.author.mention} Edits are not allowed in counting. Please send a new message.")
                try:
                    await warn.delete(delay=5)
                except discord.Forbidden:
                    logger.warning("❌ Failed to schedule warning deletion: forbidden")
            except Exception as e:
                logger.error(f"❌ Failed to send edit warning: {e}")

            # Restore context after an edit-delete as well
            try:
                state = await self.get_cached_state(after.channel.id)
                expected = int(state.get("last_number", -1)) + 1
                await after.channel.send(f"ℹ️ Current number is `{expected}`. Keep it going!")
                logger.info(f"💡 Posted current-number hint {expected} (edit case) in ch {after.channel.id}")
            except Exception as e:
                logger.error(f"❌ Failed to post current-number hint after edit in ch {after.channel.id}: {e}",
                             exc_info=True)


async def setup(bot):
    logger.info("🚀 Setting up CountingGame cog...")
    try:
        await bot.add_cog(CountingGame())
        logger.info("✅ CountingGame cog setup completed successfully")
    except Exception as e:
        logger.critical(f"💥 CRITICAL: Failed to setup CountingGame cog: {e}", exc_info=True)
        raise