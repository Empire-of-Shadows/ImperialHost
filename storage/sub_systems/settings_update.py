import json
import os
from typing import Set, Dict

import discord

from utilities.logger_setup import get_logger

logger = get_logger("SettingsUpdate")

class SettingsUpdate:
    def save_config(self):
        """Save current configuration to file"""
        logger.info(f"Saving configuration to: {self.config_path}")
        try:
            # Only save to file paths, not directories
            if os.path.isdir(self.config_path):
                logger.warning(f"Cannot save to directory path: {self.config_path}. Skipping save operation.")
                return

            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(self._values, f, indent=2, ensure_ascii=False)
            logger.debug(f"Configuration saved successfully with {len(self._values)} values")
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}", exc_info=True)
            raise

    # ===========================================
    # Counting System Update Methods
    # ===========================================

    def update_count_channel_ids(self, channel_ids: Set[int]):
        """Update counting channel IDs"""
        logger.info(f"Updating count_channel_ids to: {channel_ids}")
        try:
            self._values["count_channel_ids"] = list(channel_ids)
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated count_channel_ids to {len(channel_ids)} channels")
        except Exception as e:
            logger.error(f"Failed to update count_channel_ids: {e}", exc_info=True)
            raise

    def add_count_channel(self, channel_id: int):
        """Add a counting channel ID"""
        current_channels = self.count_channel_ids
        current_channels.add(channel_id)
        self.update_count_channel_ids(current_channels)

    def remove_count_channel(self, channel_id: int):
        """Remove a counting channel ID"""
        current_channels = self.count_channel_ids
        current_channels.discard(channel_id)
        self.update_count_channel_ids(current_channels)

    def update_counting_role(self, role_type: str, role_id: int):
        """Update a counting role ID"""
        valid_roles = ["out_of_order", "milestone", "master_counter"]
        if role_type not in valid_roles:
            raise ValueError(f"Invalid role type. Must be one of: {valid_roles}")

        config_key = f"{role_type}_role_id"
        logger.info(f"Updating {config_key} to: {role_id}")

        try:
            self._values[config_key] = role_id
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated {config_key} to: {role_id}")
        except Exception as e:
            logger.error(f"Failed to update {config_key}: {e}", exc_info=True)
            raise

    def update_counting_rule(self, rule_name: str, value: int):
        """Update a counting rule value"""
        valid_rules = [
            "double_post_grace_seconds",
            "idle_grace_seconds",
            "streak_protect_window",
            "max_digits"
        ]

        if rule_name not in valid_rules:
            raise ValueError(f"Invalid rule name. Must be one of: {valid_rules}")

        logger.info(f"Updating {rule_name} to: {value}")

        try:
            self._values[rule_name] = value
            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated {rule_name} to: {value}")
        except Exception as e:
            logger.error(f"Failed to update {rule_name}: {e}", exc_info=True)
            raise

    async def update_channel_id(self, channel_type: str, channel_id: int, channel_name: str = None, bot=None):
        """
        Update a channel ID and optionally store its name.

        Args:
            channel_type: "suggestion", "admin", "counting", or game types ("game_lobby", "allowed_channel", "win_feed")
            channel_id: The Discord channel ID
            channel_name: Optional channel name for reference
            bot: Optional bot instance to fetch channel info
        """
        logger.info(f"Updating {channel_type}_channel_id to: {channel_id}")

        try:
            # Handle special channel types
            if channel_type == "counting":
                self.add_count_channel(channel_id)
                return

            # Handle game-related channel types
            game_channels = ["game_lobby", "allowed_channel", "win_feed"]
            if channel_type in game_channels:
                await self.update_game_setting(channel_type, channel_id, bot, channel_name)
                return

            # Handle category separately
            if channel_type == "game_category":
                await self.update_game_setting("game_category", channel_id, bot, channel_name)
                return

            # Original logic for suggestion/admin channels
            config_key = f"{channel_type}_channel_id"

            if config_key not in self._config_definitions:
                raise ValueError(f"Unknown channel type: {channel_type}")

            # Validate channel_id
            if not isinstance(channel_id, int) or channel_id <= 0:
                raise ValueError(f"Invalid channel ID: {channel_id}")

            # If bot is provided and channel_name is not given, try to fetch channel name
            if bot and not channel_name:
                channel = bot.get_channel(channel_id)
                if channel:
                    channel_name = channel.name
                    logger.debug(f"Fetched channel name: {channel_name}")
                else:
                    logger.warning(f"Could not fetch channel name for ID: {channel_id}")

            # Update the channel ID
            self._values[config_key] = channel_id

            # Update channel names mapping if name is provided
            if channel_name:
                current_names = self._values.get("channel_names", {})
                current_names[str(channel_id)] = channel_name
                self._values["channel_names"] = current_names
                logger.debug(f"Updated channel name mapping: {channel_id} -> {channel_name}")

            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated {channel_type}_channel_id to: {channel_id}")

        except Exception as e:
            logger.error(f"Failed to update {channel_type}_channel_id: {e}", exc_info=True)
            raise

    # ===========================================
    # Game Settings Update Methods
    # ===========================================

    async def update_game_setting(self, setting_name: str, value: int, bot=None, channel_name: str = None):
        """
        Update a game setting (channel ID or category ID).

        Args:
            setting_name: One of "game_category", "game_lobby", "allowed_channel", "win_feed"
            value: The new channel/category ID
            bot: Optional bot instance to fetch channel info
            channel_name: Optional channel name for reference
        """
        valid_settings = ["game_category", "game_lobby", "allowed_channel", "win_feed"]

        if setting_name not in valid_settings:
            raise ValueError(f"Invalid setting name. Must be one of: {valid_settings}")

        config_key = f"{setting_name}_id"
        logger.info(f"Updating {config_key} to: {value}")

        try:
            # Validate value
            if not isinstance(value, int) or value <= 0:
                raise ValueError(f"Invalid ID: {value}")

            # If bot is provided and channel_name is not given, try to fetch channel info
            if bot and not channel_name:
                if setting_name == "game_category":
                    # For categories, we need to get from guild
                    for guild in bot.guilds:
                        category = guild.get_channel(value)
                        if category and isinstance(category, discord.CategoryChannel):
                            channel_name = category.name
                            break
                else:
                    # For regular channels
                    channel = bot.get_channel(value)
                    if channel:
                        channel_name = channel.name

            # Update the setting
            self._values[config_key] = value

            # Update channel names mapping if name is provided
            if channel_name:
                current_names = self._values.get("channel_names", {})
                current_names[str(value)] = channel_name
                self._values["channel_names"] = current_names
                logger.debug(f"Updated channel name mapping: {value} -> {channel_name}")

            self.save_config()
            self._notify_callbacks()
            logger.info(f"Successfully updated {config_key} to: {value}")

        except Exception as e:
            logger.error(f"Failed to update {config_key}: {e}", exc_info=True)
            raise

    # Convenience methods for common updates
    async def update_game_category(self, category_id: int, bot=None, category_name: str = None):
        """Update game category ID"""
        await self.update_game_setting("game_category", category_id, bot, category_name)

    async def update_game_lobby(self, channel_id: int, bot=None, channel_name: str = None):
        """Update game lobby channel ID"""
        await self.update_game_setting("game_lobby", channel_id, bot, channel_name)

    async def update_allowed_channel(self, channel_id: int, bot=None, channel_name: str = None):
        """Update allowed channel ID"""
        await self.update_game_setting("allowed_channel", channel_id, bot, channel_name)

    async def update_win_feed(self, channel_id: int, bot=None, channel_name: str = None):
        """Update win feed channel ID"""
        await self.update_game_setting("win_feed", channel_id, bot, channel_name)

    def _notify_callbacks(self):
        """Notify all callbacks of config changes"""
        logger.debug(f"Notifying {len(self._callbacks)} callbacks of config changes")
        for callback in self._callbacks:
            try:
                callback(self._values)
                logger.debug(f"Successfully executed callback: {callback.__name__}")
            except Exception as e:
                logger.error(f"Error in config callback {callback.__name__}: {e}", exc_info=True)