from dataclasses import dataclass
from typing import List, Dict, Type, Any

from utilities.logger_setup import get_logger

logger = get_logger("SettingsDefine")

@dataclass
class ConfigDefinition:
    name: str
    type: Type
    default: Any
    description: str = ""
    validator: callable = None

class SettingsDefine:

    def _define_settings(self):
        """Define all config settings with clean defaults"""
        logger.debug("Defining configuration settings")

        # Counting System Settings
        self._config_definitions["count_channel_ids"] = ConfigDefinition(
            name="count_channel_ids",
            type=List[int],
            default=[1375640636606251008],
            description="List of counting channel IDs",
            validator=lambda x: isinstance(x, list) and all(
                isinstance(i, int) and i > 0 for i in x
            )
        )

        self._config_definitions["out_of_order_role_id"] = ConfigDefinition(
            name="out_of_order_role_id",
            type=int,
            default=1375629700809887948,
            description="Role ID for out of order role",
            validator=lambda x: isinstance(x, int) and x > 0
        )

        self._config_definitions["milestone_role_id"] = ConfigDefinition(
            name="milestone_role_id",
            type=int,
            default=1365248431492300800,
            description="Role ID for milestone role",
            validator=lambda x: isinstance(x, int) and x > 0
        )

        self._config_definitions["master_counter_id"] = ConfigDefinition(
            name="master_counter_id",
            type=int,
            default=1378444505232969798,
            description="Role ID for master counter role",
            validator=lambda x: isinstance(x, int) and x > 0
        )

        self._config_definitions["double_post_grace_seconds"] = ConfigDefinition(
            name="double_post_grace_seconds",
            type=int,
            default=15,
            description="Grace period for double posts in seconds",
            validator=lambda x: isinstance(x, int) and 1 <= x <= 300
        )

        self._config_definitions["idle_grace_seconds"] = ConfigDefinition(
            name="idle_grace_seconds",
            type=int,
            default=30,
            description="Grace period for idle time in seconds",
            validator=lambda x: isinstance(x, int) and 1 <= x <= 600
        )

        self._config_definitions["streak_protect_window"] = ConfigDefinition(
            name="streak_protect_window",
            type=int,
            default=3,
            description="Number of counts to protect streaks",
            validator=lambda x: isinstance(x, int) and 1 <= x <= 10
        )

        self._config_definitions["max_digits"] = ConfigDefinition(
            name="max_digits",
            type=int,
            default=12,
            description="Maximum number of digits allowed in counts",
            validator=lambda x: isinstance(x, int) and 1 <= x <= 50
        )

        # Auto-verify interval setting
        self._config_definitions["auto_verify_interval"] = ConfigDefinition(
            name="auto_verify_interval",
            type=int,
            default=3600,
            description="Auto-verify interval in seconds",
            validator=lambda x: isinstance(x, int) and x > 0
        )