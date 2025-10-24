import json
import os
from typing import Any, Dict, List, Set
from dotenv import load_dotenv
import yaml

from storage.sub_systems.settings_define import SettingsDefine, ConfigDefinition
from storage.sub_systems.settings_update import SettingsUpdate
from storage.sub_systems.settings_validate import SettingsValidater
from utilities.logger_setup import get_logger

load_dotenv()

config_dir = os.getenv("CONFIG_PATH", "/data/config")

# Initialize logger for this module
logger = get_logger("config_system")

S = " " * 50

def format_value_for_logging(value: Any, max_single_line: int = 80) -> str:
    """Format values nicely for logging output with smart formatting."""
    if isinstance(value, (dict, list)):
        # Convert to pretty JSON
        pretty_json = json.dumps(value, indent=2, ensure_ascii=False)

        # Check if it's small enough for a single line
        compact_str = json.dumps(value, ensure_ascii=False)
        if len(compact_str) <= max_single_line:
            return f" {compact_str}"

        # For multi-line, add proper indentation and {s} at the start of each line
        indented = pretty_json.replace('\n', f'\n{S}')
        return f" \n{S}{S}{indented}"

    return f" {value}"

class BotConfig(SettingsValidater, SettingsDefine, SettingsUpdate):
    def __init__(self, config_path: str = config_dir):
        logger.info(f"Initializing BotConfig with path: {config_path}")
        self.config_path = config_path
        self._config_definitions: Dict[str, ConfigDefinition] = {}
        self._values: Dict[str, Any] = {}
        self._callbacks: List[callable] = []

        try:
            os.makedirs(config_path, exist_ok=True)
            logger.debug(f"Ensured config directory exists: {os.path.dirname(config_path)}")
        except Exception as e:
            logger.error(f"Failed to create config directory: {e}", exc_info=True)
            raise

        self._define_settings()
        self.load_config()
        logger.info("BotConfig initialization completed successfully")

    def _load_file(self, file_path: str) -> Dict[str, Any]:
        """Load a single configuration file (JSON or YAML/YML)"""
        logger.debug(f"Loading configuration file: {file_path}")

        try:
            if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f)
                    logger.debug(
                        f"YAML - Successfully loaded '{file_path}' with {len(config_data) if config_data else 0} keys")
                    return config_data if config_data else {}

            elif file_path.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    logger.debug(f"JSON - Successfully loaded '{file_path}' with {len(config_data)} keys")
                    return config_data

            else:
                logger.warning(f"Unsupported file format: {file_path}")
                return {}

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON file '{file_path}': {e}", exc_info=True)
            raise
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML file '{file_path}': {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error loading file '{file_path}': {e}", exc_info=True)
            raise

    def _merge_configs(self, base_config: Dict[str, Any], new_config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two configuration dictionaries deeply"""
        merged = base_config.copy()

        for key, value in new_config.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                # Deep merge for nested dictionaries
                merged[key] = self._merge_configs(merged[key], value)
            else:
                # Override or add new key
                merged[key] = value

        return merged

    def load_config(self):
        """Load configuration from file(s) - supports single file or directory with multiple files"""
        logger.info(f"Loading configuration from: {self.config_path}")

        if not os.path.exists(self.config_path):
            logger.warning(f"Configuration path not found, creating default: {self.config_path}")
            self._create_default_config()
            return

        try:
            file_config = {}

            # Load from a directory - walk through and merge all config files
            if os.path.isdir(self.config_path):
                logger.debug(f"Loading configuration from directory: {self.config_path}")
                config_files = []

                # Collect all valid config files
                for root, _, files in os.walk(self.config_path):
                    for file in files:
                        if file.endswith(('.yaml', '.yml', '.json')):
                            config_files.append(os.path.join(root, file))

                if not config_files:
                    logger.warning(f"No configuration files found in directory: {self.config_path}")
                    self._create_default_config()
                    return

                logger.info(f"Found {len(config_files)} configuration file(s) in directory")

                # Load and merge all config files
                for config_file in sorted(config_files):  # Sort for consistent load order
                    try:
                        loaded_config = self._load_file(config_file)
                        file_config = self._merge_configs(file_config, loaded_config)
                        logger.debug(f"Merged configuration from: {os.path.basename(config_file)}")
                        logger.debug(f"Successfully merged {len(loaded_config)} configuration key(s)")
                        for key, value in file_config.items():
                            formatted_value = format_value_for_logging(value)
                            logger.debug(f"Loaded key '{key}' with value: {formatted_value}")
                    except Exception as e:
                        logger.error(f"Failed to load config file '{config_file}': {e}", exc_info=True)
                        raise

                logger.info(f"Successfully merged {len(config_files)} configuration file(s)")

            # Load from a single YAML file
            elif self.config_path.endswith(('.yaml', '.yml')):
                logger.debug(f"Loading configuration from single YAML file: {self.config_path}")
                file_config = self._load_file(self.config_path)
                for key, value in file_config.items():
                    formatted_value = format_value_for_logging(value)
                    logger.debug(f"\n"
                                 f"YAML - Loaded key\n"
                                 f" '{key}':"
                                 f"{formatted_value}")
                logger.debug(f"YAML - Successfully loaded '{self.config_path}' with {len(file_config)} keys")

            # Load from a single JSON file
            elif self.config_path.endswith('.json'):
                logger.debug(f"Loading configuration from single JSON file: {self.config_path}")
                file_config = self._load_file(self.config_path)
                # Log the key values
                for key, value in file_config.items():
                    formatted_value = format_value_for_logging(value)
                    logger.debug(f"JSON - Loaded key '{key}' with value: {formatted_value}")
                logger.debug(f"JSON - Successfully loaded '{self.config_path}' with {len(file_config)} keys")

            # Unsupported file extension
            else:
                logger.warning(
                    f"Unknown file extension for configuration file: {self.config_path}. "
                    f"Supported extensions: .yaml, .yml, .json"
                )
                self._create_default_config()
                return

            # Validate and load the merged configuration
            self._validate_and_load(file_config)
            logger.info(f"Configuration loaded and validated successfully with {len(file_config)} top-level keys")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON configuration: {e}", exc_info=True)
            raise
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML configuration: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Error loading configuration: {e}", exc_info=True)
            raise

    def _create_default_config(self):
        """Create default config file"""
        logger.info("Creating default configuration")
        try:
            self._values = {key: definition.default for key, definition in self._config_definitions.items()}
            self.save_config()
            logger.info(f"Default configuration created successfully at: {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to create default configuration: {e}", exc_info=True)
            raise

    # Property accessors for easy usage
    @property
    def count_channel_ids(self) -> Set[int]:
        """Get counting channel IDs as a set"""
        channel_ids = self._values.get("count_channel_ids", [])
        return set(channel_ids)

    @property
    def auto_verify_interval(self) -> int:
        """Get auto-verify interval in seconds"""
        return self._values.get("auto_verify_interval", 3600)

    @property
    def out_of_order_role_id(self) -> int:
        """Get out of order role ID"""
        return self._values.get("out_of_order_role_id", 1375629700809887948)

    @property
    def milestone_role_id(self) -> int:
        """Get milestone role ID"""
        return self._values.get("milestone_role_id", 1365248431492300800)

    @property
    def master_counter_id(self) -> int:
        """Get master counter role ID"""
        return self._values.get("master_counter_id", 1378444505232969798)

    @property
    def double_post_grace_seconds(self) -> int:
        """Get double post grace period in seconds"""
        return self._values.get("double_post_grace_seconds", 15)

    @property
    def idle_grace_seconds(self) -> int:
        """Get idle grace period in seconds"""
        return self._values.get("idle_grace_seconds", 30)

    @property
    def streak_protect_window(self) -> int:
        """Get streak protection window"""
        return self._values.get("streak_protect_window", 3)

    @property
    def max_digits(self) -> int:
        """Get maximum digits allowed"""
        return self._values.get("max_digits", 12)

    # ===========================================
    # Game Settings Properties
    # ===========================================

    @property
    def game_category_id(self) -> int:
        """Get game category ID"""
        return self._values.get("game_category_id", 1368657747154964564)

    @property
    def game_lobby_channel_id(self) -> int:
        """Get game lobby channel ID"""
        return self._values.get("game_lobby_channel_id", 1406642649997508758)

    @property
    def allowed_channel_id(self) -> int:
        """Get allowed channel ID"""
        return self._values.get("allowed_channel_id", 1406642649997508758)

    @property
    def win_feed_channel_id(self) -> int:
        """Get win feed channel ID"""
        return self._values.get("win_feed_channel_id", 1365731922994528426)

    def add_callback(self, callback: callable):
        """Add callback for config changes"""
        self._callbacks.append(callback)
        logger.debug(f"Added callback: {callback.__name__}. Total callbacks: {len(self._callbacks)}")


# Optional: Add callback to log config changes
def on_config_change(new_config):
    logger.info("Configuration was updated")



config = BotConfig()
config.add_callback(on_config_change)
