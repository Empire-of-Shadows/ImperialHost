from typing import Any, Dict

from utilities.logger_setup import get_logger

logger = get_logger("SettingsValidater")

class SettingsValidater:

    def _validate_and_load(self, config_dict: Dict[str, Any]):
        """Validate and load configuration"""
        logger.debug("Validating configuration data")
        errors = []

        for key, definition in self._config_definitions.items():
            if key not in config_dict:
                if definition.default is not None:
                    self._values[key] = definition.default
                    logger.debug(f"Using default value for missing config key: {key}")
                else:
                    error_msg = f"Missing required config: {key}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                continue

            value = config_dict[key]
            if definition.validator and not definition.validator(value):
                error_msg = f"Invalid value for {key}: {value}"
                errors.append(error_msg)
                logger.error(error_msg)
            else:
                self._values[key] = value
                logger.debug(f"Loaded config value for: {key}")

        if errors:
            error_summary = f"Config validation errors:\n" + "\n".join(errors)
            logger.error(error_summary)
            raise ValueError(error_summary)

        logger.info(f"Successfully validated and loaded {len(self._values)} configuration values")