# Python
from typing import Any, Optional
from utilities.logger_setup import get_logger

logger = get_logger("UnoRules")


def _get_attr(obj: Any, name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Safely extract an attribute or dict key as a string-ish value.
    """
    if obj is None:
        return default
    if hasattr(obj, name):
        return getattr(obj, name, default)
    if isinstance(obj, dict):
        return obj.get(name, default)
    return default


def can_play_card(card: Any, top_card: Any) -> bool:
    """
    Determines if a card can be legally played based on the top card.

    Args:
        card: The card the player wants to play. Supports an object with attributes
              .type and .color or a dict with keys 'type' and 'color'.
        top_card: The current top card on the discard pile. Same structure as `card`.

    Returns:
        True if the card is playable, otherwise False.
    """
    try:
        c_type = _get_attr(card, "type")
        c_color = _get_attr(card, "color")

        t_type = _get_attr(top_card, "type")
        t_color = _get_attr(top_card, "color")

        # If no top card (e.g., game start), allow play and log
        if top_card is None or (t_type is None and t_color is None):
            logger.info(
                "Play allowed: no top card. card(type=%r, color=%r)", c_type, c_color
            )
            return True

        # Wild cards are always playable
        if c_type in ("wild", "wild_draw_four"):
            logger.debug(
                "Play allowed: wild card. card(type=%r, color=%r) top(type=%r, color=%r)",
                c_type, c_color, t_type, t_color
            )
            return True

        # Match by color
        if c_color is not None and t_color is not None and c_color == t_color:
            logger.debug(
                "Play allowed: color match. card(type=%r, color=%r) top(type=%r, color=%r)",
                c_type, c_color, t_type, t_color
            )
            return True

        # Match by number or symbol (type)
        if c_type is not None and t_type is not None and c_type == t_type:
            logger.debug(
                "Play allowed: type match. card(type=%r, color=%r) top(type=%r, color=%r)",
                c_type, c_color, t_type, t_color
            )
            return True

        logger.debug(
            "Play denied: no match. card(type=%r, color=%r) top(type=%r, color=%r)",
            c_type, c_color, t_type, t_color
        )
        return False

    except Exception as e:
        logger.error(
            "Error evaluating can_play_card for card=%r, top_card=%r", card, top_card, exc_info=e
        )
        # Be conservative on error: deny the play
        return False