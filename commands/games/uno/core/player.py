# Python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional

from utilities.logger_setup import get_logger

logger = get_logger("Player")


def _get_user_id(user: Any) -> Optional[int]:
    return getattr(user, "id", None)


def _get_user_name(user: Any) -> str:
    # Prefer display_name/nick/name if available (Discord Member-like), else str(user)
    for attr in ("display_name", "nick", "name"):
        val = getattr(user, attr, None)
        if val:
            return str(val)
    return str(user)


def _get_user_mention(user: Any) -> str:
    mention = getattr(user, "mention", None)
    return str(mention) if mention else f"@{_get_user_name(user)}"


@dataclass(eq=False, slots=True)
class Player:
    user: Any
    hand: List[str] = field(default_factory=list)
    skips: int = 0
    declared_uno: bool = False

    def __post_init__(self):
        uid = _get_user_id(self.user)
        uname = _get_user_name(self.user)
        if uid is None:
            logger.warning("Player initialized without a valid user id. user=%r", self.user)
        logger.info("Player initialized: %s (ID: %s)", uname, uid)

    # Identity: players are equal if they wrap the same user id
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Player):
            logger.debug("Cannot compare Player with non-Player object: %s", type(other))
            return False
        uid_self = _get_user_id(self.user)
        uid_other = _get_user_id(other.user)
        if uid_self is None or uid_other is None:
            logger.warning("Cannot compare Players with missing user id: self=%r, other=%r", self.user, other.user)
            return False
        is_equal = uid_self == uid_other
        logger.debug("Checking equality between players %s and %s: %s", uid_self, uid_other, is_equal)
        return is_equal

    def __hash__(self) -> int:
        uid = _get_user_id(self.user)
        return hash(("Player", uid))

    def __repr__(self) -> str:
        uid = _get_user_id(self.user)
        uname = _get_user_name(self.user)
        return f"Player(user={uname!r}, id={uid}, hand={len(self.hand)} cards, skips={self.skips}, declared_uno={self.declared_uno})"

    # Convenience properties
    @property
    def user_id(self) -> Optional[int]:
        return _get_user_id(self.user)

    @property
    def mention(self) -> str:
        return _get_user_mention(self.user)

    # Collection conveniences
    def __len__(self) -> int:
        return len(self.hand)

    def __contains__(self, card: str) -> bool:
        return card in self.hand

    # Hand operations
    def add_card(self, card: str) -> None:
        self.hand.append(card)
        logger.debug("Added card %r to %s's hand. Count now: %d", card, _get_user_name(self.user), len(self.hand))

    def add_cards(self, cards: Iterable[str]) -> int:
        count_before = len(self.hand)
        self.hand.extend(cards)
        added = len(self.hand) - count_before
        logger.debug("Added %d card(s) to %s's hand. Count now: %d", added, _get_user_name(self.user), len(self.hand))
        return added

    def remove_card(self, card: str) -> bool:
        try:
            self.hand.remove(card)
            logger.debug("Removed card %r from %s's hand. Count now: %d", card, _get_user_name(self.user), len(self.hand))
            return True
        except ValueError:
            logger.warning("Attempted to remove missing card %r from %s's hand.", card, _get_user_name(self.user))
            return False

    def remove_cards(self, cards: Iterable[str]) -> int:
        removed = 0
        for c in cards:
            if self.remove_card(c):
                removed += 1
        logger.debug("Removed %d requested card(s) from %s's hand.", removed, _get_user_name(self.user))
        return removed

    # Turn/penalty helpers
    def skip_turn(self) -> int:
        self.skips += 1
        logger.info("Player %s skipped their turn. Total skips: %d", _get_user_name(self.user), self.skips)
        return self.skips

    def reset_skips(self) -> None:
        logger.debug("Resetting skips for %s (was %d).", _get_user_name(self.user), self.skips)
        self.skips = 0

    # Visibility helpers
    def display_hand(self, as_string: bool = False):
        """
        Returns a safe representation of the hand (copy by default).
        Logs only at DEBUG to avoid leaking sensitive state at INFO.
        """
        if as_string:
            hand_display = ", ".join(self.hand) if self.hand else "No cards"
            logger.debug("%s's hand: %s", _get_user_name(self.user), hand_display)
            return hand_display
        logger.debug("%s requested hand view (count=%d).", _get_user_name(self.user), len(self.hand))
        return list(self.hand)  # defensive copy

    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "user_name": _get_user_name(self.user),
            "hand_count": len(self.hand),
            "skips": self.skips,
            "declared_uno": self.declared_uno,
        }