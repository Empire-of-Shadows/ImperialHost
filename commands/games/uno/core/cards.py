# Python
import random
from typing import List, Optional, Iterable

from utilities.logger_setup import get_logger

logger = get_logger("CardDeck")


class CardDeck:
    COLORS: List[str] = ['🔴', '🔵', '🟢', '🟡']  # Red, Blue, Green, Yellow
    NUMBERS: List[int] = list(range(0, 10))      # 0..9
    ACTIONS: List[str] = ['Skip', 'Reverse', '+2']
    WILDS: List[str] = ['Wild', '+4']

    # Expected UNO counts (classic rules)
    # - For each color: one 0, two each of 1..9, two each of Skip/Reverse/+2 -> 1 + 9*2 + 3*2 = 1 + 18 + 6 = 25 per color
    # - 4 colors -> 25 * 4 = 100
    # - Wilds: 4 Wild, 4 +4 -> 8
    # Total = 108
    EXPECTED_TOTAL_CARDS = 108

    def __init__(self, seed: Optional[int] = None, discard_pile: Optional[List[str]] = None):
        """
        Initialize the deck.

        Args:
            seed: Optional seed for deterministic shuffling (useful for tests).
            discard_pile: Optional reference to a discard pile (list of card strings).
                          If provided and deck empties, the deck can be refilled from it via refill_from_discard().
        """
        logger.info("Initializing CardDeck...")
        self._rng = random.Random(seed)
        self._seed_used = seed
        self._discard_pile = discard_pile  # reference; not owned
        self.deck: List[str] = self.generate_deck()
        self.shuffle()
        logger.info("Deck initialized with %d cards (seed=%r).", len(self.deck), seed)

    def generate_deck(self) -> List[str]:
        """
        Generate a complete UNO deck following the classic rules.

        Returns:
            list[str]: The complete, unshuffled deck.
        """
        logger.info("Generating the UNO deck...")
        deck: List[str] = []

        # Number cards
        for color in self.COLORS:
            # One 0
            deck.append(f"{color} 0")
            # Two of each 1..9
            for number in range(1, 10):
                deck.extend([f"{color} {number}", f"{color} {number}"])

        # Action cards: two per color for each action
        for color in self.COLORS:
            for action in self.ACTIONS:
                deck.extend([f"{color} {action}", f"{color} {action}"])

        # Wilds
        deck.extend(["Wild"] * 4)
        deck.extend(["+4"] * 4)

        total = len(deck)
        if total != self.EXPECTED_TOTAL_CARDS:
            logger.error("Generated deck has unexpected size: %d (expected %d).", total, self.EXPECTED_TOTAL_CARDS)
        else:
            logger.info("Generated deck with %d cards.", total)

        return deck

    def shuffle(self) -> None:
        """
        Shuffle the deck in place using the deck's RNG.
        """
        logger.info("Shuffling the deck...")
        self._rng.shuffle(self.deck)
        logger.debug("Deck shuffled. Top card is now: %r", self.peek())

    def draw(self) -> Optional[str]:
        """
        Draw a single card from the deck. Optionally attempts refill from discard pile if configured.

        Returns:
            str | None: The drawn card, or None if the deck is empty (and cannot be refilled).
        """
        if not self.deck:
            logger.warning("Deck is empty. Attempting to refill from discard pile (if available).")
            self.refill_from_discard()

        if self.deck:
            card = self.deck.pop()
            logger.debug("Drew card: %r. %d cards remaining.", card, len(self.deck))
            return card

        logger.warning("The deck is empty and could not be refilled. No cards to draw.")
        return None

    def draw_many(self, n: int) -> List[Optional[str]]:
        """
        Draw multiple cards, attempting to refill when needed.

        Args:
            n: Number of cards to draw.

        Returns:
            List of drawn cards; entries may be None if deck remained empty.
        """
        if n <= 0:
            return []
        logger.debug("Drawing %d card(s). Remaining before draw: %d", n, len(self.deck))
        out: List[Optional[str]] = []
        for _ in range(n):
            out.append(self.draw())
        logger.debug("Finished drawing %d card(s). Remaining after draw: %d", n, len(self.deck))
        return out

    def refill_from_discard(self) -> None:
        """
        If a discard pile reference was provided, refill the deck from it (leaving the top-most card).
        The refill shuffles the collected cards using the deck RNG.
        """
        if not self._discard_pile:
            logger.debug("No discard pile provided; cannot refill.")
            return

        if len(self._discard_pile) <= 1:
            # Nothing to take (keep the top card on the table)
            logger.debug("Discard pile has <= 1 card; nothing to refill.")
            return

        # Keep the top card, take the rest to form the new deck
        keep_top = self._discard_pile[-1]
        refill_cards = self._discard_pile[:-1]
        self._discard_pile[:] = [keep_top]  # mutate in place, preserving reference

        if not refill_cards:
            logger.debug("No cards available from discard to refill.")
            return

        logger.info("Refilling deck from discard pile with %d card(s).", len(refill_cards))
        self.deck.extend(refill_cards)
        self.shuffle()

    def remaining(self) -> int:
        """
        Returns:
            Number of cards remaining in the deck.
        """
        return len(self.deck)

    def peek(self) -> Optional[str]:
        """
        Returns:
            The top card without removing it, or None if deck is empty.
        """
        return self.deck[-1] if self.deck else None

    def reset(self, reshuffle: bool = True) -> None:
        """
        Reset the deck to a freshly generated state. Optionally reshuffle.

        Args:
            reshuffle: Whether to shuffle after regenerating.
        """
        logger.info("Resetting the deck (reshuffle=%s).", reshuffle)
        self.deck = self.generate_deck()
        if reshuffle:
            self.shuffle()

    def to_list(self) -> List[str]:
        """
        Snapshot the current deck as a list (top is last element).
        """
        return list(self.deck)

    @classmethod
    def from_cards(cls, cards: Iterable[str], seed: Optional[int] = None) -> "CardDeck":
        """
        Create a CardDeck from a provided iterable of card strings (no validation of UNO rules).
        Useful for testing or custom game modes.
        """
        instance = cls(seed=seed)
        instance.deck = list(cards)
        logger.info("Custom deck loaded with %d card(s). Seed=%r", len(instance.deck), seed)
        return instance