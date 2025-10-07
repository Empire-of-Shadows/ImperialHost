# Python
import asyncio
import json
import logging
import random
import string
from dataclasses import dataclass
from typing import Iterable, List, Optional

import discord
import requests

from commands.games.hangman_utils.hangman_globals import hangman_games, HANGMAN_STAGES, WORD_LIST
from commands.games.hangman_utils.Hangman_Game_Message import message_editor
from utilities.logger_setup import get_logger

logger = get_logger("HangmanStarter")


# One per channel: prevents double starts and race conditions updating shared dict
_channel_locks: dict[int, asyncio.Lock] = {}


@dataclass
class GameState:
    word: str
    correct: set
    wrong: set
    attempts: int
    participants: List[int]
    message_id: Optional[int]
    custom_word: bool
    current_turn_index: Optional[int]


class HangmanStarter:
    def __init__(self, message_editor):
        self.message_editor = message_editor

    async def start_hangman(
        self,
        interaction: discord.Interaction,
        word: Optional[str] = None,
        participants: Optional[Iterable[int]] = None,
        min_length: int = 3,
        max_length: int = 15,
        target_channel: Optional[discord.TextChannel] = None,
    ) -> None:
        """
        Start a new Hangman game.

        If target_channel is provided, the game state and messages will be placed there.
        Otherwise, it uses the current interaction channel.
        """
        # Decide which channel will host the game
        channel_id = target_channel.id if target_channel else interaction.channel_id
        lock = _channel_locks.setdefault(channel_id, asyncio.Lock())

        # Avoid overlapping starts in same channel
        async with lock:
            # If a game is already running, exit early
            if channel_id in hangman_games and "word" in hangman_games[channel_id]:
                await self._safe_send(interaction, "A game is already running in this channel!", ephemeral=True)
                return

            await self._safe_defer(interaction)

            try:
                normalized_word, is_custom = await self._resolve_word(word, min_length, max_length)
                if not normalized_word:
                    await interaction.followup.send(
                        "Failed to fetch or generate a valid word. Please try again later.",
                        ephemeral=True,
                    )
                    return

                state = self._build_initial_state(
                    normalized_word,
                    is_custom,
                    participants=participants or [],
                )

                # Persist state
                hangman_games[channel_id] = {
                    "word": state.word,
                    "correct": state.correct,
                    "wrong": state.wrong,
                    "attempts": state.attempts,
                    "participants": state.participants,
                    "message_id": state.message_id,
                    "custom_word": state.custom_word,
                    "current_turn_index": state.current_turn_index,
                }

                logger.info(
                    "Hangman started | channel=%s custom=%s len=%s",
                    channel_id,
                    is_custom,
                    len(normalized_word),
                )

                # Build initial embed
                embed = await self.message_editor.build_hangman_embed(channel_id)
                if embed is None:
                    await interaction.followup.send(
                        "Failed to create the game state. Please try again later.",
                        ephemeral=True,
                    )
                    # Clean up partially created state
                    hangman_games.pop(channel_id, None)
                    return

                if target_channel:
                    message = await target_channel.send(embed=embed)
                    # Notify the command invoker where the game was created (ephemeral)
                    await self._safe_followup(interaction, f"Created game channel: {target_channel.mention}", True)
                else:
                    message = await interaction.followup.send(embed=embed)

                hangman_games[channel_id]["message_id"] = message.id
                logger.debug("Hangman message posted | channel=%s message_id=%s", channel_id, message.id)

            except Exception as e:
                logger.exception("Error in start_hangman | channel=%s", channel_id)
                await self._safe_followup(interaction, f"An error occurred while starting the game: {e}", True)

    def _build_initial_state(self, word: str, custom: bool, participants: List[int]) -> GameState:
        # You can tune attempts here; keeping existing logic to avoid behavior changes
        attempts = len(HANGMAN_STAGES) - 1
        return GameState(
            word=word,
            correct=set(),
            wrong=set(),
            attempts=attempts,
            participants=participants,
            message_id=None,
            custom_word=custom,
            current_turn_index=0 if participants else None,
        )

    async def _resolve_word(self, word: Optional[str], min_length: int, max_length: int) -> tuple[Optional[str], bool]:
        if word:
            normalized = self._normalize_word(word)
            if not self._is_valid_word(normalized, min_length, max_length):
                raise ValueError(
                    f"Custom word must be alphabetic and length {min_length}-{max_length}."
                )
            return normalized, True

        # Random fetch with validation and layered fallbacks
        random_word = await self._fetch_random_word(min_length, max_length)
        if random_word and self._is_valid_word(random_word, min_length, max_length):
            return random_word, False

        # As a final fallback, try one last time using filtered WORD_LIST
        candidates = [w for w in WORD_LIST if self._is_valid_word(w, min_length, max_length)]
        return (random.choice(candidates) if candidates else None), False

    def _normalize_word(self, w: str) -> str:
        w = w.strip().lower()
        # Keep only letters; hangman typically excludes spaces/digits/symbols
        return "".join(ch for ch in w if ch in string.ascii_lowercase)

    def _is_valid_word(self, w: Optional[str], min_length: int, max_length: int) -> bool:
        return bool(w) and w.isalpha() and (min_length <= len(w) <= max_length)

    async def _fetch_random_word(self, min_length: int, max_length: int) -> Optional[str]:
        # Try local JSON file in a thread to avoid blocking the loop
        try:
            words = await asyncio.to_thread(self._load_words_from_json)
            valid = [w for w in words if self._is_valid_word(w, min_length, max_length)]
            if valid:
                return random.choice(valid)
        except Exception as e:
            logger.debug("Local JSON word fetch failed: %s", e)

        # Try API (requests) in a thread to avoid blocking
        try:
            # Pick a random target length to avoid always the same size
            target_len = random.randint(min_length, max_length)
            api_word = await asyncio.to_thread(self._fetch_from_api, target_len)
            if api_word and self._is_valid_word(api_word, min_length, max_length):
                return api_word
        except Exception as e:
            logger.debug("API word fetch failed: %s", e)

        # Try WORD_LIST fallback here (final validation occurs in caller)
        filtered = [w for w in WORD_LIST if min_length <= len(w) <= max_length]
        return random.choice(filtered) if filtered else None

    def _load_words_from_json(self) -> List[str]:
        with open("commands/games/hangman_utils/storage/words.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        # support both dict-like {"word": meta} and list-like ["word1", ...]
        if isinstance(data, dict):
            return list(data.keys())
        if isinstance(data, list):
            return [str(w) for w in data]
        return []

    def _fetch_from_api(self, word_length: int) -> Optional[str]:
        url = f"https://random-word-api.herokuapp.com/word?length={word_length}"
        try:
            resp = requests.get(url, timeout=6)
            if resp.ok:
                payload = resp.json()
                if isinstance(payload, list) and payload:
                    candidate = str(payload[0]).lower()
                    return "".join(ch for ch in candidate if ch in string.ascii_lowercase)
        except Exception as e:
            logger.debug("requests error: %s", e)
        return None

    async def _safe_defer(self, interaction: discord.Interaction) -> None:
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
        except Exception:
            # If defer fails (rare) just continue; we'll try to send followup
            pass

    async def _safe_send(self, interaction: discord.Interaction, content: str, ephemeral: bool = False) -> None:
        try:
            await interaction.response.send_message(content, ephemeral=ephemeral)
        except Exception:
            try:
                await interaction.followup.send(content, ephemeral=ephemeral)
            except Exception:
                logger.debug("Failed to send message to interaction")

    async def _safe_followup(self, interaction: discord.Interaction, content: str, ephemeral: bool = False) -> None:
        try:
            await interaction.followup.send(content, ephemeral=ephemeral)
        except Exception:
            logger.debug("Failed to send followup message")


# Singleton instance used by the cog
hangman_starter = HangmanStarter(message_editor)