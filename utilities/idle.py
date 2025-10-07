# Python
import random
from typing import Dict, Optional

import discord
from discord.ext import tasks

from utilities.bot import bot
from utilities.logger_setup import get_logger, PerformanceLogger

logger = get_logger(__name__)

status_options: Dict[str, object] = {
    "playing": [
        "with codes 👾",
        "a fun game 🎮",
        "hide and seek 🤫",
        "an exciting new feature 🛠️",
        "kick the bucket",
        "hide-and-seek in the shadows 🕵️",
        "your favorite games with friends 🎮",
        "memes IRL 📸",
        "guardian of Freebies and Drops 🛡️",
        "infinite co-op adventures ⚔️",
        "the soundtrack of the Empire 🎵",
    ],
    "watching": [
        "for help 🌌",
        "the server activity 👀",
        "Hunger Force 🖥️",
        "the stars ✨",
        "you invite friends",
        "the shadows grow darker... 🌑",
        "for new Prime Drops 👀",
        "gamers strategize in The Room 🎧",
        "you claim epic Freebies 🎁",
        "your gaming achievements unfold 🏆",
        "for the next big update 📣",
    ],
    "listening": [
        "community feedback 🎧",
        "lofi beats to listen to 🎵",
        "your comments ",
        "the sound of silence 😌",
        "the whispers of the shadows 🌌",
        "your epic gaming tales 🎮",
        "the crackling fire of memes 🔥",
        "the sound of victory 🏆",
        "players strategizing in The Room 🎧",
        "the call of Freebies and Drops 🎁",
        "the heartbeat of the Empire 💓",
    ],
    "streaming": {
        "phrases": [
            "offline 🚀",
            "Discord bot coding 🔧",
            "community discussions 🌟",
            "open-source magic 💻",
            "memes from the void 📸",
            "Prime Drops and Freebies live 🪂🎁",
            "your gaming highlights 🎮✨",
            "the chaos of the Living Room 🛋️",
            "shadows across the Empire 🌑",
            "adventures in The Room 🎧",
            "a 24/7 gaming marathon ⚔️",
        ],
        "url": "https://twitch.tv/thegreateos",  # Shared URL for streaming
    },
}


def _pick_from_list(items: list) -> str:
    choice = random.choice(items)
    logger.debug(f"Picked status phrase: {choice}")
    return choice


def get_random_status() -> Dict[str, str]:
    """
    Select a random status config. Returns a dict with keys:
    - type: playing|watching|listening|streaming
    - name: text to display
    - url: only for streaming
    """
    status_type = random.choice(list(status_options.keys()))
    logger.debug(f"Selected status type: {status_type}")

    if status_type == "streaming":
        cfg = status_options.get("streaming", {})
        phrases = cfg.get("phrases") if isinstance(cfg, dict) else None
        url = cfg.get("url") if isinstance(cfg, dict) else None

        if not phrases or not isinstance(phrases, list):
            logger.warning("Streaming phrases are missing or invalid; falling back to 'playing'.")
            status_type = "playing"
        elif not url or not isinstance(url, str):
            logger.warning("Streaming URL is missing or invalid; falling back to 'playing'.")
            status_type = "playing"
        else:
            name = _pick_from_list(phrases)
            return {"type": "streaming", "name": name, "url": url}

    # Non-streaming types
    phrases = status_options.get(status_type)
    if not isinstance(phrases, list) or not phrases:
        logger.warning(f"Status list for type '{status_type}' is missing/empty; using a safe default.")
        return {"type": "playing", "name": "with defaults 🎲"}

    name = _pick_from_list(phrases)
    return {"type": status_type, "name": name}


def _status_to_activity(status: Dict[str, str]) -> Optional[discord.BaseActivity]:
    stype = status.get("type")
    name = status.get("name")
    if not stype or not name:
        logger.error(f"Invalid status payload: {status}")
        return None

    if stype == "playing":
        return discord.Game(name=name)
    if stype == "watching":
        return discord.Activity(type=discord.ActivityType.watching, name=name)
    if stype == "listening":
        return discord.Activity(type=discord.ActivityType.listening, name=name)
    if stype == "streaming":
        url = status.get("url")
        if not url:
            logger.warning("Streaming status has no URL; skipping this rotation.")
            return None
        return discord.Streaming(name=name, url=url)

    logger.warning(f"Unknown status type '{stype}'; skipping.")
    return None


@tasks.loop(seconds=30)
async def rotate_status():
    try:
        status = get_random_status()
        activity = _status_to_activity(status)
        if not activity:
            return

        with PerformanceLogger(logger, "bot.change_presence"):
            await bot.change_presence(activity=activity)

        # Structured summary log
        payload = {k: v for k, v in status.items() if k in {"type", "name", "url"}}
        logger.info(f"Rotated presence: {payload}")

    except discord.HTTPException as http_err:
        logger.error(f"Failed to change presence due to HTTPException: {http_err}")
    except Exception as e:
        logger.exception(f"Unexpected error during status rotation: {e}")


@rotate_status.before_loop
async def _before_rotate_status():
    logger.info("Waiting for bot to become ready before starting status rotation...")
    await bot.wait_until_ready()
    logger.info("Bot is ready. Status rotation will start.")


@rotate_status.error
async def _on_rotate_status_error(exc: Exception):
    # This handler is invoked for errors in the task loop
    logger.exception(f"rotate_status loop encountered an error: {exc}")


def start_status_rotation() -> None:
    """
    Safe helper to start the loop. Idempotent.
    """
    if not rotate_status.is_running():
        rotate_status.start()
        logger.info("Status rotation task started.")
    else:
        logger.debug("Status rotation task already running; start request ignored.")


def stop_status_rotation() -> None:
    """
    Safe helper to stop the loop. Idempotent.
    """
    if rotate_status.is_running():
        rotate_status.stop()
        logger.info("Status rotation task stopped.")
    else:
        logger.debug("Status rotation task not running; stop request ignored.")