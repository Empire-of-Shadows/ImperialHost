# Python
from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Awaitable, Callable, Dict, Optional

from utilities.logger_setup import get_logger, PerformanceLogger, log_context

logger = get_logger("Timers")


@dataclass
class TimerHandle:
    name: str
    task: asyncio.Task
    created_ts: float = field(default_factory=lambda: time.monotonic())
    interval: Optional[float] = None  # None => one-shot
    next_run_in: Optional[float] = None

    def is_interval(self) -> bool:
        return self.interval is not None

    def cancelled(self) -> bool:
        return self.task.cancelled()

    def done(self) -> bool:
        return self.task.done()


class TimerManager:
    """
    Centralized manager for one-shot and repeating timers with:
    - structured logging
    - performance measurements
    - safe exception handling
    - graceful shutdown & cancellation
    """

    def __init__(self) -> None:
        self._timers: Dict[str, TimerHandle] = {}
        self._lock = asyncio.Lock()
        self._running = True

    async def shutdown(self, wait: bool = True) -> None:
        """
        Cancel all timers and optionally wait for them to finish.
        """
        async with self._lock:
            self._running = False
            timers = list(self._timers.values())
            self._timers.clear()

        if not timers:
            logger.info("TimerManager shutdown: no timers to cancel")
            return

        logger.info(f"TimerManager shutdown: cancelling {len(timers)} timers")
        for th in timers:
            th.task.cancel()

        if wait:
            await asyncio.gather(*(th.task for th in timers), return_exceptions=True)
        logger.info("TimerManager shutdown complete")

    async def schedule_once(
        self,
        name: str,
        delay: float,
        func: Callable[..., Any] | Callable[..., Awaitable[Any]],
        *args,
        **kwargs,
    ) -> TimerHandle:
        """
        Schedule a single execution after `delay` seconds.
        """
        if delay < 0:
            raise ValueError("delay must be >= 0")

        async with self._lock:
            if not self._running:
                raise RuntimeError("TimerManager is not running")
            if name in self._timers:
                raise ValueError(f"Timer '{name}' already exists")

            logger.debug(f"Scheduling one-shot timer '{name}' in {delay:.3f}s")
            task = asyncio.create_task(
                self._runner_once(name, delay, func, *args, **kwargs),
                name=f"timer-once:{name}",
            )
            th = TimerHandle(name=name, task=task, interval=None, next_run_in=delay)
            self._timers[name] = th
            return th


    async def schedule_interval(
        self,
        name: str,
        interval: float,
        func: Callable[..., Any] | Callable[..., Awaitable[Any]],
        *args,
        initial_delay: Optional[float] = None,
        **kwargs,
    ) -> TimerHandle:
        """
        Schedule a repeating execution every `interval` seconds.
        Optional `initial_delay` before the first run (defaults to interval).
        """
        if interval <= 0:
            raise ValueError("interval must be > 0")

        delay = interval if initial_delay is None else max(0.0, float(initial_delay))

        async with self._lock:
            if not self._running:
                raise RuntimeError("TimerManager is not running")
            if name in self._timers:
                raise ValueError(f"Timer '{name}' already exists")

            logger.debug(
                f"Scheduling interval timer '{name}' every {interval:.3f}s "
                f"(first run in {delay:.3f}s)"
            )
            task = asyncio.create_task(
                self._runner_interval(name, interval, delay, func, *args, **kwargs),
                name=f"timer-interval:{name}",
            )
            th = TimerHandle(name=name, task=task, interval=interval, next_run_in=delay)
            self._timers[name] = th
            return th


    async def cancel(self, name: str) -> bool:
        """
        Cancel a timer by name.
        Returns True if cancelled, False if not found.
        """
        async with self._lock:
            th = self._timers.pop(name, None)

        if not th:
            logger.debug(f"cancel('{name}'): no such timer")
            return False

        logger.info(f"Cancelling timer '{name}'")
        th.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await th.task
        return True

    async def exists(self, name: str) -> bool:
        async with self._lock:
            return name in self._timers

    async def list(self) -> Dict[str, Dict[str, Any]]:
        """
        Lightweight status view for diagnostics.
        """
        async with self._lock:
            out = {}
            for name, th in self._timers.items():
                out[name] = {
                    "interval": th.interval,
                    "cancelled": th.cancelled(),
                    "done": th.done(),
                    "age_s": round(time.monotonic() - th.created_ts, 3),
                    "next_run_in": th.next_run_in,
                }
            return out

    async def _runner_once(
        self,
        name: str,
        delay: float,
        func: Callable[..., Any] | Callable[..., Awaitable[Any]],
        *args,
        **kwargs,
    ) -> None:
        try:
            await asyncio.sleep(delay)
            await self._invoke(name, func, *args, **kwargs)
        except asyncio.CancelledError:
            logger.debug(f"One-shot timer '{name}' cancelled")
            raise
        except Exception:
            logger.exception(f"Timer '{name}' failed")
        finally:
            # cleanup from registry
            async with self._lock:
                self._timers.pop(name, None)


    async def _runner_interval(
        self,
        name: str,
        interval: float,
        delay: float,
        func: Callable[..., Any] | Callable[..., Awaitable[Any]],
        *args,
        **kwargs,
    ) -> None:
        try:
            await asyncio.sleep(delay)
            while True:
                await self._invoke(name, func, *args, **kwargs)

                async with self._lock:
                    th = self._timers.get(name)
                    if th:
                        th.next_run_in = interval

                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.debug(f"Interval timer '{name}' cancelled")
            raise
        except Exception:
            logger.exception(f"Interval timer '{name}' encountered an error")
        finally:
            # cleanup from registry
            async with self._lock:
                self._timers.pop(name, None)


    async def _invoke(
        self,
        name: str,
        func: Callable[..., Any] | Callable[..., Awaitable[Any]],
        *args,
        **kwargs,
    ) -> None:
        """
        Invoke the callback with performance logging, handling both sync and async callables.
        """
        op_name = f"timer:{name}"
        with PerformanceLogger(logger, operation_name=op_name):
            # Add a lightweight context log around user code
            with log_context(logger, f"{op_name}:invoke", level=20):  # INFO
                if inspect.iscoroutinefunction(func):
                    await func(*args, **kwargs)  # type: ignore[misc]
                else:
                    # Run sync work in the default loop executor to avoid blocking
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, partial(func, *args, **kwargs))


def get_10_min_countdown_timestamp() -> str:
    """
    Returns a Discord timestamp for 10 minutes from now with countdown formatting.
    """
    import datetime
    future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
    unix_timestamp = int(future_time.timestamp())
    return f"<t:{unix_timestamp}:R>"

def get_1_min_countdown_timestamp() -> str:
    """
    Returns a Discord timestamp for 1 minute from now with countdown formatting.
    """
    import datetime
    future_time = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=1)
    unix_timestamp = int(future_time.timestamp())
    return f"<t:{unix_timestamp}:R>"
