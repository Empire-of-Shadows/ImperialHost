import asyncio
from typing import Coroutine, Optional

from utilities.logger_setup import get_logger

logger = get_logger("GlobalLimiter")


class RequestDispatcher:
    def __init__(self, interval: float = 0.03):  # ~33 requests/sec
        self.queue: asyncio.Queue[Coroutine] = asyncio.Queue()
        self.interval = float(interval)
        self.task: Optional[asyncio.Task] = None
        self.running = False
        self._job_id_counter = 0

    def _next_job_id(self) -> int:
        self._job_id_counter += 1
        return self._job_id_counter

    async def start_worker(self):
        """Start the worker task safely when an event loop is running."""
        if self.task and not self.task.done():
            logger.debug("RequestDispatcher worker already running.")
            return

        logger.info("Starting RequestDispatcher worker.")
        self.running = True
        self.task = asyncio.create_task(self.worker(), name="RequestDispatcherWorker")

        def _on_done(t: asyncio.Task):
            err = t.exception()
            if err:
                logger.error("RequestDispatcher worker crashed", exc_info=err)
            else:
                logger.info("RequestDispatcher worker finished normally.")

        self.task.add_done_callback(_on_done)

    async def stop_worker(self, graceful: bool = True, drain_timeout: float = 5.0):
        """
        Stop the worker. If graceful=True, wait for the queue to drain (up to drain_timeout).
        """
        if not self.task:
            logger.debug("RequestDispatcher stop requested, but no worker task found.")
            return

        logger.info(
            "Stopping RequestDispatcher worker (graceful=%s, queue_size=%d).",
            graceful, self.queue.qsize()
        )

        self.running = False

        if graceful:
            try:
                await asyncio.wait_for(self.queue.join(), timeout=drain_timeout)
                logger.info("Queue drained successfully before shutdown.")
            except asyncio.TimeoutError:
                logger.warning(
                    "Timeout while draining queue (remaining=%d). Cancelling worker.",
                    self.queue.qsize()
                )

        if not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                logger.info("RequestDispatcher worker cancelled cleanly.")
            except Exception as e:
                logger.error("Error stopping RequestDispatcher worker.", exc_info=e)
        self.task = None

    async def worker(self):
        logger.info("RequestDispatcher worker loop started.")
        try:
            while self.running:
                try:
                    coro = await self.queue.get()
                except asyncio.CancelledError:
                    logger.info("Worker cancelled while waiting for queue item.")
                    break

                job_id = self._next_job_id()
                pending = self.queue.qsize()
                if pending > 0 and pending % 10 == 0:
                    # Periodic queue size info to avoid log spam
                    logger.warning("Queue backlog: %d pending tasks.", pending)

                logger.debug("Job #%d starting. pending=%d", job_id, pending)
                try:
                    await coro
                    logger.debug("Job #%d completed successfully.", job_id)
                except asyncio.CancelledError:
                    logger.info("Job #%d was cancelled.", job_id)
                except Exception as e:
                    logger.error("Job #%d failed.", job_id, exc_info=e)
                finally:
                    self.queue.task_done()

                # Rate limit interval
                await asyncio.sleep(self.interval)
        except Exception as e:
            logger.error("Unhandled exception in worker loop.", exc_info=e)
            raise
        finally:
            logger.info("RequestDispatcher worker loop exiting.")

    async def submit(self, coro: Coroutine):
        """
        Submit a coroutine to be executed respecting the rate limit.
        """
        await self.start_worker()
        size_before = self.queue.qsize()
        await self.queue.put(coro)
        logger.debug("Task enqueued. size_before=%d size_after=%d", size_before, self.queue.qsize())