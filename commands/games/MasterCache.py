# Python
import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, Iterable
import traceback

from utilities.logger_setup import get_logger, PerformanceLogger

logger = get_logger("MasterCache")


class MasterCache:
	"""
    A simple write-back cache for Counting game state and leaderboard updates.

    - States are kept in memory and periodically flushed to DB.
    - Leaderboard increments are batched and flushed periodically.
    - Leaderboard reads are served from an in-memory cache merged with pending increments.
    """

	def __init__(
			self,
			state_collection,
			leaderboard_collection,
			flush_interval: float = 30.0,
			state_ttl: float = 60.0,
			leaderboard_ttl: float | None = None,
	):
		logger.debug(
			f"MasterCache.__init__(flush_interval={flush_interval}, state_ttl={state_ttl}, "
			f"leaderboard_ttl={leaderboard_ttl})"
		)
		self.state_collection = state_collection
		self.leaderboard_collection = leaderboard_collection

		self.flush_interval = float(flush_interval)
		self.state_ttl = float(state_ttl)
		self.leaderboard_ttl = float(leaderboard_ttl) if leaderboard_ttl is not None else float(state_ttl)

		# In-memory caches
		self._state_cache: Dict[int, Dict[str, Any]] = {}
		self._state_meta: Dict[int, float] = {}  # last fetch time (monotonic)
		self._dirty_states: set[int] = set()

		# Leaderboard increments are batched
		self._lb_deltas: Dict[str, int] = {}

		# Leaderboard read cache (base snapshot loaded from DB; merged with _lb_deltas on read)
		self._lb_cache: Dict[str, int] | None = None
		self._lb_meta_ts: float = 0.0  # last fetch time (monotonic)

		# Concurrency
		self._lock = asyncio.Lock()
		self._flush_task: Optional[asyncio.Task] = None
		self._running = False

		# Optional periodic JSON dump
		self._dump_task: Optional[asyncio.Task] = None
		self._dump_path: Optional[Path] = None
		self._dump_interval: float = 0.0

		# Performance and reliability metrics
		self._stats = {
			'cache_hits': 0,
			'cache_misses': 0,
			'db_reads': 0,
			'db_writes': 0,
			'flush_count': 0,
			'errors': 0,
			'last_flush_duration': 0.0,
			'total_flush_time': 0.0
		}

		# Error tracking for resilience
		self._consecutive_errors = 0
		self._max_consecutive_errors = 5
		self._error_backoff_factor = 2.0
		self._base_retry_delay = 1.0

		logger.info(f"MasterCache initialized with flush_interval={flush_interval}s, state_ttl={state_ttl}s")

	def get_stats(self) -> Dict[str, Any]:
		"""Return cache performance statistics"""
		cache_size = len(self._state_cache)
		dirty_count = len(self._dirty_states)
		pending_deltas = len(self._lb_deltas)

		stats = dict(self._stats)
		stats.update({
			'cache_size': cache_size,
			'dirty_states': dirty_count,
			'pending_deltas': pending_deltas,
			'hit_rate': self._stats['cache_hits'] / max(1, self._stats['cache_hits'] + self._stats['cache_misses']),
			'avg_flush_duration': self._stats['total_flush_time'] / max(1, self._stats['flush_count']),
			'running': self._running,
			'consecutive_errors': self._consecutive_errors
		})

		logger.debug(f"Cache stats: {stats}")
		return stats

	def log_cache_stats(self):
		"""Log detailed cache statistics"""
		stats = self.get_stats()
		logger.info(
			f"Cache Stats - Size: {stats['cache_size']}, "
			f"Dirty: {stats['dirty_states']}, "
			f"Pending: {stats['pending_deltas']}, "
			f"Hit Rate: {stats['hit_rate']:.2%}, "
			f"Avg Flush: {stats['avg_flush_duration']:.3f}s, "
			f"Errors: {stats['consecutive_errors']}"
		)

	async def start(self):
		logger.debug("MasterCache.start() called")
		if self._running:
			logger.debug("MasterCache already running; start() is a no-op")
			return
		self._running = True
		self._flush_task = asyncio.create_task(self._flush_loop(), name="MasterCacheFlushLoop")
		logger.info("MasterCache started.")

	async def shutdown(self):
		logger.info("MasterCache shutting down: flushing pending changes...")
		self._running = False

		# Log final stats
		self.log_cache_stats()

		# Stop periodic dump loop
		if self._dump_task:
			logger.debug("Stopping JSON dump loop task")
			self._dump_task.cancel()
			try:
				await self._dump_task
			except asyncio.CancelledError:
				pass
			self._dump_task = None

		# Stop flush loop
		if self._flush_task:
			logger.debug("Stopping flush loop task")
			self._flush_task.cancel()
			try:
				await self._flush_task
			except asyncio.CancelledError:
				pass
			self._flush_task = None

		# Final flush to DB
		try:
			await self.flush_once()
		except Exception as e:
			logger.error(f"Error during final flush: {e}", exc_info=True)

		logger.info("MasterCache shutdown complete.")

	async def preload_states(self, channel_ids: Iterable[int]) -> None:
		channel_list = list(channel_ids)
		logger.debug(f"preload_states(channel_ids={channel_list})")

		with PerformanceLogger(logger, f"preload_states({len(channel_list)} channels)"):
			tasks = [self.get_state(cid) for cid in channel_list]
			try:
				results = await asyncio.gather(*tasks, return_exceptions=True)

				# Count successes and failures
				successes = sum(1 for r in results if not isinstance(r, Exception))
				failures = len(results) - successes

				if failures > 0:
					logger.warning(f"Preload completed with {failures} failures out of {len(results)} channels")
					for i, result in enumerate(results):
						if isinstance(result, Exception):
							logger.error(f"Failed to preload channel {channel_list[i]}: {result}")
				else:
					logger.info(f"Successfully preloaded states for {successes} channels")

			except Exception as e:
				logger.error(f"Critical error during preload: {e}", exc_info=True)
				raise

	async def preload_leaderboard(self) -> None:
		logger.debug("preload_leaderboard()")
		with PerformanceLogger(logger, "preload_leaderboard"):
			try:
				await self._ensure_lb_cache(refresh=True)
				logger.info("Leaderboard cache preloaded successfully")
			except Exception as e:
				logger.error(f"Failed to preload leaderboard: {e}", exc_info=True)
				raise

	# Python
	async def get_state(self, channel_id: int) -> Dict[str, Any]:
		now = time.monotonic()
		logger.debug(f"get_state(channel_id={channel_id})")

		async with self._lock:
			# If we have the state in cache, check if it's still valid OR if it's dirty (not yet flushed)
			if channel_id in self._state_cache:
				is_fresh = (now - self._state_meta.get(channel_id, 0.0) <= self.state_ttl)
				is_dirty = channel_id in self._dirty_states

				if is_fresh or is_dirty:
					self._stats['cache_hits'] += 1
					logger.debug(f"State cache hit for channel {channel_id} (fresh={is_fresh}, dirty={is_dirty})")
					return self._state_cache[channel_id]

		# Cache miss - fetch from DB
		self._stats['cache_misses'] += 1
		self._stats['db_reads'] += 1
		logger.debug(f"State cache miss for channel {channel_id}; fetching from DB")

		try:
			with PerformanceLogger(logger, f"fetch_state_from_db(channel_id={channel_id})"):
				db_doc = await self.state_collection.find_one({"_id": str(channel_id)})

			if db_doc is None:
				# Treat missing state as a failure instead of fabricating a default
				raise LookupError(f"No state found for channel {channel_id}")

			state = db_doc

			async with self._lock:
				self._state_cache[channel_id] = state
				self._state_meta[channel_id] = now
				logger.debug(f"State cached for channel {channel_id}")
				return state

		except Exception as e:
			self._stats['errors'] += 1
			self._consecutive_errors += 1
			logger.error(f"Failed to fetch state for channel {channel_id}: {e}", exc_info=True)
			# Do not return a default state; let callers handle the failure
			raise

	async def update_state(self, channel_id: int, partial: Dict[str, Any]) -> Dict[str, Any]:
		logger.debug(f"update_state(channel_id={channel_id}, keys={list(partial.keys())})")

		try:
			async with self._lock:
				state = self._state_cache.get(channel_id)
			if state is None:
				logger.debug(f"update_state: state not cached for {channel_id}, fetching...")
				state = await self.get_state(channel_id)

			async with self._lock:
				state.update(partial)
				self._dirty_states.add(channel_id)
				self._state_cache[channel_id] = state
				logger.debug(f"State updated and marked dirty for channel {channel_id}")
				return state

		except Exception as e:
			self._stats['errors'] += 1
			logger.error(f"Failed to update state for channel {channel_id}: {e}", exc_info=True)
			raise

	async def replace_state(self, channel_id: int, new_state: Dict[str, Any]) -> Dict[str, Any]:
		logger.debug(f"replace_state(channel_id={channel_id})")

		try:
			new_state["_id"] = str(channel_id)
			async with self._lock:
				self._state_cache[channel_id] = new_state
				self._state_meta[channel_id] = time.monotonic()
				self._dirty_states.add(channel_id)
				logger.debug(f"State replaced and marked dirty for channel {channel_id}")
				return new_state

		except Exception as e:
			self._stats['errors'] += 1
			logger.error(f"Failed to replace state for channel {channel_id}: {e}", exc_info=True)
			raise

	async def increment_leaderboard(self, user_id: int, amount: int = 1):
		logger.debug(f"increment_leaderboard(user_id={user_id}, amount={amount})")

		try:
			key = str(user_id)
			async with self._lock:
				old_value = self._lb_deltas.get(key, 0)
				self._lb_deltas[key] = old_value + int(amount)
				logger.debug(
					f"Queued leaderboard delta: {key} {old_value} -> {self._lb_deltas[key]} (pending size={len(self._lb_deltas)})")

		except Exception as e:
			self._stats['errors'] += 1
			logger.error(f"Failed to increment leaderboard for user {user_id}: {e}", exc_info=True)
			raise

	async def get_leaderboard(self, include_pending: bool = True) -> Dict[str, int]:
		logger.debug(f"get_leaderboard(include_pending={include_pending})")

		try:
			base = await self._ensure_lb_cache()
			if not include_pending:
				logger.debug("Returning base leaderboard without pending deltas")
				return dict(base)

			async with self._lock:
				if not self._lb_deltas:
					logger.debug("No pending deltas; returning base leaderboard")
					return dict(base)

				merged = dict(base)
				for k, inc in self._lb_deltas.items():
					merged[k] = merged.get(k, 0) + inc
				logger.debug(f"Returning merged leaderboard (base+{len(self._lb_deltas)} pending)")
				return merged

		except Exception as e:
			self._stats['errors'] += 1
			logger.error(f"Failed to get leaderboard: {e}", exc_info=True)
			raise

	async def flush_once(self):
		flush_start = time.perf_counter()
		logger.debug("flush_once() started")

		try:
			# Snapshot to minimize lock duration
			async with self._lock:
				dirty = list(self._dirty_states)
				self._dirty_states.clear()

				lb_deltas = self._lb_deltas
				self._lb_deltas = {}

				states_to_write = {cid: dict(self._state_cache[cid]) for cid in dirty}

			logger.info(f"Flushing {len(states_to_write)} states and {len(lb_deltas)} leaderboard deltas to DB")

			any_writes = False
			state_errors = 0
			lb_errors = 0

			# Write states with individual error handling
			for cid, state in states_to_write.items():
				try:
					with PerformanceLogger(logger, f"flush_state({cid})"):
						await self.state_collection.update_one({"_id": str(cid)}, {"$set": state}, upsert=True)
					any_writes = True
					self._stats['db_writes'] += 1
					logger.debug(f"State flushed for channel {cid}")
				except Exception as e:
					state_errors += 1
					self._stats['errors'] += 1
					logger.error(f"Failed to flush state for channel {cid}: {e}", exc_info=True)

					# Re-mark as dirty for retry
					async with self._lock:
						self._dirty_states.add(cid)

			# Write leaderboard
			if lb_deltas:
				try:
					with PerformanceLogger(logger, f"flush_leaderboard({len(lb_deltas)} deltas)"):
						await self.leaderboard_collection.update_one({"_id": "leaderboard"}, {"$inc": lb_deltas},
																	 upsert=True)
					any_writes = True
					self._stats['db_writes'] += 1
					logger.debug(f"Leaderboard deltas flushed: {len(lb_deltas)} increments")

					# Update the in-memory leaderboard base so it stays consistent post-flush
					async with self._lock:
						if self._lb_cache is None:
							self._lb_cache = {}
						for k, inc in lb_deltas.items():
							self._lb_cache[k] = self._lb_cache.get(k, 0) + inc
						logger.debug("In-memory leaderboard base updated post-flush")
				except Exception as e:
					lb_errors += 1
					self._stats['errors'] += 1
					logger.error(f"Failed to flush leaderboard deltas: {e}", exc_info=True)

					# Re-queue deltas for retry
					async with self._lock:
						for k, inc in lb_deltas.items():
							self._lb_deltas[k] = self._lb_deltas.get(k, 0) + inc

			# Update error tracking
			total_errors = state_errors + lb_errors
			if total_errors == 0:
				self._consecutive_errors = 0  # Reset on success
			else:
				self._consecutive_errors += total_errors
				logger.warning(f"Flush completed with {total_errors} errors (consecutive: {self._consecutive_errors})")

			# After DB flush, dump snapshot into cache folder with rotation (keep last 10)
			if any_writes:
				try:
					logger.debug("Dumping rotated cache snapshot (keep=10)")
					await self.dump_rotate(folder="cache", prefix="cache_dump", keep=10, pretty=True, include_meta=True)
				except Exception as e:
					logger.error(f"Failed rotating cache dump after flush: {e}", exc_info=True)
			else:
				logger.debug("No writes performed; skipping cache dump rotation")

			# Update performance stats
			flush_duration = time.perf_counter() - flush_start
			self._stats['flush_count'] += 1
			self._stats['last_flush_duration'] = flush_duration
			self._stats['total_flush_time'] += flush_duration

			logger.debug(f"flush_once() completed in {flush_duration:.3f}s")

		except Exception as e:
			self._stats['errors'] += 1
			self._consecutive_errors += 1
			logger.error(f"Critical error in flush_once(): {e}", exc_info=True)
			raise

	async def _flush_loop(self):
		logger.debug("Flush loop started")
		try:
			while self._running:
				try:
					# Implement exponential backoff for consecutive errors
					if self._consecutive_errors >= self._max_consecutive_errors:
						backoff_delay = min(300, int(self._base_retry_delay * (self._error_backoff_factor ** self._consecutive_errors)))
						logger.warning(
							f"Too many consecutive errors ({self._consecutive_errors}), backing off for {backoff_delay:.1f}s")
						await asyncio.sleep(backoff_delay)
					else:
						await asyncio.sleep(self.flush_interval)

					await self.flush_once()

					# Log stats periodically (every 10 flushes)
					if self._stats['flush_count'] % 10 == 0:
						self.log_cache_stats()

				except Exception as e:
					logger.error(f"Error in flush loop: {e}", exc_info=True)
					# Continue running even if flush fails

		except asyncio.CancelledError:
			logger.debug("Flush loop cancelled")
		except Exception as e:
			logger.error(f"Fatal error in flush loop: {e}", exc_info=True)
		finally:
			logger.debug("Flush loop exited")

	# ---------- Leaderboard cache helpers ----------

	async def _ensure_lb_cache(self, refresh: bool = False) -> Dict[str, int]:
		logger.debug(f"_ensure_lb_cache(refresh={refresh})")
		now = time.monotonic()
		async with self._lock:
			need_fetch = (
					refresh
					or self._lb_cache is None
					or (now - self._lb_meta_ts) > self.leaderboard_ttl
			)
		if not need_fetch:
			logger.debug("Leaderboard cache is fresh; returning current snapshot")
			async with self._lock:
				return dict(self._lb_cache or {})

		logger.debug("Fetching leaderboard from DB")
		try:
			with PerformanceLogger(logger, "fetch_leaderboard_from_db"):
				doc = await self.leaderboard_collection.find_one({"_id": "leaderboard"})

			base = {}
			if doc:
				base = {k: int(v) for k, v in doc.items() if k != "_id"}

			async with self._lock:
				self._lb_cache = base
				self._lb_meta_ts = time.monotonic()
				logger.info(f"Leaderboard cache refreshed (size={len(base)})")
				return dict(self._lb_cache)

		except Exception as e:
			self._stats['errors'] += 1
			logger.error(f"Failed to fetch leaderboard from DB: {e}", exc_info=True)

			# Return existing cache or empty dict as fallback
			async with self._lock:
				fallback = dict(self._lb_cache or {})
			logger.warning(f"Returning cached/empty leaderboard due to DB error (size={len(fallback)})")
			return fallback

	# ---------- JSON snapshot/dump utilities ----------

	async def snapshot(self, include_meta: bool = False) -> Dict[str, Any]:
		logger.debug(f"snapshot(include_meta={include_meta})")
		async with self._lock:
			state_cache = {str(cid): dict(state) for cid, state in self._state_cache.items()}
			data: Dict[str, Any] = {
				"states": state_cache,
				"leaderboard_cached_base": dict(self._lb_cache or {}),
				"leaderboard_pending_increments": dict(self._lb_deltas),
			}
			if include_meta:
				data["meta"] = {
					"last_fetch_monotonic": {str(cid): ts for cid, ts in self._state_meta.items()},
					"leaderboard_last_fetch_monotonic": self._lb_meta_ts,
					"dirty_channels": [str(cid) for cid in self._dirty_states],
					"flush_interval": self.flush_interval,
					"state_ttl": self.state_ttl,
					"leaderboard_ttl": self.leaderboard_ttl,
					"running": self._running,
					"stats": self.get_stats()
				}
			logger.debug(
				f"Snapshot built: states={len(state_cache)}, "
				f"lb_base={len(data['leaderboard_cached_base'])}, "
				f"lb_pending={len(data['leaderboard_pending_increments'])}, include_meta={include_meta}"
			)
			return data

	async def to_json(self, pretty: bool = True, include_meta: bool = False) -> str:
		logger.debug(f"to_json(pretty={pretty}, include_meta={include_meta})")
		snap = await self.snapshot(include_meta=include_meta)
		if pretty:
			return json.dumps(snap, ensure_ascii=False, indent=2, sort_keys=True)
		return json.dumps(snap, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

	async def dump_to_file(self, path: str | Path, pretty: bool = True, include_meta: bool = False) -> Path:
		logger.debug(f"dump_to_file(path={path}, pretty={pretty}, include_meta={include_meta})")
		p = Path(path).expanduser().resolve()
		p.parent.mkdir(parents=True, exist_ok=True)

		try:
			with PerformanceLogger(logger, f"dump_to_file({p.name})"):
				txt = await self.to_json(pretty=pretty, include_meta=include_meta)
				p.write_text(txt, encoding="utf-8")
			logger.info(f"MasterCache JSON dump written to: {p}")
		except Exception as e:
			logger.error(f"Failed to write MasterCache JSON dump to {p}: {e}", exc_info=True)
			raise
		return p

	async def dump_rotate(
			self,
			folder: str | Path = "cache",
			prefix: str = "cache_dump",
			keep: int = 10,
			pretty: bool = True,
			include_meta: bool = True,
	) -> Path:
		logger.debug(
			f"dump_rotate(folder={folder}, prefix={prefix}, keep={keep}, "
			f"pretty={pretty}, include_meta={include_meta})"
		)

		try:
			folder_path = Path(folder).expanduser().resolve()
			folder_path.mkdir(parents=True, exist_ok=True)

			ts = time.strftime("%Y%m%d-%H%M%S")
			filename = f"{prefix}_{ts}.json"
			target = folder_path / filename

			txt = await self.to_json(pretty=pretty, include_meta=include_meta)
			target.write_text(txt, encoding="utf-8")
			logger.info(f"MasterCache rotated JSON dump written to: {target}")

			# Prune older files beyond `keep`
			try:
				pattern = f"{prefix}_*.json"
				files = sorted(folder_path.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
				logger.debug(f"Found {len(files)} dumps; pruning beyond keep={keep}")
				pruned = 0
				if len(files) > keep:
					for old in files[keep:]:
						try:
							old.unlink(missing_ok=True)
							pruned += 1
							logger.debug(f"Pruned old dump: {old}")
						except Exception as e:
							logger.warning(f"Failed to remove old dump {old}: {e}")
				if pruned > 0:
					logger.info(f"Pruned {pruned} old cache dumps")
			except Exception as e:
				logger.warning(f"Failed to prune old dumps in {folder_path}: {e}")

			return target

		except Exception as e:
			logger.error(f"Failed to rotate dump: {e}", exc_info=True)
			raise

	async def start_json_dump(self, path: str | Path, interval: float = 10.0, pretty: bool = True,
							  include_meta: bool = False):
		logger.debug(f"start_json_dump(path={path}, interval={interval}, pretty={pretty}, include_meta={include_meta})")
		if self._dump_task:
			logger.warning("JSON dump loop already running; restarting with new parameters.")
			await self.stop_json_dump()

		self._dump_path = Path(path).expanduser().resolve()
		self._dump_interval = float(interval)

		async def _dump_loop():
			logger.debug("JSON dump loop started")
			try:
				while True:
					try:
						await self.dump_to_file(self._dump_path, pretty=pretty, include_meta=include_meta)
					except Exception as e:
						logger.error(f"Error in JSON dump loop: {e}", exc_info=True)
					await asyncio.sleep(self._dump_interval)
			except asyncio.CancelledError:
				logger.debug("JSON dump loop cancelled; doing a final dump")
				try:
					await self.dump_to_file(self._dump_path, pretty=pretty, include_meta=include_meta)
				except Exception:
					logger.debug("Final dump failed, but that's okay during shutdown")
			finally:
				logger.debug("JSON dump loop exited")

		self._dump_task = asyncio.create_task(_dump_loop(), name="MasterCacheJsonDumpLoop")
		logger.info(f"MasterCache JSON dump loop started: {self._dump_path} every {self._dump_interval:.1f}s")

	async def stop_json_dump(self):
		logger.debug("stop_json_dump()")
		if not self._dump_task:
			logger.debug("No JSON dump loop to stop")
			return
		self._dump_task.cancel()
		try:
			await self._dump_task
		except asyncio.CancelledError:
			pass
		self._dump_task = None
		logger.info("MasterCache JSON dump loop stopped.")