"""Health monitoring: stale stream detection + auto re-seed."""

import asyncio
import time
import logging

from .config import CollectorConfig
from .book_manager import BookManager
from .market_manager import MarketManager
from .storage import Storage

logger = logging.getLogger("collector.health")


class HealthMonitor:
    def __init__(
        self,
        config: CollectorConfig,
        book_manager: BookManager,
        market_manager: MarketManager,
        storage: Storage,
    ):
        self._config = config
        self._book_mgr = book_manager
        self._mkt_mgr = market_manager
        self._storage = storage
        self._running = False

    async def run(self):
        self._running = True
        interval = self._config.health_check_interval_s
        logger.info(f"Health monitor started (interval={interval}s)")

        while self._running:
            await asyncio.sleep(interval)
            try:
                await self._check()
            except Exception as e:
                logger.error(f"Health check error: {e}", exc_info=True)

    def stop(self):
        self._running = False

    async def _check(self):
        now_ms = int(time.time() * 1000)
        now_s = now_ms // 1000
        timeout_ms = self._config.stale_stream_timeout_s * 1000
        states = self._book_mgr.get_all_active_states()
        storage_stats = self._storage.stats

        n_connected = sum(1 for s in states.values() if s.ws_connected)
        n_stale = 0
        reseed_tasks = []

        for cid, state in states.items():
            if state.last_message_ts > 0 and (now_ms - state.last_message_ts) > timeout_ms:
                n_stale += 1

                # Only re-seed if the window is currently live or starting soon
                window = self._mkt_mgr.get_window(state.token.window_slug)
                if window and window.start_ts - 60 <= now_s <= window.end_ts:
                    reseed_tasks.append(cid)

        # Re-seed stale streams that are in live windows
        reseeded = 0
        for cid in reseed_tasks[:20]:  # max 20 per health check
            try:
                await self._book_mgr.reseed_from_rest(cid)
                reseeded += 1
            except Exception as e:
                logger.warning(f"Reseed failed for {cid[:16]}: {e}")

        if reseeded:
            logger.info(f"Re-seeded {reseeded} stale streams from REST")

        logger.info(
            f"Health: streams={len(states)} connected={n_connected} stale={n_stale} | "
            f"snap_buf={storage_stats['snapshots_pending']} tick_buf={storage_stats['ticks_pending']}"
        )
