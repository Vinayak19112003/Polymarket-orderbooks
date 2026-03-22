"""Health monitoring: stale stream detection, periodic stats logging."""

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
                self._check()
            except Exception as e:
                logger.error(f"Health check error: {e}", exc_info=True)

    def stop(self):
        self._running = False

    def _check(self):
        now_ms = int(time.time() * 1000)
        timeout_ms = self._config.stale_stream_timeout_s * 1000
        states = self._book_mgr.get_all_active_states()
        storage_stats = self._storage.stats

        n_connected = sum(1 for s in states.values() if s.ws_connected)
        n_stale = 0

        for cid, state in states.items():
            if state.last_message_ts > 0 and (now_ms - state.last_message_ts) > timeout_ms:
                n_stale += 1
                logger.warning(
                    f"Stale stream: {state.token.outcome}:{state.token.window_slug[:25]} "
                    f"(last msg {(now_ms - state.last_message_ts) / 1000:.0f}s ago)"
                )

        logger.info(
            f"Health: streams={len(states)} connected={n_connected} stale={n_stale} | "
            f"snap_buf={storage_stats['snapshots_pending']} tick_buf={storage_stats['ticks_pending']}"
        )
