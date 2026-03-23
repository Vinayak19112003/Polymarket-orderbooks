"""Entry point: wires all modules and runs the collector."""

import asyncio
import signal
import logging

import aiohttp

from .config import CollectorConfig
from .logging_setup import setup_logging
from .storage import Storage
from .market_manager import MarketManager
from .book_manager import BookManager
from .snapshot_scheduler import SnapshotScheduler
from .btc_price import BtcPriceFetcher
from .health import HealthMonitor

logger = logging.getLogger("collector.main")


class Collector:
    def __init__(self):
        self.config = CollectorConfig()
        setup_logging(self.config)

        self.storage = Storage(self.config)
        self._session: aiohttp.ClientSession | None = None
        self._shutdown = asyncio.Event()

    async def run(self):
        logger.info("=== Polymarket BTC 15m Collector starting ===")
        logger.info(f"Config: slug_pattern={self.config.slug_pattern} "
                     f"snapshot_interval={self.config.snapshot_interval_s}s "
                     f"data_dir={self.config.data_dir}")

        self._session = aiohttp.ClientSession()

        market_mgr = MarketManager(self.config, self._session)
        book_mgr = BookManager(self.config, self._session, self.storage.write_tick)
        btc_fetcher = BtcPriceFetcher(self._session)
        snap_sched = SnapshotScheduler(
            self.config, book_mgr, market_mgr, btc_fetcher, self.storage.write_snapshot
        )
        health = HealthMonitor(self.config, book_mgr, market_mgr, self.storage)

        # Register shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._shutdown.set)

        # Start background tasks
        tasks = [
            asyncio.create_task(self._discovery_loop(market_mgr, book_mgr), name="discovery"),
            asyncio.create_task(snap_sched.run(), name="snapshots"),
            asyncio.create_task(health.run(), name="health"),
        ]

        try:
            await self._shutdown.wait()
        finally:
            logger.info("Shutting down...")
            snap_sched.stop()
            health.stop()

            for cid in list(book_mgr.get_all_active_states().keys()):
                await book_mgr.stop_token(cid)

            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            self.storage.flush_all()

            if self._session:
                await self._session.close()

            logger.info("=== Collector stopped ===")

    async def _discovery_loop(self, market_mgr: MarketManager, book_mgr: BookManager):
        interval = self.config.discovery_interval_s

        while not self._shutdown.is_set():
            try:
                new_windows, new_tokens = await market_mgr.discover()
                for tok in new_tokens:
                    await book_mgr.start_token(tok)

                expired = market_mgr.get_expired_token_ids()
                for cid in expired:
                    await book_mgr.stop_token(cid)

                await market_mgr.check_resolutions()

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"Discovery loop error: {e}", exc_info=True)

            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass


def main():
    collector = Collector()
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
