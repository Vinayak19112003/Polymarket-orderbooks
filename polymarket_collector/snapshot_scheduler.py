"""1-second snapshot loop synchronized to wall clock."""

import asyncio
import time
import logging
from typing import Callable, Awaitable

from .config import CollectorConfig
from .models import Snapshot
from .book_manager import BookManager
from .market_manager import MarketManager

logger = logging.getLogger("collector.snapshots")


class SnapshotScheduler:
    def __init__(
        self,
        config: CollectorConfig,
        book_manager: BookManager,
        market_manager: MarketManager,
        on_snapshot: Callable[[Snapshot], Awaitable[None]],
    ):
        self._config = config
        self._book_mgr = book_manager
        self._mkt_mgr = market_manager
        self._on_snapshot = on_snapshot
        self._running = False

    async def run(self):
        self._running = True
        interval = self._config.snapshot_interval_s
        logger.info(f"Snapshot scheduler started (interval={interval}s)")

        while self._running:
            t0 = time.time()
            ts_ms = int(t0 * 1000)

            try:
                await self._tick(ts_ms)
            except Exception as e:
                logger.error(f"Snapshot tick error: {e}", exc_info=True)

            # Sleep until next aligned tick
            elapsed = time.time() - t0
            sleep_for = max(0, interval - elapsed)
            await asyncio.sleep(sleep_for)

    def stop(self):
        self._running = False

    async def _tick(self, ts_ms: int):
        depth = self._config.snapshot_book_depth
        now_s = ts_ms // 1000
        states = self._book_mgr.get_all_active_states()

        for cid, state in states.items():
            if not state.book.bids and not state.book.asks:
                continue

            window = self._mkt_mgr.get_window(state.token.window_slug)
            if not window:
                continue

            # Only snapshot windows in their live trading period
            if now_s < window.start_ts or now_s > window.end_ts:
                continue

            book = state.book
            counters = state.reset_tick_counters()

            bid_levels = book.bids[:depth]
            ask_levels = book.asks[:depth]

            snap = Snapshot(
                ts_ms=ts_ms,
                window_slug=state.token.window_slug,
                window_end_ts=window.end_ts,
                outcome=state.token.outcome,
                clob_token_id=cid,
                best_bid=book.best_bid,
                best_ask=book.best_ask,
                mid=book.mid,
                spread=book.spread,
                bid_prices=[l.price for l in bid_levels],
                bid_sizes=[l.size for l in bid_levels],
                ask_prices=[l.price for l in ask_levels],
                ask_sizes=[l.size for l in ask_levels],
                total_bid_size=book.total_bid_size(depth),
                total_ask_size=book.total_ask_size(depth),
                bid_levels=len(book.bids),
                ask_levels=len(book.asks),
                imbalance_5=book.imbalance(5),
                n_price_changes=counters.get("price_change", 0),
                n_trades=counters.get("trade", 0),
                last_trade_price=state.last_trade_price,
                last_trade_size=state.last_trade_size,
            )
            await self._on_snapshot(snap)
