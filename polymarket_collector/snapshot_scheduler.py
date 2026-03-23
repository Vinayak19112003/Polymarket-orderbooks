"""1-second snapshot loop — merges YES/NO into one row per window."""

import asyncio
import time
import logging
from datetime import datetime, timezone
from typing import Callable, Awaitable

from .config import CollectorConfig
from .models import MergedSnapshot
from .book_manager import BookManager
from .market_manager import MarketManager
from .btc_price import BtcPriceFetcher

logger = logging.getLogger("collector.snapshots")


class SnapshotScheduler:
    def __init__(
        self,
        config: CollectorConfig,
        book_manager: BookManager,
        market_manager: MarketManager,
        btc_fetcher: BtcPriceFetcher,
        on_snapshot: Callable[[MergedSnapshot], Awaitable[None]],
    ):
        self._config = config
        self._book_mgr = book_manager
        self._mkt_mgr = market_manager
        self._btc = btc_fetcher
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
                await self._btc.update()
                await self._tick(ts_ms)
            except Exception as e:
                logger.error(f"Snapshot tick error: {e}", exc_info=True)

            elapsed = time.time() - t0
            sleep_for = max(0, interval - elapsed)
            await asyncio.sleep(sleep_for)

    def stop(self):
        self._running = False

    async def _tick(self, ts_ms: int):
        now_s = ts_ms // 1000
        states = self._book_mgr.get_all_active_states()

        # Group states by window_slug
        by_window: dict[str, dict[str, object]] = {}
        for cid, state in states.items():
            slug = state.token.window_slug
            window = self._mkt_mgr.get_window(slug)
            if not window:
                continue
            # Only live windows
            if now_s < window.start_ts or now_s > window.end_ts:
                continue
            if slug not in by_window:
                by_window[slug] = {"window": window, "tokens": {}}
            outcome = state.token.outcome
            by_window[slug]["tokens"][outcome] = (cid, state)

        timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3]

        btc_price = self._btc.price

        for slug, info in by_window.items():
            window = info["window"]
            tokens = info["tokens"]

            # Get YES (Up) and NO (Down) states
            yes_cid, yes_state = tokens.get("Up", (None, None))
            no_cid, no_state = tokens.get("Down", (None, None))

            if not yes_state and not no_state:
                continue

            yes_book = yes_state.book if yes_state else None
            no_book = no_state.book if no_state else None

            # Build imbalance: bid_size / (bid_size + ask_size)
            def calc_imbalance(book):
                if not book:
                    return None
                bs = book.best_bid_size
                as_ = book.best_ask_size
                if bs is None or as_ is None:
                    return None
                total = bs + as_
                return round(bs / total, 6) if total > 0 else None

            snap = MergedSnapshot(
                ts_ms=ts_ms,
                timestamp=timestamp,
                window_slug=slug,
                window_end_ts=window.end_ts,
                outcome="",  # filled after settlement
                yes_token_id=yes_cid or "",
                no_token_id=no_cid or "",
                yes_bid=yes_book.best_bid if yes_book else None,
                yes_ask=yes_book.best_ask if yes_book else None,
                yes_ask_size=yes_book.best_ask_size if yes_book else None,
                yes_bid_size=yes_book.best_bid_size if yes_book else None,
                yes_spread=yes_book.spread if yes_book else None,
                no_bid=no_book.best_bid if no_book else None,
                no_ask=no_book.best_ask if no_book else None,
                no_ask_size=no_book.best_ask_size if no_book else None,
                no_bid_size=no_book.best_bid_size if no_book else None,
                no_spread=no_book.spread if no_book else None,
                yes_mid=yes_book.mid if yes_book else None,
                no_mid=no_book.mid if no_book else None,
                yes_imbalance=calc_imbalance(yes_book),
                no_imbalance=calc_imbalance(no_book),
                btc_price=btc_price,
            )
            await self._on_snapshot(snap)
