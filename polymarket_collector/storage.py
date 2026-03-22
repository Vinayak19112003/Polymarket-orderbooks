"""Parquet-based buffered storage with date partitioning."""

import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .config import CollectorConfig
from .models import Snapshot, RawTick

logger = logging.getLogger("collector.storage")

# --- Schemas ---

SNAPSHOT_SCHEMA = pa.schema([
    ("ts_ms", pa.int64()),
    ("window_slug", pa.string()),
    ("window_end_ts", pa.int64()),
    ("outcome", pa.string()),
    ("clob_token_id", pa.string()),
    ("best_bid", pa.float64()),
    ("best_ask", pa.float64()),
    ("mid", pa.float64()),
    ("spread", pa.float64()),
    ("bid_prices", pa.list_(pa.float64())),
    ("bid_sizes", pa.list_(pa.float64())),
    ("ask_prices", pa.list_(pa.float64())),
    ("ask_sizes", pa.list_(pa.float64())),
    ("total_bid_size", pa.float64()),
    ("total_ask_size", pa.float64()),
    ("bid_levels", pa.int32()),
    ("ask_levels", pa.int32()),
    ("imbalance_5", pa.float64()),
    ("n_price_changes", pa.int32()),
    ("n_trades", pa.int32()),
    ("last_trade_price", pa.float64()),
    ("last_trade_size", pa.float64()),
])

TICK_SCHEMA = pa.schema([
    ("ts_ms", pa.int64()),
    ("received_ts_ms", pa.int64()),
    ("window_slug", pa.string()),
    ("outcome", pa.string()),
    ("tick_type", pa.string()),
    ("price", pa.float64()),
    ("size", pa.float64()),
    ("side", pa.string()),
    ("best_bid", pa.float64()),
    ("best_ask", pa.float64()),
])


class ParquetBuffer:
    """Buffers rows in memory, flushes to date-partitioned parquet files."""

    def __init__(self, base_dir: Path, name: str, schema: pa.Schema, config: CollectorConfig):
        self._base_dir = base_dir
        self._name = name
        self._schema = schema
        self._config = config
        self._rows: list[dict] = []
        self._last_flush_ts: float = time.time()

    def append(self, row: dict):
        self._rows.append(row)

    def should_flush(self) -> bool:
        if not self._rows:
            return False
        if len(self._rows) >= self._config.parquet_row_group_size:
            return True
        if time.time() - self._last_flush_ts >= self._config.parquet_flush_interval_s:
            return True
        return False

    def flush(self):
        if not self._rows:
            return
        rows = self._rows
        self._rows = []
        self._last_flush_ts = time.time()

        # Partition by date
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        out_dir = self._base_dir / f"date={date_str}"
        out_dir.mkdir(parents=True, exist_ok=True)

        ts_tag = int(time.time() * 1000)
        path = out_dir / f"{self._name}_{ts_tag}.parquet"

        try:
            table = pa.Table.from_pylist(rows, schema=self._schema)
            pq.write_table(table, path, compression="snappy")
            logger.info(f"Flushed {len(rows)} {self._name} rows → {path.name}")
        except Exception as e:
            logger.error(f"Parquet write error ({self._name}): {e}", exc_info=True)
            # Put rows back to avoid data loss
            self._rows = rows + self._rows

    @property
    def pending_count(self) -> int:
        return len(self._rows)


class Storage:
    """Facade over snapshot + tick parquet buffers."""

    def __init__(self, config: CollectorConfig):
        self._config = config
        data_dir = config.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)

        self._snap_buf = ParquetBuffer(
            data_dir / "snapshots", "snap", SNAPSHOT_SCHEMA, config
        )
        self._tick_buf = ParquetBuffer(
            data_dir / "ticks", "tick", TICK_SCHEMA, config
        )

    async def write_snapshot(self, snap: Snapshot):
        self._snap_buf.append({
            "ts_ms": snap.ts_ms,
            "window_slug": snap.window_slug,
            "window_end_ts": snap.window_end_ts,
            "outcome": snap.outcome,
            "clob_token_id": snap.clob_token_id,
            "best_bid": snap.best_bid,
            "best_ask": snap.best_ask,
            "mid": snap.mid,
            "spread": snap.spread,
            "bid_prices": snap.bid_prices,
            "bid_sizes": snap.bid_sizes,
            "ask_prices": snap.ask_prices,
            "ask_sizes": snap.ask_sizes,
            "total_bid_size": snap.total_bid_size,
            "total_ask_size": snap.total_ask_size,
            "bid_levels": snap.bid_levels,
            "ask_levels": snap.ask_levels,
            "imbalance_5": snap.imbalance_5,
            "n_price_changes": snap.n_price_changes,
            "n_trades": snap.n_trades,
            "last_trade_price": snap.last_trade_price,
            "last_trade_size": snap.last_trade_size,
        })
        if self._snap_buf.should_flush():
            self._snap_buf.flush()

    async def write_tick(self, tick: RawTick):
        self._tick_buf.append({
            "ts_ms": tick.ts_ms,
            "received_ts_ms": tick.received_ts_ms,
            "window_slug": tick.window_slug,
            "outcome": tick.outcome,
            "tick_type": tick.tick_type,
            "price": tick.price,
            "size": tick.size,
            "side": tick.side,
            "best_bid": tick.best_bid,
            "best_ask": tick.best_ask,
        })
        if self._tick_buf.should_flush():
            self._tick_buf.flush()

    def flush_all(self):
        self._snap_buf.flush()
        self._tick_buf.flush()

    @property
    def stats(self) -> dict:
        return {
            "snapshots_pending": self._snap_buf.pending_count,
            "ticks_pending": self._tick_buf.pending_count,
        }
