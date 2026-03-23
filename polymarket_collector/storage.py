"""CSV + Parquet storage with daily file rotation."""

import os
import csv
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .config import CollectorConfig
from .models import MergedSnapshot, RawTick

logger = logging.getLogger("collector.storage")

CSV_COLUMNS = [
    "ts_ms", "timestamp", "window_slug", "window_end_ts", "outcome",
    "yes_token_id", "no_token_id",
    "yes_bid", "yes_ask", "yes_ask_size", "yes_bid_size", "yes_spread",
    "no_bid", "no_ask", "no_ask_size", "no_bid_size", "no_spread",
    "yes_mid", "no_mid", "yes_imbalance", "no_imbalance", "btc_price",
]

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


class CsvWriter:
    """Appends merged snapshot rows to daily CSV files."""

    def __init__(self, base_dir: Path):
        self._base_dir = base_dir
        self._base_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: str = ""
        self._file = None
        self._writer = None
        self._row_count = 0

    def _rotate(self, date_str: str):
        if self._file:
            self._file.close()
        path = self._base_dir / f"orderbook_{date_str}.csv"
        exists = path.exists()
        self._file = open(path, "a", newline="")
        self._writer = csv.DictWriter(self._file, fieldnames=CSV_COLUMNS)
        if not exists:
            self._writer.writeheader()
        self._current_date = date_str
        logger.info(f"CSV writer opened: {path.name}")

    def append(self, snap: MergedSnapshot):
        date_str = datetime.fromtimestamp(
            snap.ts_ms / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d")

        if date_str != self._current_date:
            self._rotate(date_str)

        row = {}
        for col in CSV_COLUMNS:
            val = getattr(snap, col, None)
            if val is None:
                row[col] = ""
            else:
                row[col] = val
        self._writer.writerow(row)
        self._row_count += 1

        # Flush every 100 rows
        if self._row_count % 100 == 0:
            self._file.flush()

    def close(self):
        if self._file:
            self._file.flush()
            self._file.close()
            self._file = None

    @property
    def pending_count(self) -> int:
        return 0  # CSV writes immediately


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
            self._rows = rows + self._rows

    @property
    def pending_count(self) -> int:
        return len(self._rows)


class Storage:
    """Facade over CSV snapshot writer + tick parquet buffer."""

    def __init__(self, config: CollectorConfig):
        self._config = config
        data_dir = config.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)

        self._csv_writer = CsvWriter(data_dir / "orderbook")
        self._tick_buf = ParquetBuffer(
            data_dir / "ticks", "tick", TICK_SCHEMA, config
        )

    async def write_snapshot(self, snap: MergedSnapshot):
        self._csv_writer.append(snap)

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
        self._csv_writer.close()
        self._tick_buf.flush()

    @property
    def stats(self) -> dict:
        return {
            "snapshots_pending": self._csv_writer.pending_count,
            "ticks_pending": self._tick_buf.pending_count,
        }
