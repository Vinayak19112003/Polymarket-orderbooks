#!/usr/bin/env python3
"""
Polymarket BTC 15-Minute Up/Down — Full Orderbook Tick Collector
Captures every single bid/ask change via WebSocket for backtesting.

Usage:
    python collector.py               # Start collecting
    python collector.py --export      # Export DB to CSV files
    python collector.py --export --output ./data/
"""

import asyncio
import aiohttp
import websockets
import sqlite3
import json
import logging
import signal
import time
import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
DB_PATH = "orderbooks.db"
LOG_PATH = "collector.log"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_URL = "https://gamma-api.polymarket.com/events"
CLOB_URL = "https://clob.polymarket.com"

# How often (seconds) to check if a new 15m market started
MARKET_CHECK_INTERVAL = 60

# DB write batch: commit after this many rows or this many seconds
BATCH_SIZE = 100
BATCH_TIMEOUT = 2.0


# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
def setup_logging():
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=[
        TimedRotatingFileHandler(LOG_PATH, when="midnight", backupCount=30),
        logging.StreamHandler(sys.stdout),
    ])

logger = logging.getLogger("collector")


# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────
def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")   # allows concurrent reads during writes
    conn.execute("PRAGMA synchronous=NORMAL") # faster writes, safe with WAL
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS markets (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            market_start_ts INTEGER NOT NULL,
            market_end_ts   INTEGER NOT NULL,
            condition_id    TEXT NOT NULL UNIQUE,
            up_token_id     TEXT NOT NULL,
            down_token_id   TEXT NOT NULL,
            resolved        TEXT,
            discovered_at   INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orderbook_events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id     INTEGER NOT NULL REFERENCES markets(id),
            event_ts      INTEGER NOT NULL,
            received_ts   INTEGER NOT NULL,
            token_side    TEXT NOT NULL,
            event_type    TEXT NOT NULL,
            price         REAL,
            size          REAL,
            side          TEXT,
            best_bid      REAL,
            best_ask      REAL,
            full_book_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_events_market ON orderbook_events(market_id);
        CREATE INDEX IF NOT EXISTS idx_events_ts     ON orderbook_events(event_ts);
        CREATE INDEX IF NOT EXISTS idx_events_side   ON orderbook_events(token_side);
    """)
    conn.commit()
    return conn


# ─────────────────────────────────────────────
# In-memory L2 orderbook state
# ─────────────────────────────────────────────
class OrderbookState:
    def __init__(self):
        self.bids: dict[str, float] = {}   # price_str → size
        self.asks: dict[str, float] = {}

    def apply_book(self, bids: list, asks: list):
        """Full snapshot — replace entire book."""
        self.bids = {b["price"]: float(b["size"]) for b in bids}
        self.asks = {a["price"]: float(a["size"]) for a in asks}

    def apply_price_change(self, side: str, price: str, size: float):
        """Delta update — update or remove one price level."""
        book = self.bids if side == "BUY" else self.asks
        if size == 0:
            book.pop(price, None)
        else:
            book[price] = size

    def best_bid(self) -> Optional[float]:
        if not self.bids:
            return None
        return max(float(p) for p in self.bids)

    def best_ask(self) -> Optional[float]:
        if not self.asks:
            return None
        return min(float(p) for p in self.asks)

    def to_json(self) -> str:
        bids = sorted(
            [{"price": p, "size": s} for p, s in self.bids.items()],
            key=lambda x: float(x["price"]), reverse=True
        )
        asks = sorted(
            [{"price": p, "size": s} for p, s in self.asks.items()],
            key=lambda x: float(x["price"])
        )
        return json.dumps({"bids": bids, "asks": asks})


# ─────────────────────────────────────────────
# Market Discovery
# ─────────────────────────────────────────────
class MarketDiscovery:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def get_current_btc_15m_market(self) -> Optional[dict]:
        """
        Query Gamma API for the active BTC 15-minute Up/Down market.
        Returns dict with condition_id, up_token_id, down_token_id, start_ts, end_ts.
        """
        params = {
            "active": "true",
            "closed": "false",
            "limit": "50",
            "order": "id",
            "ascending": "false",
        }
        try:
            async with self.session.get(GAMMA_URL, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                resp.raise_for_status()
                events = await resp.json()
        except Exception as e:
            logger.error(f"Gamma API error: {e}")
            return None

        now_ts = int(time.time())

        for event in events:
            slug = event.get("slug", "")
            # Match pattern: btc-updown-15m-{unix_timestamp}
            if not ("btc" in slug.lower() and "updown" in slug.lower() and "15m" in slug.lower()):
                continue

            markets = event.get("markets", [])
            if not markets:
                continue

            up_token_id = None
            down_token_id = None
            start_ts = None
            end_ts = None
            condition_id = None

            for market in markets:
                question = market.get("question", "").lower()
                clob_ids = market.get("clobTokenIds", [])
                if not clob_ids:
                    continue

                condition_id = market.get("conditionId", "")
                end_time_str = market.get("endDate") or market.get("endDateIso")
                start_time_str = market.get("startDate") or market.get("startDateIso")

                # Parse timestamps
                if end_time_str:
                    try:
                        end_ts = int(datetime.fromisoformat(
                            end_time_str.replace("Z", "+00:00")
                        ).timestamp())
                    except Exception:
                        pass

                if start_time_str:
                    try:
                        start_ts = int(datetime.fromisoformat(
                            start_time_str.replace("Z", "+00:00")
                        ).timestamp())
                    except Exception:
                        pass

                # Determine Up/Down from outcomes
                outcomes = market.get("outcomes", [])
                for i, outcome in enumerate(outcomes):
                    if isinstance(outcome, str):
                        if outcome.lower() == "up" and i < len(clob_ids):
                            up_token_id = clob_ids[i]
                        elif outcome.lower() == "down" and i < len(clob_ids):
                            down_token_id = clob_ids[i]

                # Fallback: first token=Up, second token=Down
                if not up_token_id and len(clob_ids) >= 1:
                    up_token_id = clob_ids[0]
                if not down_token_id and len(clob_ids) >= 2:
                    down_token_id = clob_ids[1]

            if up_token_id and down_token_id and end_ts:
                # Skip if market already ended
                if end_ts < now_ts - 60:
                    continue
                if start_ts is None:
                    start_ts = end_ts - 15 * 60

                logger.info(
                    f"Discovered market: {slug} | end={datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat()}"
                )
                return {
                    "condition_id": condition_id,
                    "up_token_id": up_token_id,
                    "down_token_id": down_token_id,
                    "market_start_ts": start_ts,
                    "market_end_ts": end_ts,
                }

        logger.warning("No active BTC 15m market found in Gamma API response")
        return None

    def upsert_market(self, conn: sqlite3.Connection, market: dict) -> int:
        """Insert market if not exists. Return market id."""
        row = conn.execute(
            "SELECT id FROM markets WHERE condition_id = ?",
            (market["condition_id"],)
        ).fetchone()
        if row:
            return row[0]
        cur = conn.execute(
            """INSERT INTO markets
               (market_start_ts, market_end_ts, condition_id, up_token_id, down_token_id, discovered_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                market["market_start_ts"],
                market["market_end_ts"],
                market["condition_id"],
                market["up_token_id"],
                market["down_token_id"],
                int(time.time()),
            ),
        )
        conn.commit()
        logger.info(f"Inserted new market id={cur.lastrowid} condition={market['condition_id'][:16]}...")
        return cur.lastrowid

    async def resolve_market(self, conn: sqlite3.Connection, market_id: int, condition_id: str):
        """Check if a past market resolved and update DB."""
        params = {"conditionIds": condition_id}
        try:
            async with self.session.get(
                "https://gamma-api.polymarket.com/markets",
                params=params,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except Exception as e:
            logger.error(f"Resolve check error: {e}")
            return

        for market in data:
            if not market.get("closed"):
                continue
            winner_index = market.get("winnerIndex")
            outcomes = market.get("outcomes", [])
            resolved = None
            if winner_index is not None and winner_index < len(outcomes):
                resolved = outcomes[winner_index].upper()
            elif market.get("resolution"):
                resolved = str(market["resolution"]).upper()

            if resolved:
                conn.execute(
                    "UPDATE markets SET resolved = ? WHERE id = ?",
                    (resolved, market_id)
                )
                conn.commit()
                logger.info(f"Market id={market_id} resolved: {resolved}")
                return


# ─────────────────────────────────────────────
# WebSocket Stream (one per token)
# ─────────────────────────────────────────────
class TokenStream:
    def __init__(self, token_side: str, write_queue: asyncio.Queue):
        self.token_side = token_side
        self.write_queue = write_queue
        self.token_id: Optional[str] = None
        self.market_id: Optional[int] = None
        self.book = OrderbookState()
        self._stop = False
        self._resubscribe = asyncio.Event()
        self.task: Optional[asyncio.Task] = None

    def set_market(self, token_id: str, market_id: int):
        """Called by market manager when a new market starts."""
        self.token_id = token_id
        self.market_id = market_id
        self.book = OrderbookState()
        self._resubscribe.set()

    def stop(self):
        self._stop = True
        self._resubscribe.set()

    async def run(self):
        delay = 1
        while not self._stop:
            if not self.token_id:
                await asyncio.sleep(1)
                continue
            try:
                await self._stream_loop()
                delay = 1  # reset on clean exit
            except Exception as e:
                logger.warning(f"[{self.token_side}] WS error: {e}, retry in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def _stream_loop(self):
        self._resubscribe.clear()
        token_id = self.token_id
        market_id = self.market_id
        log = logging.getLogger(f"stream.{self.token_side}")

        log.info(f"Connecting | token={token_id[:16]}...")
        async with websockets.connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        ) as ws:
            # Subscribe
            await ws.send(json.dumps({
                "assets_ids": [token_id],
                "type": "market"
            }))
            log.info("Subscribed")

            recv_task = asyncio.ensure_future(self._recv_loop(ws, token_id, market_id, log))
            resub_task = asyncio.ensure_future(self._resubscribe.wait())

            done, pending = await asyncio.wait(
                [recv_task, resub_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()

            if self._resubscribe.is_set() and not self._stop:
                log.info("Resubscribing to new market token")

    async def _recv_loop(self, ws, token_id: str, market_id: int, log):
        async for raw in ws:
            received_ts = int(time.time() * 1000)
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # Handle both single dict and list of events
            events = msg if isinstance(msg, list) else [msg]

            for event in events:
                etype = event.get("event_type")
                if not etype:
                    continue

                event_ts_raw = event.get("timestamp")
                event_ts = int(event_ts_raw) if event_ts_raw else received_ts

                if etype == "book":
                    bids = event.get("bids", [])
                    asks = event.get("asks", [])
                    self.book.apply_book(bids, asks)
                    await self.write_queue.put({
                        "market_id": market_id,
                        "event_ts": event_ts,
                        "received_ts": received_ts,
                        "token_side": self.token_side,
                        "event_type": "book",
                        "price": None,
                        "size": None,
                        "side": None,
                        "best_bid": self.book.best_bid(),
                        "best_ask": self.book.best_ask(),
                        "full_book_json": self.book.to_json(),
                    })

                elif etype == "price_change":
                    for change in event.get("changes", []):
                        side = change.get("side", "")
                        price_str = change.get("price", "0")
                        size = float(change.get("size", 0))
                        self.book.apply_price_change(side, price_str, size)
                        await self.write_queue.put({
                            "market_id": market_id,
                            "event_ts": event_ts,
                            "received_ts": received_ts,
                            "token_side": self.token_side,
                            "event_type": "price_change",
                            "price": float(price_str),
                            "size": size,
                            "side": side,
                            "best_bid": self.book.best_bid(),
                            "best_ask": self.book.best_ask(),
                            "full_book_json": None,  # save space — reconstruct from deltas
                        })

                elif etype == "last_trade_price":
                    await self.write_queue.put({
                        "market_id": market_id,
                        "event_ts": event_ts,
                        "received_ts": received_ts,
                        "token_side": self.token_side,
                        "event_type": "trade",
                        "price": float(event.get("price", 0)),
                        "size": float(event.get("size", 0)),
                        "side": None,
                        "best_bid": self.book.best_bid(),
                        "best_ask": self.book.best_ask(),
                        "full_book_json": None,
                    })


# ─────────────────────────────────────────────
# DB Writer (batched inserts)
# ─────────────────────────────────────────────
async def db_writer_task(queue: asyncio.Queue, conn: sqlite3.Connection):
    log = logging.getLogger("db_writer")
    INSERT_SQL = """
        INSERT INTO orderbook_events
        (market_id, event_ts, received_ts, token_side, event_type,
         price, size, side, best_bid, best_ask, full_book_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    total = 0
    while True:
        batch = []
        deadline = time.monotonic() + BATCH_TIMEOUT
        while len(batch) < BATCH_SIZE:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                row = await asyncio.wait_for(queue.get(), timeout=remaining)
                batch.append((
                    row["market_id"], row["event_ts"], row["received_ts"],
                    row["token_side"], row["event_type"],
                    row["price"], row["size"], row["side"],
                    row["best_bid"], row["best_ask"], row["full_book_json"],
                ))
                queue.task_done()
            except asyncio.TimeoutError:
                break

        if batch:
            conn.executemany(INSERT_SQL, batch)
            conn.commit()
            total += len(batch)
            log.debug(f"Wrote {len(batch)} rows (total={total})")


# ─────────────────────────────────────────────
# Market Manager
# ─────────────────────────────────────────────
async def market_manager_task(
    discovery: MarketDiscovery,
    conn: sqlite3.Connection,
    stream_up: TokenStream,
    stream_down: TokenStream,
):
    log = logging.getLogger("market_mgr")
    current_market_id: Optional[int] = None
    current_end_ts: Optional[int] = None
    prev_market_id: Optional[int] = None
    prev_condition_id: Optional[str] = None

    while True:
        market = await discovery.get_current_btc_15m_market()

        if market:
            market_id = discovery.upsert_market(conn, market)

            if market_id != current_market_id:
                log.info(f"New market detected: id={market_id}")

                # Try to resolve previous market
                if prev_market_id and prev_condition_id:
                    asyncio.ensure_future(
                        discovery.resolve_market(conn, prev_market_id, prev_condition_id)
                    )

                prev_market_id = current_market_id
                prev_condition_id = (
                    conn.execute("SELECT condition_id FROM markets WHERE id=?", (current_market_id,)).fetchone() or [None]
                )[0] if current_market_id else None

                current_market_id = market_id
                current_end_ts = market["market_end_ts"]

                # Signal streams to resubscribe
                stream_up.set_market(market["up_token_id"], market_id)
                stream_down.set_market(market["down_token_id"], market_id)

        await asyncio.sleep(MARKET_CHECK_INTERVAL)


# ─────────────────────────────────────────────
# Export to CSV
# ─────────────────────────────────────────────
def export_to_csv(db_path: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)

    # Export markets
    markets_path = os.path.join(output_dir, "markets.csv")
    rows = conn.execute("SELECT * FROM markets ORDER BY market_start_ts").fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM markets LIMIT 0").description]
    with open(markets_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"Exported {len(rows)} markets → {markets_path}")

    # Export events grouped by month
    months = conn.execute(
        """SELECT DISTINCT strftime('%Y-%m', datetime(event_ts/1000, 'unixepoch')) as ym
           FROM orderbook_events ORDER BY ym"""
    ).fetchall()

    for (ym,) in months:
        year, month = ym.split("-")
        start = int(datetime(int(year), int(month), 1, tzinfo=timezone.utc).timestamp() * 1000)
        import calendar
        last_day = calendar.monthrange(int(year), int(month))[1]
        end = int(datetime(int(year), int(month), last_day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

        sql = """
            SELECT
                e.event_ts, e.received_ts,
                m.market_start_ts, m.market_end_ts,
                e.token_side, e.event_type,
                e.price, e.size, e.side,
                e.best_bid, e.best_ask,
                m.resolved,
                e.full_book_json
            FROM orderbook_events e
            JOIN markets m ON e.market_id = m.id
            WHERE e.event_ts BETWEEN ? AND ?
            ORDER BY e.event_ts
        """
        rows = conn.execute(sql, (start, end)).fetchall()
        out_path = os.path.join(output_dir, f"orderbook_events_{ym}.csv")
        with open(out_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "event_ts_ms", "received_ts_ms",
                "market_start_ts", "market_end_ts",
                "token_side", "event_type",
                "price", "size", "side",
                "best_bid", "best_ask",
                "resolved", "full_book_json"
            ])
            w.writerows(rows)
        print(f"Exported {len(rows)} events → {out_path}")

    conn.close()


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def run_collector():
    setup_logging()
    logger.info("Starting Polymarket BTC 15m orderbook collector")

    conn = init_db(DB_PATH)
    write_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)

    stream_up = TokenStream("UP", write_queue)
    stream_down = TokenStream("DOWN", write_queue)

    shutdown = asyncio.Event()

    def _handle_signal():
        logger.info("Shutdown signal received")
        shutdown.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    async with aiohttp.ClientSession() as session:
        discovery = MarketDiscovery(session)

        tasks = [
            asyncio.create_task(stream_up.run(), name="stream_UP"),
            asyncio.create_task(stream_down.run(), name="stream_DOWN"),
            asyncio.create_task(db_writer_task(write_queue, conn), name="db_writer"),
            asyncio.create_task(
                market_manager_task(discovery, conn, stream_up, stream_down),
                name="market_mgr"
            ),
        ]

        await shutdown.wait()

        logger.info("Stopping streams...")
        stream_up.stop()
        stream_down.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Flush remaining queue
        while not write_queue.empty():
            await asyncio.sleep(0.1)

    conn.close()
    logger.info("Collector stopped cleanly")


def main():
    parser = argparse.ArgumentParser(description="Polymarket BTC 15m orderbook collector")
    parser.add_argument("--export", action="store_true", help="Export DB to CSV and exit")
    parser.add_argument("--output", default="./data", help="Output directory for CSV export")
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    args = parser.parse_args()

    if args.export:
        setup_logging()
        export_to_csv(args.db, args.output)
        return

    asyncio.run(run_collector())


if __name__ == "__main__":
    main()
