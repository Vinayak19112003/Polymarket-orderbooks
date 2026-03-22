#!/usr/bin/env python3
"""
Polymarket BTC Orderbook Collector — ALL market types.

Collects tick-level orderbook data from ALL active BTC binary markets:
  - Daily Up/Down     (bitcoin-up-or-down-on-march-XX)      ~$300K/day
  - Above strikes     (bitcoin-above-on-march-XX)            ~$2.5M/day
  - Price targets     (what-price-will-bitcoin-hit-on-XX)    ~$350K/day
  - 5-minute slots    (btc-updown-5m-*)                      ~$260K/day
  - 15-minute slots   (btc-updown-15m-*)
  - Weekly/monthly    (btc-multi-strikes-weekly, etc.)

Usage:
    python collector.py                # Start collecting
    python collector.py --export       # Export DB to CSV
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

MARKET_CHECK_INTERVAL = 30     # check for new markets every 30s
MAX_CONCURRENT_STREAMS = 50    # max WS connections
BATCH_SIZE = 200
BATCH_TIMEOUT = 2.0

# Slug patterns to match — only BTC 15-minute Up/Down markets
BTC_PATTERNS = [
    ("slug", "btc-updown-15m"),
]


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
# Database — generalized for multi-outcome markets
# ─────────────────────────────────────────────
def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            slug           TEXT NOT NULL,
            title          TEXT,
            event_type     TEXT NOT NULL,
            end_ts         INTEGER NOT NULL,
            discovered_at  INTEGER NOT NULL,
            UNIQUE(slug)
        );

        CREATE TABLE IF NOT EXISTS tokens (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id       INTEGER NOT NULL REFERENCES events(id),
            condition_id   TEXT NOT NULL,
            token_id       TEXT NOT NULL UNIQUE,
            outcome        TEXT NOT NULL,
            question       TEXT,
            resolved       TEXT
        );

        CREATE TABLE IF NOT EXISTS orderbook_ticks (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            token_id       INTEGER NOT NULL REFERENCES tokens(id),
            event_ts       INTEGER NOT NULL,
            received_ts    INTEGER NOT NULL,
            tick_type      TEXT NOT NULL,
            price          REAL,
            size           REAL,
            side           TEXT,
            best_bid       REAL,
            best_ask       REAL,
            full_book_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_ticks_token ON orderbook_ticks(token_id);
        CREATE INDEX IF NOT EXISTS idx_ticks_ts    ON orderbook_ticks(event_ts);
        CREATE INDEX IF NOT EXISTS idx_ticks_type  ON orderbook_ticks(tick_type);
    """)
    conn.commit()
    return conn


# ─────────────────────────────────────────────
# In-memory L2 orderbook
# ─────────────────────────────────────────────
class OrderbookState:
    def __init__(self):
        self.bids: dict[str, float] = {}
        self.asks: dict[str, float] = {}

    def apply_book(self, bids: list, asks: list):
        self.bids = {b["price"]: float(b["size"]) for b in bids}
        self.asks = {a["price"]: float(a["size"]) for a in asks}

    def apply_price_change(self, side: str, price: str, size: float):
        book = self.bids if side == "BUY" else self.asks
        if size == 0:
            book.pop(price, None)
        else:
            book[price] = size

    def best_bid(self) -> Optional[float]:
        return max(float(p) for p in self.bids) if self.bids else None

    def best_ask(self) -> Optional[float]:
        return min(float(p) for p in self.asks) if self.asks else None

    def to_json(self) -> str:
        bids = sorted([{"price": p, "size": s} for p, s in self.bids.items()],
                       key=lambda x: float(x["price"]), reverse=True)
        asks = sorted([{"price": p, "size": s} for p, s in self.asks.items()],
                       key=lambda x: float(x["price"]))
        return json.dumps({"bids": bids, "asks": asks})


# ─────────────────────────────────────────────
# Market Discovery — finds ALL BTC markets
# ─────────────────────────────────────────────
def _matches_btc(event: dict) -> bool:
    slug = event.get("slug", "").lower()
    for field, pattern in BTC_PATTERNS:
        val = event.get(field, "").lower()
        if pattern in val:
            return True
    return False


def _parse_json_field(val):
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return val
    return val


class MarketDiscovery:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def discover_btc_markets(self) -> list[dict]:
        """Return list of {slug, title, event_type, end_ts, tokens: [{token_id, outcome, question, condition_id}]}."""
        now_ts = int(time.time())
        results = []

        # Fetch active, unclosed events
        params = {
            "active": "true",
            "closed": "false",
            "limit": "200",
            "order": "id",
            "ascending": "false",
        }
        try:
            async with self.session.get(GAMMA_URL, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                resp.raise_for_status()
                events = await resp.json()
        except Exception as e:
            logger.error(f"Gamma API error: {e}")
            return []

        for event in events:
            if not _matches_btc(event):
                continue

            slug = event.get("slug", "")
            title = event.get("title", "")
            end_str = event.get("endDate", "")

            end_ts = 0
            if end_str:
                try:
                    end_ts = int(datetime.fromisoformat(end_str.replace("Z", "+00:00")).timestamp())
                except Exception:
                    pass

            # Skip markets that ended >5min ago
            if end_ts and end_ts < now_ts - 300:
                continue

            raw_markets = event.get("markets", [])
            tokens = []

            for mk in raw_markets:
                if isinstance(mk, str):
                    try:
                        mk = json.loads(mk)
                    except Exception:
                        continue
                if not isinstance(mk, dict):
                    continue

                clob_ids = _parse_json_field(mk.get("clobTokenIds", []))
                outcomes = _parse_json_field(mk.get("outcomes", []))
                if not isinstance(clob_ids, list) or not clob_ids:
                    continue
                if not isinstance(outcomes, list):
                    outcomes = []

                condition_id = mk.get("conditionId", "")
                question = mk.get("question", "")

                for i, token_id in enumerate(clob_ids):
                    outcome = outcomes[i] if i < len(outcomes) else f"outcome_{i}"
                    tokens.append({
                        "token_id": token_id,
                        "outcome": outcome,
                        "question": question,
                        "condition_id": condition_id,
                    })

            if tokens:
                # Classify event type
                event_type = "other"
                sl = slug.lower()
                if "updown-5m" in sl:
                    event_type = "5m_updown"
                elif "updown-15m" in sl:
                    event_type = "15m_updown"
                elif "up-or-down" in sl:
                    event_type = "daily_updown"
                elif "above" in sl:
                    event_type = "strike_above"
                elif "price-will" in sl or "price-on" in sl:
                    event_type = "price_target"

                results.append({
                    "slug": slug,
                    "title": title,
                    "event_type": event_type,
                    "end_ts": end_ts,
                    "tokens": tokens,
                })

        # Sort by end_ts ascending (soonest-expiring first)
        results.sort(key=lambda x: x["end_ts"])
        return results

    def upsert_event(self, conn: sqlite3.Connection, event: dict) -> int:
        """Insert event if not exists, return event_id."""
        row = conn.execute("SELECT id FROM events WHERE slug = ?", (event["slug"],)).fetchone()
        if row:
            return row[0]
        cur = conn.execute(
            "INSERT INTO events (slug, title, event_type, end_ts, discovered_at) VALUES (?,?,?,?,?)",
            (event["slug"], event["title"], event["event_type"], event["end_ts"], int(time.time()))
        )
        conn.commit()
        return cur.lastrowid

    def upsert_token(self, conn: sqlite3.Connection, event_id: int, token: dict) -> int:
        """Insert token if not exists, return token_id (our internal DB id)."""
        row = conn.execute("SELECT id FROM tokens WHERE token_id = ?", (token["token_id"],)).fetchone()
        if row:
            return row[0]
        cur = conn.execute(
            "INSERT INTO tokens (event_id, condition_id, token_id, outcome, question) VALUES (?,?,?,?,?)",
            (event_id, token["condition_id"], token["token_id"], token["outcome"], token["question"])
        )
        conn.commit()
        return cur.lastrowid

    async def resolve_tokens(self, conn: sqlite3.Connection, condition_id: str):
        """Check resolution status for tokens."""
        try:
            async with self.session.get(
                "https://gamma-api.polymarket.com/markets",
                params={"conditionIds": condition_id},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except Exception:
            return

        for market in data:
            if not market.get("closed"):
                continue
            winner_index = market.get("winnerIndex")
            outcomes = _parse_json_field(market.get("outcomes", []))
            resolved = None
            if winner_index is not None and isinstance(outcomes, list) and winner_index < len(outcomes):
                resolved = outcomes[winner_index].upper()
            if resolved:
                cond = market.get("conditionId", condition_id)
                conn.execute("UPDATE tokens SET resolved = ? WHERE condition_id = ? AND resolved IS NULL",
                             (resolved, cond))
                conn.commit()


# ─────────────────────────────────────────────
# WebSocket Stream — one per token
# ─────────────────────────────────────────────
class TokenStream:
    def __init__(self, clob_token_id: str, db_token_id: int, label: str, write_queue: asyncio.Queue):
        self.clob_token_id = clob_token_id
        self.db_token_id = db_token_id
        self.label = label
        self.write_queue = write_queue
        self.book = OrderbookState()
        self._stop = False
        self.task: Optional[asyncio.Task] = None

    def stop(self):
        self._stop = True

    async def run(self):
        delay = 1
        while not self._stop:
            try:
                await self._stream_loop()
                delay = 1
            except Exception as e:
                if self._stop:
                    return
                logger.warning(f"[{self.label}] WS error: {e}, retry in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def _stream_loop(self):
        log = logging.getLogger(f"ws.{self.label[:20]}")
        log.info(f"Connecting | token={self.clob_token_id[:16]}...")

        async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
            await ws.send(json.dumps({"assets_ids": [self.clob_token_id], "type": "market"}))
            log.info("Subscribed")

            async for raw in ws:
                if self._stop:
                    return
                received_ts = int(time.time() * 1000)
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                events = msg if isinstance(msg, list) else [msg]
                for event in events:
                    etype = event.get("event_type")
                    if not etype:
                        continue

                    event_ts_raw = event.get("timestamp")
                    event_ts = int(event_ts_raw) if event_ts_raw else received_ts

                    if etype == "book":
                        self.book.apply_book(event.get("bids", []), event.get("asks", []))
                        await self.write_queue.put({
                            "token_id": self.db_token_id,
                            "event_ts": event_ts, "received_ts": received_ts,
                            "tick_type": "book",
                            "price": None, "size": None, "side": None,
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
                                "token_id": self.db_token_id,
                                "event_ts": event_ts, "received_ts": received_ts,
                                "tick_type": "price_change",
                                "price": float(price_str), "size": size, "side": side,
                                "best_bid": self.book.best_bid(),
                                "best_ask": self.book.best_ask(),
                                "full_book_json": None,
                            })

                    elif etype == "last_trade_price":
                        await self.write_queue.put({
                            "token_id": self.db_token_id,
                            "event_ts": event_ts, "received_ts": received_ts,
                            "tick_type": "trade",
                            "price": float(event.get("price", 0)),
                            "size": float(event.get("size", 0)),
                            "side": None,
                            "best_bid": self.book.best_bid(),
                            "best_ask": self.book.best_ask(),
                            "full_book_json": None,
                        })


# ─────────────────────────────────────────────
# DB Writer (batched)
# ─────────────────────────────────────────────
async def db_writer_task(queue: asyncio.Queue, conn: sqlite3.Connection):
    log = logging.getLogger("db_writer")
    SQL = """INSERT INTO orderbook_ticks
             (token_id, event_ts, received_ts, tick_type, price, size, side, best_bid, best_ask, full_book_json)
             VALUES (?,?,?,?,?,?,?,?,?,?)"""
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
                    row["token_id"], row["event_ts"], row["received_ts"],
                    row["tick_type"], row["price"], row["size"], row["side"],
                    row["best_bid"], row["best_ask"], row["full_book_json"],
                ))
                queue.task_done()
            except asyncio.TimeoutError:
                break

        if batch:
            conn.executemany(SQL, batch)
            conn.commit()
            total += len(batch)
            if total % 1000 < len(batch):
                log.info(f"Total ticks written: {total:,}")


# ─────────────────────────────────────────────
# Stream Manager — dynamic, multi-market
# ─────────────────────────────────────────────
async def stream_manager_task(
    discovery: MarketDiscovery,
    conn: sqlite3.Connection,
    write_queue: asyncio.Queue,
):
    log = logging.getLogger("stream_mgr")
    active_streams: dict[str, TokenStream] = {}  # clob_token_id → stream

    while True:
        try:
            markets = await discovery.discover_btc_markets()
            log.info(f"Discovered {len(markets)} BTC events with {sum(len(m['tokens']) for m in markets)} tokens")

            seen_token_ids = set()

            for market in markets:
                event_id = discovery.upsert_event(conn, market)

                for token in market["tokens"]:
                    clob_id = token["token_id"]
                    seen_token_ids.add(clob_id)

                    if clob_id in active_streams:
                        continue  # already streaming

                    if len(active_streams) >= MAX_CONCURRENT_STREAMS:
                        log.warning(f"Hit max streams ({MAX_CONCURRENT_STREAMS}), skipping new tokens")
                        break

                    db_token_id = discovery.upsert_token(conn, event_id, token)
                    label = f"{market['event_type']}:{token['outcome'][:10]}"
                    stream = TokenStream(clob_id, db_token_id, label, write_queue)
                    stream.task = asyncio.create_task(stream.run(), name=f"ws_{clob_id[:12]}")
                    active_streams[clob_id] = stream
                    log.info(f"Started stream: {label} | {market['slug'][:40]}")

            # Stop streams for markets no longer active
            to_remove = [cid for cid in active_streams if cid not in seen_token_ids]
            for cid in to_remove:
                stream = active_streams.pop(cid)
                stream.stop()
                if stream.task:
                    stream.task.cancel()
                log.info(f"Stopped stream: {stream.label}")

            # Resolve ended tokens
            ended_events = conn.execute(
                "SELECT DISTINCT condition_id FROM tokens WHERE resolved IS NULL"
            ).fetchall()
            for (cid,) in ended_events:
                await discovery.resolve_tokens(conn, cid)

        except Exception as e:
            log.error(f"Stream manager error: {e}", exc_info=True)

        await asyncio.sleep(MARKET_CHECK_INTERVAL)


# ─────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────
def export_to_csv(db_path: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)

    # Events
    path = os.path.join(output_dir, "events.csv")
    rows = conn.execute("SELECT * FROM events ORDER BY end_ts").fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM events LIMIT 0").description]
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(cols); w.writerows(rows)
    print(f"events.csv         → {len(rows)} rows")

    # Tokens
    path = os.path.join(output_dir, "tokens.csv")
    rows = conn.execute("SELECT * FROM tokens ORDER BY event_id, outcome").fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM tokens LIMIT 0").description]
    with open(path, "w", newline="") as f:
        w = csv.writer(f); w.writerow(cols); w.writerows(rows)
    print(f"tokens.csv         → {len(rows)} rows")

    # Ticks (joined)
    path = os.path.join(output_dir, "ticks.csv")
    sql = """
        SELECT
            t.event_ts, t.received_ts, t.tick_type,
            t.price, t.size, t.side,
            t.best_bid, t.best_ask,
            tk.outcome, tk.question,
            e.slug, e.event_type, e.end_ts,
            tk.resolved
        FROM orderbook_ticks t
        JOIN tokens tk ON t.token_id = tk.id
        JOIN events e ON tk.event_id = e.id
        ORDER BY t.event_ts
    """
    rows = conn.execute(sql).fetchall()
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "event_ts_ms", "received_ts_ms", "tick_type",
            "price", "size", "side",
            "best_bid", "best_ask",
            "outcome", "question",
            "event_slug", "event_type", "event_end_ts",
            "resolved"
        ])
        w.writerows(rows)
    print(f"ticks.csv          → {len(rows):,} rows")

    conn.close()


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def run_collector():
    setup_logging()
    logger.info("Starting Polymarket BTC orderbook collector (all market types)")

    conn = init_db(DB_PATH)
    write_queue: asyncio.Queue = asyncio.Queue(maxsize=50000)

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
            asyncio.create_task(db_writer_task(write_queue, conn), name="db_writer"),
            asyncio.create_task(stream_manager_task(discovery, conn, write_queue), name="stream_mgr"),
        ]

        await shutdown.wait()

        logger.info("Stopping...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    conn.close()
    logger.info("Collector stopped cleanly")


def main():
    parser = argparse.ArgumentParser(description="Polymarket BTC orderbook collector")
    parser.add_argument("--export", action="store_true", help="Export DB to CSV and exit")
    parser.add_argument("--output", default="./backtest_data", help="Output directory for CSV export")
    parser.add_argument("--db", default=DB_PATH, help="SQLite DB path")
    args = parser.parse_args()

    if args.export:
        setup_logging()
        export_to_csv(args.db, args.output)
        return

    asyncio.run(run_collector())


if __name__ == "__main__":
    main()
