#!/usr/bin/env python3
"""
Export Polymarket BTC 15m orderbook data to clean backtest CSVs.

Output:
  backtest_data/
    windows.csv         — one row per 15m window with resolution
    ticks.csv           — every orderbook tick with window label
    books.csv           — full L2 snapshots (flattened top N levels)

Usage:
    python export_backtest.py
    python export_backtest.py --db orderbooks.db --output ./backtest_data --depth 10
"""

import sqlite3, json, csv, os, argparse
from datetime import datetime, timezone


def iso(ts_ms):
    if not ts_ms: return ""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def iso_sec(ts):
    if not ts: return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(db_path, output_dir, depth):
    os.makedirs(output_dir, exist_ok=True)
    conn = sqlite3.connect(db_path)

    # ── windows.csv ──
    rows = conn.execute("""
        SELECT
            e.id, e.slug, e.event_type, e.end_ts,
            (SELECT COUNT(*) FROM orderbook_ticks ot JOIN tokens tk ON ot.token_id=tk.id WHERE tk.event_id=e.id) as tick_count,
            (SELECT COUNT(*) FROM orderbook_ticks ot JOIN tokens tk ON ot.token_id=tk.id WHERE tk.event_id=e.id AND ot.tick_type='trade') as trade_count,
            (SELECT COUNT(*) FROM orderbook_ticks ot JOIN tokens tk ON ot.token_id=tk.id WHERE tk.event_id=e.id AND ot.tick_type='price_change') as change_count
        FROM events e ORDER BY e.end_ts
    """).fetchall()

    p = os.path.join(output_dir, "windows.csv")
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["window_id", "slug", "type", "window_end_utc", "window_end_ts", "ticks", "trades", "price_changes"])
        for r in rows:
            w.writerow([r[0], r[1], r[2], iso_sec(r[3]), r[3], r[4], r[5], r[6]])
    print(f"  windows.csv     → {len(rows)} windows")

    # ── ticks.csv ──
    rows = conn.execute("""
        SELECT
            ot.event_ts, ot.received_ts, ot.tick_type,
            ot.price, ot.size, ot.side,
            ot.best_bid, ot.best_ask,
            tk.outcome, tk.question,
            e.slug, e.end_ts,
            tk.resolved
        FROM orderbook_ticks ot
        JOIN tokens tk ON ot.token_id = tk.id
        JOIN events e ON tk.event_id = e.id
        ORDER BY ot.event_ts, tk.outcome
    """).fetchall()

    p = os.path.join(output_dir, "ticks.csv")
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "timestamp", "received", "type",
            "price", "size", "side",
            "best_bid", "best_ask", "mid", "spread",
            "outcome", "question",
            "window", "window_end_utc",
            "resolved",
        ])
        for r in rows:
            bid, ask = r[6], r[7]
            mid = round((bid + ask) / 2, 6) if bid and ask else ""
            spread = round(ask - bid, 6) if bid and ask else ""
            w.writerow([
                iso(r[0]), iso(r[1]), r[2],
                r[3] if r[3] is not None else "", r[4] if r[4] is not None else "", r[5] or "",
                bid or "", ask or "", mid, spread,
                r[8], r[9],
                r[10], iso_sec(r[11]),
                r[12] or "",
            ])
    print(f"  ticks.csv       → {len(rows):,} ticks")

    # ── books.csv ──
    book_rows = conn.execute("""
        SELECT
            ot.event_ts, ot.received_ts,
            ot.best_bid, ot.best_ask,
            ot.full_book_json,
            tk.outcome,
            e.slug, e.end_ts,
            tk.resolved
        FROM orderbook_ticks ot
        JOIN tokens tk ON ot.token_id = tk.id
        JOIN events e ON tk.event_id = e.id
        WHERE ot.tick_type = 'book' AND ot.full_book_json IS NOT NULL
        ORDER BY ot.event_ts
    """).fetchall()

    header = ["timestamp", "outcome", "window", "window_end_utc", "best_bid", "best_ask"]
    for i in range(1, depth + 1):
        header += [f"bid_p{i}", f"bid_s{i}"]
    for i in range(1, depth + 1):
        header += [f"ask_p{i}", f"ask_s{i}"]
    header += ["total_bid_size", "total_ask_size", "bid_levels", "ask_levels", "resolved"]

    p = os.path.join(output_dir, "books.csv")
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in book_rows:
            book = json.loads(r[4])
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            row = [iso(r[0]), r[5], r[6], iso_sec(r[7]), r[2], r[3]]
            for i in range(depth):
                if i < len(bids):
                    row += [bids[i]["price"], bids[i]["size"]]
                else:
                    row += ["", ""]
            for i in range(depth):
                if i < len(asks):
                    row += [asks[i]["price"], asks[i]["size"]]
                else:
                    row += ["", ""]
            row += [
                round(sum(float(b["size"]) for b in bids), 2),
                round(sum(float(a["size"]) for a in asks), 2),
                len(bids), len(asks),
                r[8] or "",
            ]
            w.writerow(row)
    print(f"  books.csv       → {len(book_rows)} snapshots (top {depth} levels)")

    # Summary
    total_ticks = conn.execute("SELECT COUNT(*) FROM orderbook_ticks").fetchone()[0]
    total_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    total_tokens = conn.execute("SELECT COUNT(*) FROM tokens").fetchone()[0]
    print(f"\n  Summary: {total_events} windows | {total_tokens} tokens | {total_ticks:,} ticks")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="orderbooks.db")
    parser.add_argument("--output", default="./backtest_data")
    parser.add_argument("--depth", type=int, default=10)
    args = parser.parse_args()
    run(args.db, args.output, args.depth)
