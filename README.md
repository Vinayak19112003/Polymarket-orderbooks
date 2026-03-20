# Polymarket BTC 15m Orderbook Collector

Captures **every single bid/ask change** on Polymarket's BTC Up/Down 15-minute markets via WebSocket. Stores tick-level orderbook data in SQLite for backtesting.

## What it collects

For every 15-minute BTC market, both the **Up token** and **Down token**:

| Event | What it means |
|-------|---------------|
| `book` | Full L2 snapshot (on connect / market start) |
| `price_change` | One price level changed: price, new size, BUY/SELL side |
| `trade` | A trade was executed at a price |

Every row also records `best_bid` and `best_ask` at that moment.

---

## Setup on VM

```bash
# 1. Clone repo
git clone <repo-url>
cd Polymarket-orderbooks

# 2. Install dependencies (Python 3.10+)
pip install -r requirements.txt

# 3. Run collector
python collector.py
```

---

## Run in background (recommended)

```bash
nohup python collector.py > /dev/null 2>&1 &
echo "PID: $!"
```

To check it's running:
```bash
tail -f collector.log
```

To stop:
```bash
kill <PID>
```

---

## Run as systemd service (auto-restart on reboot)

Create `/etc/systemd/system/polymarket-collector.service`:

```ini
[Unit]
Description=Polymarket BTC Orderbook Collector
After=network.target

[Service]
Type=simple
WorkingDirectory=/home/user/Polymarket-orderbooks
ExecStart=/usr/bin/python3 /home/user/Polymarket-orderbooks/collector.py
Restart=always
RestartSec=10
StandardOutput=append:/home/user/Polymarket-orderbooks/collector.log
StandardError=append:/home/user/Polymarket-orderbooks/collector.log

[Install]
WantedBy=multi-user.target
```

Enable it:
```bash
sudo systemctl daemon-reload
sudo systemctl enable polymarket-collector
sudo systemctl start polymarket-collector
sudo systemctl status polymarket-collector
```

---

## Check collected data

```bash
# Count total events
sqlite3 orderbooks.db "SELECT COUNT(*) FROM orderbook_events;"

# Events per token side
sqlite3 orderbooks.db "SELECT token_side, COUNT(*) FROM orderbook_events GROUP BY token_side;"

# Markets collected so far
sqlite3 orderbooks.db "SELECT id, datetime(market_start_ts,'unixepoch'), datetime(market_end_ts,'unixepoch'), resolved FROM markets ORDER BY id DESC LIMIT 10;"

# Events per market
sqlite3 orderbooks.db "SELECT market_id, token_side, COUNT(*) FROM orderbook_events GROUP BY market_id, token_side ORDER BY market_id DESC LIMIT 20;"

# Latest best bid/ask for Up token
sqlite3 orderbooks.db "SELECT datetime(event_ts/1000,'unixepoch'), best_bid, best_ask FROM orderbook_events WHERE token_side='UP' ORDER BY id DESC LIMIT 5;"
```

---

## Export to CSV (for backtest)

```bash
# Export all data to ./data/ folder
python collector.py --export --output ./data/
```

Produces:
- `data/markets.csv` — one row per 15m market with resolution (UP/DOWN)
- `data/orderbook_events_YYYY-MM.csv` — all tick events for each month

### CSV columns (orderbook_events)

| Column | Description |
|--------|-------------|
| `event_ts_ms` | Event timestamp in milliseconds (from Polymarket) |
| `received_ts_ms` | Local time your VM received the event |
| `market_start_ts` | Start of the 15m window (seconds) |
| `market_end_ts` | End of the 15m window (seconds) |
| `token_side` | `UP` or `DOWN` |
| `event_type` | `book`, `price_change`, or `trade` |
| `price` | Price level that changed (e.g. `0.77` = 77¢) |
| `size` | New size at this price (`0` = level removed) |
| `side` | `BUY` (bid) or `SELL` (ask) |
| `best_bid` | Best bid after this event |
| `best_ask` | Best ask after this event |
| `resolved` | `UP`, `DOWN`, or empty (unresolved) |
| `full_book_json` | Full L2 JSON (only on `book` events) |

---

## Load in pandas

```python
import pandas as pd

# Load one month of events
df = pd.read_csv("data/orderbook_events_2025-05.csv")
df["event_dt"] = pd.to_datetime(df["event_ts_ms"], unit="ms", utc=True)

# Filter only Up token price changes
up_changes = df[(df["token_side"] == "UP") & (df["event_type"] == "price_change")]

# Get best bid/ask over time for Up token
up_quotes = df[df["token_side"] == "UP"][["event_dt", "best_bid", "best_ask"]].dropna()

# Load markets
markets = pd.read_csv("data/markets.csv")
markets["start_dt"] = pd.to_datetime(markets["market_start_ts"], unit="s", utc=True)
markets["end_dt"]   = pd.to_datetime(markets["market_end_ts"], unit="s", utc=True)
```

---

## Data volume estimate

| Period | Markets | Events (approx) | DB size |
|--------|---------|-----------------|---------|
| 1 day  | ~96     | 100k – 500k     | ~30 MB  |
| 1 month| ~2,900  | 3M – 15M        | ~1 GB   |
| 3 months| ~8,600 | 10M – 45M       | ~3 GB   |

---

## Sanity checks

```python
# Polymarket invariant: Up price + Down price ≈ 1.0
# Merge Up and Down best_bid at same timestamp and verify
up = df[df["token_side"]=="UP"][["event_ts_ms","best_bid"]].rename(columns={"best_bid":"up_bid"})
dn = df[df["token_side"]=="DOWN"][["event_ts_ms","best_bid"]].rename(columns={"best_bid":"dn_bid"})
merged = pd.merge_asof(up.sort_values("event_ts_ms"), dn.sort_values("event_ts_ms"), on="event_ts_ms")
print((merged["up_bid"] + merged["dn_bid"]).describe())  # should be ~1.0
```
