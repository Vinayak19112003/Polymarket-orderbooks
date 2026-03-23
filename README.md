# Polymarket BTC 15m Orderbook Collector

24/7 automated collector that captures **1-second orderbook snapshots** from Polymarket's Bitcoin Up/Down 15-minute prediction markets, with live BTC price from Binance.

## Live Data

CSV files in [`csv_data/`](csv_data/) are updated **every 15 minutes** automatically.

### Output Format — `orderbook_YYYY-MM-DD.csv`

One row per second, both YES (Up) and NO (Down) tokens merged:

| # | Column | Type | Description |
|---|--------|------|-------------|
| 1 | `ts_ms` | int | Unix timestamp in milliseconds |
| 2 | `timestamp` | string | Human readable UTC timestamp |
| 3 | `window_slug` | string | Market identifier (e.g. `btc-updown-15m-1774155600`) |
| 4 | `window_end_ts` | int | Window end unix timestamp |
| 5 | `outcome` | string | Final result (`Up`/`Down`), filled after settlement |
| 6 | `yes_token_id` | string | YES token CLOB ID |
| 7 | `no_token_id` | string | NO token CLOB ID |
| 8 | `yes_bid` | float | Best bid for YES token |
| 9 | `yes_ask` | float | Best ask for YES token |
| 10 | `yes_ask_size` | float | Shares available at YES best ask |
| 11 | `yes_bid_size` | float | Shares available at YES best bid |
| 12 | `yes_spread` | float | `yes_ask - yes_bid` |
| 13 | `no_bid` | float | Best bid for NO token |
| 14 | `no_ask` | float | Best ask for NO token |
| 15 | `no_ask_size` | float | Shares available at NO best ask |
| 16 | `no_bid_size` | float | Shares available at NO best bid |
| 17 | `no_spread` | float | `no_ask - no_bid` |
| 18 | `yes_mid` | float | `(yes_bid + yes_ask) / 2` |
| 19 | `no_mid` | float | `(no_bid + no_ask) / 2` |
| 20 | `yes_imbalance` | float | `yes_bid_size / (yes_bid_size + yes_ask_size)` |
| 21 | `no_imbalance` | float | `no_bid_size / (no_bid_size + no_ask_size)` |
| 22 | `btc_price` | float | Current BTC/USDT price from Binance |

### Example

```csv
ts_ms,timestamp,window_slug,window_end_ts,outcome,yes_token_id,no_token_id,yes_bid,yes_ask,yes_ask_size,yes_bid_size,yes_spread,no_bid,no_ask,no_ask_size,no_bid_size,no_spread,yes_mid,no_mid,yes_imbalance,no_imbalance,btc_price
1774240513565,2026-03-23T04:35:13.565,btc-updown-15m-1774240200,1774241100,Up,abc123,def456,0.73,0.74,43.81,787.74,0.01,0.26,0.27,747.74,67.0,0.01,0.735,0.265,0.947,0.082,68299.99
```

### Data Volume

| Period | Windows | Rows | CSV Size |
|--------|---------|------|----------|
| 1 day | ~96 | ~86,400 | ~30 MB |
| 1 month | ~2,900 | ~2.6M | ~900 MB |

## Architecture

```
polymarket_collector/
├── __main__.py          # Entry point
├── config.py            # Settings via env vars (PMC_ prefix)
├── models.py            # MarketWindow, Token, L2Book, MergedSnapshot
├── market_manager.py    # Gamma API discovery + lifecycle
├── book_manager.py      # WebSocket L2 book management
├── btc_price.py         # Binance BTC/USDT price fetcher
├── snapshot_scheduler.py # 1-sec merged snapshot loop
├── storage.py           # CSV writer + Parquet tick buffer
├── health.py            # Stale stream detection
└── logging_setup.py     # Structured rotating logs
```

**Data flow:**
1. `MarketManager` discovers BTC 15m markets via Polymarket Gamma API
2. `BookManager` opens WebSocket streams and maintains in-memory L2 orderbooks
3. `SnapshotScheduler` ticks every 1 second, merges YES/NO books into one row
4. `BtcPriceFetcher` gets live BTC price from Binance (cached 3s)
5. `Storage` writes rows to daily CSV files + raw ticks to Parquet

## Setup

```bash
# Clone
git clone https://github.com/Vinayak19112003/Polymarket-orderbooks.git
cd Polymarket-orderbooks

# Install
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run
python -m polymarket_collector
```

### Run as systemd service (recommended)

```bash
sudo tee /etc/systemd/system/polymarket-collector.service << 'EOF'
[Unit]
Description=Polymarket BTC 15m Orderbook Collector
After=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/Polymarket-orderbooks
ExecStart=/home/ubuntu/Polymarket-orderbooks/.venv/bin/python -m polymarket_collector
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable polymarket-collector
sudo systemctl start polymarket-collector
```

### Auto-push to GitHub (every 15 min)

```bash
crontab -e
# Add:
*/15 * * * * /home/ubuntu/Polymarket-orderbooks/daily_push.sh >> /home/ubuntu/Polymarket-orderbooks/logs/push.log 2>&1
```

## Configuration

All settings can be overridden via environment variables with `PMC_` prefix:

| Variable | Default | Description |
|----------|---------|-------------|
| `PMC_DATA_DIR` | `./data` | Data output directory |
| `PMC_SLUG_PATTERN` | `btc-updown-15m` | Market slug filter |
| `PMC_SNAPSHOT_INTERVAL_S` | `1.0` | Snapshot frequency (seconds) |
| `PMC_DISCOVERY_INTERVAL_S` | `30` | Market discovery polling (seconds) |
| `PMC_WS_RECONNECT_MAX_S` | `60.0` | Max WebSocket reconnect backoff |

## Load in Python

```python
import pandas as pd

df = pd.read_csv("csv_data/orderbook_2026-03-23.csv")

# Filter one window
window = df[df["window_slug"] == "btc-updown-15m-1774240200"]

# Yes price over time
window.plot(x="timestamp", y=["yes_bid", "yes_ask"], title="YES Token Price")

# Imbalance signal
window.plot(x="timestamp", y=["yes_imbalance", "no_imbalance"], title="Order Imbalance")

# BTC price vs prediction
window.plot(x="timestamp", y="btc_price", secondary_y="yes_mid", title="BTC vs Market")
```
