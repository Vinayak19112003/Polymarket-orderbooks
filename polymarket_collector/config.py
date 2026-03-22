"""Centralized configuration via pydantic-settings + env vars."""

from pathlib import Path
from pydantic_settings import BaseSettings


class CollectorConfig(BaseSettings):
    # Paths
    data_dir: Path = Path("/home/ubuntu/Polymarket-orderbooks/data")
    log_dir: Path = Path("/home/ubuntu/Polymarket-orderbooks/logs")

    # API endpoints
    gamma_url: str = "https://gamma-api.polymarket.com/events"
    clob_url: str = "https://clob.polymarket.com"
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Market discovery
    slug_pattern: str = "btc-updown-15m"
    gamma_tag_id: str = "102467"  # "15M" tag on Gamma API
    discovery_interval_s: int = 30
    market_expire_grace_s: int = 300

    # WebSocket
    max_concurrent_streams: int = 100
    ws_ping_interval_s: int = 20
    ws_ping_timeout_s: int = 20
    ws_reconnect_base_s: float = 1.0
    ws_reconnect_max_s: float = 60.0

    # REST fallback
    rest_book_timeout_s: int = 15

    # Snapshot scheduler
    snapshot_interval_s: float = 1.0
    snapshot_book_depth: int = 20

    # Storage
    parquet_row_group_size: int = 10_000
    parquet_flush_interval_s: float = 30.0

    # Health
    stale_stream_timeout_s: int = 120
    health_check_interval_s: int = 60

    model_config = {"env_prefix": "PMC_"}
