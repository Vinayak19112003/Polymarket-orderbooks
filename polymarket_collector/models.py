"""Data models for markets, tokens, L2 books, snapshots, and raw ticks."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class MarketWindow:
    slug: str
    title: str
    start_ts: int  # UTC epoch seconds
    end_ts: int  # UTC epoch seconds
    condition_ids: list[str] = field(default_factory=list)


@dataclass
class Token:
    clob_token_id: str
    outcome: str  # "Yes" (Up) or "No" (Down)
    question: str
    condition_id: str
    window_slug: str


@dataclass
class L2Level:
    price: float
    size: float


class L2Book:
    """Sorted L2 orderbook. Bids descending, asks ascending."""

    __slots__ = ("bids", "asks", "last_update_ts")

    def __init__(self):
        self.bids: list[L2Level] = []
        self.asks: list[L2Level] = []
        self.last_update_ts: int = 0

    def set_snapshot(self, raw_bids: list[dict], raw_asks: list[dict], ts_ms: int):
        self.bids = sorted(
            [L2Level(float(b["price"]), float(b["size"])) for b in raw_bids],
            key=lambda x: x.price, reverse=True,
        )
        self.asks = sorted(
            [L2Level(float(a["price"]), float(a["size"])) for a in raw_asks],
            key=lambda x: x.price,
        )
        self.last_update_ts = ts_ms

    def apply_delta(self, side: str, price_str: str, size: float, ts_ms: int):
        price = float(price_str)
        book = self.bids if side == "BUY" else self.asks
        reverse = side == "BUY"

        # Find existing level
        for i, lvl in enumerate(book):
            if abs(lvl.price - price) < 1e-12:
                if size == 0:
                    book.pop(i)
                else:
                    lvl.size = size
                self.last_update_ts = ts_ms
                return

        # New level
        if size > 0:
            new_lvl = L2Level(price, size)
            if reverse:
                idx = 0
                for i, lvl in enumerate(book):
                    if price > lvl.price:
                        idx = i
                        break
                    idx = i + 1
                book.insert(idx, new_lvl)
            else:
                idx = 0
                for i, lvl in enumerate(book):
                    if price < lvl.price:
                        idx = i
                        break
                    idx = i + 1
                book.insert(idx, new_lvl)
        self.last_update_ts = ts_ms

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def best_bid_size(self) -> Optional[float]:
        return self.bids[0].size if self.bids else None

    @property
    def best_ask_size(self) -> Optional[float]:
        return self.asks[0].size if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        bb, ba = self.best_bid, self.best_ask
        return round((bb + ba) / 2, 8) if bb is not None and ba is not None else None

    @property
    def spread(self) -> Optional[float]:
        bb, ba = self.best_bid, self.best_ask
        return round(ba - bb, 8) if bb is not None and ba is not None else None

    def total_bid_size(self, depth: int = 0) -> float:
        levels = self.bids[:depth] if depth > 0 else self.bids
        return sum(l.size for l in levels)

    def total_ask_size(self, depth: int = 0) -> float:
        levels = self.asks[:depth] if depth > 0 else self.asks
        return sum(l.size for l in levels)

    def imbalance(self, depth: int = 5) -> Optional[float]:
        tb = self.total_bid_size(depth)
        ta = self.total_ask_size(depth)
        total = tb + ta
        return round(tb / total, 6) if total > 0 else None


@dataclass
class MergedSnapshot:
    """One row per second with both YES and NO tokens merged."""
    ts_ms: int
    timestamp: str
    window_slug: str
    window_end_ts: int
    outcome: str  # filled after settlement
    yes_token_id: str
    no_token_id: str
    yes_bid: Optional[float]
    yes_ask: Optional[float]
    yes_ask_size: Optional[float]
    yes_bid_size: Optional[float]
    yes_spread: Optional[float]
    no_bid: Optional[float]
    no_ask: Optional[float]
    no_ask_size: Optional[float]
    no_bid_size: Optional[float]
    no_spread: Optional[float]
    yes_mid: Optional[float]
    no_mid: Optional[float]
    yes_imbalance: Optional[float]
    no_imbalance: Optional[float]
    btc_price: Optional[float]


@dataclass
class RawTick:
    ts_ms: int
    received_ts_ms: int
    window_slug: str
    outcome: str
    tick_type: str
    price: Optional[float]
    size: Optional[float]
    side: Optional[str]
    best_bid: Optional[float]
    best_ask: Optional[float]
