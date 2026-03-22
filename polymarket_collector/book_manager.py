"""WebSocket streams + REST fallback for in-memory L2 state."""

import asyncio
import time
import logging
from typing import Optional, Callable, Awaitable

import orjson
import aiohttp
import websockets

from .config import CollectorConfig
from .models import Token, L2Book, RawTick

logger = logging.getLogger("collector.book_mgr")


class TokenStreamState:
    """Per-token mutable state: L2 book + tick counters."""

    __slots__ = (
        "token", "book", "_tick_counts",
        "last_trade_price", "last_trade_size",
        "ws_connected", "last_message_ts",
    )

    def __init__(self, token: Token):
        self.token = token
        self.book = L2Book()
        self._tick_counts: dict[str, int] = {}
        self.last_trade_price: Optional[float] = None
        self.last_trade_size: Optional[float] = None
        self.ws_connected: bool = False
        self.last_message_ts: int = 0

    def apply_book(self, bids: list, asks: list, ts_ms: int):
        self.book.set_snapshot(bids, asks, ts_ms)
        self.last_message_ts = ts_ms
        self._tick_counts["book"] = self._tick_counts.get("book", 0) + 1

    def apply_price_change(self, side: str, price: str, size: float, ts_ms: int):
        self.book.apply_delta(side, price, size, ts_ms)
        self.last_message_ts = ts_ms
        self._tick_counts["price_change"] = self._tick_counts.get("price_change", 0) + 1

    def apply_trade(self, price: float, size: float, ts_ms: int):
        self.last_trade_price = price
        self.last_trade_size = size
        self.last_message_ts = ts_ms
        self._tick_counts["trade"] = self._tick_counts.get("trade", 0) + 1

    def reset_tick_counters(self) -> dict[str, int]:
        counts = dict(self._tick_counts)
        self._tick_counts.clear()
        return counts


class BookManager:
    def __init__(
        self,
        config: CollectorConfig,
        session: aiohttp.ClientSession,
        on_raw_tick: Callable[[RawTick], Awaitable[None]],
    ):
        self._config = config
        self._session = session
        self._on_raw_tick = on_raw_tick
        self._states: dict[str, TokenStreamState] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._semaphore = asyncio.Semaphore(config.max_concurrent_streams)

    async def start_token(self, token: Token):
        cid = token.clob_token_id
        if cid in self._states:
            return
        state = TokenStreamState(token)
        self._states[cid] = state
        task = asyncio.create_task(self._run_stream(cid), name=f"ws_{cid[:12]}")
        self._tasks[cid] = task
        logger.info(f"Started stream: {token.outcome} | {token.window_slug}")

    async def stop_token(self, clob_token_id: str):
        task = self._tasks.pop(clob_token_id, None)
        state = self._states.pop(clob_token_id, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if state:
            logger.info(f"Stopped stream: {state.token.outcome} | {state.token.window_slug}")

    def get_state(self, clob_token_id: str) -> Optional[TokenStreamState]:
        return self._states.get(clob_token_id)

    def get_all_active_states(self) -> dict[str, TokenStreamState]:
        return dict(self._states)

    async def _run_stream(self, clob_token_id: str):
        delay = self._config.ws_reconnect_base_s
        state = self._states.get(clob_token_id)
        if not state:
            return

        while clob_token_id in self._states:
            async with self._semaphore:
                try:
                    # Seed from REST first
                    await self._seed_from_rest(state)
                    # Then run WS loop
                    await self._ws_loop(state)
                    delay = self._config.ws_reconnect_base_s
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    state.ws_connected = False
                    if clob_token_id not in self._states:
                        return
                    logger.warning(
                        f"[{state.token.outcome}:{state.token.window_slug[:25]}] "
                        f"WS error: {e}, retry in {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._config.ws_reconnect_max_s)

    async def _seed_from_rest(self, state: TokenStreamState):
        url = f"{self._config.clob_url}/book"
        try:
            async with self._session.get(
                url,
                params={"token_id": state.token.clob_token_id},
                timeout=aiohttp.ClientTimeout(total=self._config.rest_book_timeout_s),
            ) as resp:
                data = await resp.json()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            ts_ms = int(time.time() * 1000)
            state.apply_book(bids, asks, ts_ms)
            logger.debug(
                f"REST seed: {state.token.outcome}:{state.token.window_slug[:25]} "
                f"bids={len(bids)} asks={len(asks)}"
            )
        except Exception as e:
            logger.warning(f"REST seed failed for {state.token.clob_token_id[:16]}: {e}")

    async def _ws_loop(self, state: TokenStreamState):
        async with websockets.connect(
            self._config.ws_url,
            ping_interval=self._config.ws_ping_interval_s,
            ping_timeout=self._config.ws_ping_timeout_s,
            close_timeout=5,
        ) as ws:
            await ws.send(orjson.dumps({
                "assets_ids": [state.token.clob_token_id],
                "type": "market",
            }).decode())
            state.ws_connected = True
            logger.info(
                f"WS subscribed: {state.token.outcome}:{state.token.window_slug[:25]}"
            )

            async for raw in ws:
                if state.token.clob_token_id not in self._states:
                    return
                received_ts = int(time.time() * 1000)
                try:
                    msg = orjson.loads(raw)
                except Exception:
                    continue

                events = msg if isinstance(msg, list) else [msg]
                for event in events:
                    await self._handle_event(state, event, received_ts)

    async def _handle_event(self, state: TokenStreamState, event: dict, received_ts: int):
        etype = event.get("event_type")
        if not etype:
            return

        ts_raw = event.get("timestamp")
        ts_ms = int(ts_raw) if ts_raw else received_ts
        tok = state.token

        if etype == "book":
            bids = event.get("bids", [])
            asks = event.get("asks", [])
            state.apply_book(bids, asks, ts_ms)
            await self._on_raw_tick(RawTick(
                ts_ms=ts_ms, received_ts_ms=received_ts,
                window_slug=tok.window_slug, outcome=tok.outcome,
                tick_type="book", price=None, size=None, side=None,
                best_bid=state.book.best_bid, best_ask=state.book.best_ask,
            ))

        elif etype == "price_change":
            for change in event.get("changes", []):
                side = change.get("side", "")
                price_str = change.get("price", "0")
                size = float(change.get("size", 0))
                state.apply_price_change(side, price_str, size, ts_ms)
                await self._on_raw_tick(RawTick(
                    ts_ms=ts_ms, received_ts_ms=received_ts,
                    window_slug=tok.window_slug, outcome=tok.outcome,
                    tick_type="price_change",
                    price=float(price_str), size=size, side=side,
                    best_bid=state.book.best_bid, best_ask=state.book.best_ask,
                ))

        elif etype == "last_trade_price":
            price = float(event.get("price", 0))
            size = float(event.get("size", 0))
            state.apply_trade(price, size, ts_ms)
            await self._on_raw_tick(RawTick(
                ts_ms=ts_ms, received_ts_ms=received_ts,
                window_slug=tok.window_slug, outcome=tok.outcome,
                tick_type="trade", price=price, size=size, side=None,
                best_bid=state.book.best_bid, best_ask=state.book.best_ask,
            ))
