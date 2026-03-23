"""BTC price fetcher with caching."""

import time
import logging
import aiohttp

logger = logging.getLogger("collector.btc_price")

BINANCE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"


class BtcPriceFetcher:
    def __init__(self, session: aiohttp.ClientSession, refresh_interval: float = 3.0):
        self._session = session
        self._refresh_interval = refresh_interval
        self._price: float | None = None
        self._last_fetch: float = 0

    @property
    def price(self) -> float | None:
        return self._price

    async def update(self):
        """Fetch BTC price if cache is stale."""
        now = time.time()
        if now - self._last_fetch < self._refresh_interval:
            return
        try:
            async with self._session.get(BINANCE_URL, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    self._price = float(data["price"])
                    self._last_fetch = now
        except Exception as e:
            logger.warning(f"BTC price fetch failed: {e}")
