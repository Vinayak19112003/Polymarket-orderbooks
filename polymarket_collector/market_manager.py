"""Market discovery, filtering, and lifecycle tracking."""

import time
import logging
import orjson
import aiohttp
from typing import Optional

from .config import CollectorConfig
from .models import MarketWindow, Token

logger = logging.getLogger("collector.market_mgr")


def _parse_json_field(val):
    if isinstance(val, str):
        try:
            return orjson.loads(val)
        except Exception:
            return val
    return val


class MarketManager:
    def __init__(self, config: CollectorConfig, session: aiohttp.ClientSession):
        self._config = config
        self._session = session
        self._windows: dict[str, MarketWindow] = {}  # slug -> window
        self._tokens: dict[str, Token] = {}           # clob_token_id -> token
        self._resolved: dict[str, str] = {}           # condition_id -> winner

    async def discover(self) -> tuple[list[MarketWindow], list[Token]]:
        """Fetch Gamma API with 15M tag filter, return (new_windows, new_tokens)."""
        now_ts = int(time.time())
        grace = self._config.market_expire_grace_s

        # Paginate through the 15M tag to get all BTC 15m markets
        events = []
        try:
            for offset in range(0, 2000, 200):
                params = {
                    "active": "true",
                    "closed": "false",
                    "limit": "200",
                    "offset": str(offset),
                    "tag_id": self._config.gamma_tag_id,
                }
                async with self._session.get(
                    self._config.gamma_url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    resp.raise_for_status()
                    batch = await resp.json()
                if not batch:
                    break
                events.extend(batch)
                if len(batch) < 200:
                    break
        except Exception as e:
            logger.error(f"Gamma API error: {e}")
            return [], []

        new_windows = []
        new_tokens = []
        pattern = self._config.slug_pattern

        for event in events:
            slug = event.get("slug", "")
            if pattern not in slug.lower():
                continue

            end_str = event.get("endDate", "")
            start_str = event.get("startTime", "")
            end_ts = 0
            start_ts = 0
            from datetime import datetime, timezone
            if end_str:
                try:
                    end_ts = int(datetime.fromisoformat(
                        end_str.replace("Z", "+00:00")
                    ).timestamp())
                except Exception:
                    pass
            if start_str:
                try:
                    start_ts = int(datetime.fromisoformat(
                        start_str.replace("Z", "+00:00")
                    ).timestamp())
                except Exception:
                    pass
            if not start_ts and end_ts:
                start_ts = end_ts - 900

            # Skip expired beyond grace
            if end_ts and end_ts < now_ts - grace:
                continue

            # Already known?
            if slug in self._windows:
                continue

            title = event.get("title", "")
            condition_ids = []
            tokens_for_window = []

            for mk in event.get("markets", []):
                if isinstance(mk, str):
                    try:
                        mk = orjson.loads(mk)
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
                if condition_id:
                    condition_ids.append(condition_id)

                for i, token_id in enumerate(clob_ids):
                    if token_id in self._tokens:
                        continue
                    outcome = outcomes[i] if i < len(outcomes) else f"outcome_{i}"
                    tok = Token(
                        clob_token_id=token_id,
                        outcome=outcome,
                        question=question,
                        condition_id=condition_id,
                        window_slug=slug,
                    )
                    self._tokens[token_id] = tok
                    tokens_for_window.append(tok)

            if tokens_for_window:
                window = MarketWindow(
                    slug=slug,
                    title=title,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    condition_ids=condition_ids,
                )
                self._windows[slug] = window
                new_windows.append(window)
                new_tokens.extend(tokens_for_window)
                logger.info(
                    f"New window: {slug} | end_ts={end_ts} | "
                    f"tokens={len(tokens_for_window)}"
                )

        return new_windows, new_tokens

    def get_active_token_ids(self) -> set[str]:
        now_ts = int(time.time())
        grace = self._config.market_expire_grace_s
        active = set()
        for tid, tok in self._tokens.items():
            w = self._windows.get(tok.window_slug)
            if w and (w.end_ts == 0 or w.end_ts + grace > now_ts):
                active.add(tid)
        return active

    def get_expired_token_ids(self) -> set[str]:
        now_ts = int(time.time())
        grace = self._config.market_expire_grace_s
        expired = set()
        for tid, tok in self._tokens.items():
            w = self._windows.get(tok.window_slug)
            if w and w.end_ts > 0 and w.end_ts + grace <= now_ts:
                expired.add(tid)
        return expired

    def get_token(self, clob_token_id: str) -> Optional[Token]:
        return self._tokens.get(clob_token_id)

    def get_window(self, slug: str) -> Optional[MarketWindow]:
        return self._windows.get(slug)

    async def check_resolutions(self):
        """Poll Gamma /markets for closed conditions."""
        unresolved_cids = set()
        for tok in self._tokens.values():
            if tok.condition_id and tok.condition_id not in self._resolved:
                unresolved_cids.add(tok.condition_id)

        for cid in unresolved_cids:
            try:
                async with self._session.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"conditionIds": cid},
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    data = await resp.json()

                for market in data:
                    if not isinstance(market, dict):
                        continue
                    if not market.get("closed"):
                        continue
                    winner_idx = market.get("winnerIndex")
                    outcomes = _parse_json_field(market.get("outcomes", []))
                    if (
                        winner_idx is not None
                        and isinstance(outcomes, list)
                        and winner_idx < len(outcomes)
                    ):
                        resolved = outcomes[winner_idx].upper()
                        self._resolved[cid] = resolved
                        logger.info(f"Resolved condition={cid[:16]}... → {resolved}")
            except Exception as e:
                logger.debug(f"Resolution check error for {cid[:16]}: {e}")

    def get_resolution(self, condition_id: str) -> Optional[str]:
        return self._resolved.get(condition_id)
