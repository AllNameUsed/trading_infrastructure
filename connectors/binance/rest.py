"""
connectors/binance/rest.py

Public (unauthenticated) Binance market-data REST client. Always mainnet:
market data is mainnet regardless of where a strategy's orders are routed.

Used for: gap healing at startup, backfill after websocket reconnects, and
exchangeInfo (instrument filters). One long-lived aiohttp session, explicit
timeouts, pagination, and ONLY closed klines are ever returned.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Iterable, List, Optional

import aiohttp

from connectors.binance.codec import bar_from_rest_kline, interval_ms, spec_from_symbol_info
from messages.instruments import InstrumentSpec
from messages.model import Bar

logger = logging.getLogger("BinanceREST")

REST_BASE_URL = "https://api.binance.com"
_KLINE_LIMIT = 1000


class BinanceMarketDataREST:
    """
    HistoricalBarSource adapter + exchangeInfo loader (public mainnet REST, no keys).
      start() / stop()                          one shared aiohttp session (explicit timeout)
      fetch_bars(sym, interval, start_ms, end_ms) -> [Bar]   paginated by 1000, closed bars only
      fetch_instrument_specs(symbols) -> [InstrumentSpec]    filters for the InstrumentRegistry
    """
    def __init__(self, base_url: str = REST_BASE_URL, timeout_seconds: float = 10.0, pause_seconds: float = 0.1):
        self.base_url = base_url
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.pause = pause_seconds
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self.timeout)

    async def stop(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _get(self, path: str, params: dict):
        await self.start()
        assert self._session is not None
        async with self._session.get(f"{self.base_url}{path}", params=params) as resp:
            data = await resp.json(content_type=None)
            if resp.status != 200:
                raise RuntimeError(f"GET {path} -> HTTP {resp.status}: {data}")
            return data

    async def fetch_bars(self, symbol: str, interval: str, start_ms: int, end_ms: Optional[int] = None) -> List[Bar]:
        """
        Closed bars with start_time in [start_ms, end_ms], oldest first, paginated.
        The currently-forming candle is NEVER returned (close_time must be in the past).
        """
        step = interval_ms(interval)
        bars: List[Bar] = []
        cursor = start_ms
        while True:
            now_ms = int(time.time() * 1000)
            params = {"symbol": symbol.upper(), "interval": interval, "startTime": cursor, "limit": _KLINE_LIMIT}
            if end_ms is not None:
                params["endTime"] = end_ms
            rows = await self._get("/api/v3/klines", params)
            if not rows:
                break
            for k in rows:
                if int(k[6]) >= now_ms:      # close_time in the future -> still forming
                    continue
                bars.append(bar_from_rest_kline(symbol, interval, k))
            last_start = int(rows[-1][0])
            if len(rows) < _KLINE_LIMIT or (end_ms is not None and last_start + step > end_ms):
                break
            cursor = last_start + step
            await asyncio.sleep(self.pause)  # stay well inside the request-weight budget
        return bars

    async def fetch_instrument_specs(self, symbols: Iterable[str]) -> List[InstrumentSpec]:
        wanted = sorted({s.upper() for s in symbols})
        if not wanted:
            return []
        data = await self._get("/api/v3/exchangeInfo", {"symbols": json.dumps(wanted, separators=(",", ":"))})
        return [spec_from_symbol_info(info) for info in data.get("symbols", [])]
