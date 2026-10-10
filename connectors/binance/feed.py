"""
connectors/binance/feed.py

The Binance Market Data Feed Adapter (always mainnet).
Maintains a persistent kline websocket, parses closed candles into Bars and
publishes BarClosed. On every (re)connect it backfills the gap since the last
bar it knows of via REST, publishing those bars with replay=True: they bring
the rolling windows up to date but can never trigger a trade.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from connectors.binance.codec import bar_from_ws_kline, interval_ms
from connectors.binance.rest import BinanceMarketDataREST
from messages.events import BarClosed

# Market data ALWAYS uses mainnet; order routing (sim/testnet/live) is the venues' concern.
WS_BASE_URL = "wss://stream.binance.com:9443"

logger = logging.getLogger("BinanceFeed")


class BinanceFeed:
    """
    MarketDataFeed adapter: mainnet kline websocket -> BarClosed.

    Publishes:  BarClosed(bar, replay=False) for live closes, replay=True for backfill
    Depends on: BinanceMarketDataREST (backfill), get_latest_time_cb (seed for the first backfill)
    State:
        _last_start_ms  Dict[symbol, epoch-ms]   newest bar published -> dedupe + next backfill start
        _ws             current connection (closed by stop())
    Lifecycle: start() loops connect -> _backfill_gaps() -> read messages, reconnecting with
    exponential backoff (1s .. 60s); stop() ends the loop. Intra-candle updates are ignored.
    Combined-stream URL: wss://stream.binance.com:9443/stream?streams=<sym>@kline_<tf>/...
    """
    def __init__(
        self,
        event_bus,
        symbols: List[str],
        timeframe: str = "1m",
        rest: Optional[BinanceMarketDataREST] = None,
        get_latest_time_cb: Optional[Callable[[str, str], Awaitable[Optional[int]]]] = None,
    ):
        """
        :param get_latest_time_cb: async (symbol, interval) -> epoch-ms start time of the
                                   newest stored bar; seeds the first backfill.
        """
        self.bus = event_bus
        self.symbols = [s.upper() for s in symbols]
        self.timeframe = timeframe
        self.rest = rest
        self.get_latest_time_cb = get_latest_time_cb
        self._running = False
        self._ws = None
        # epoch-ms start time of the newest bar this feed has published, per symbol
        self._last_start_ms: Dict[str, int] = {}

    async def start(self) -> None:
        """Persistent websocket loop with exponential backoff."""
        self._running = True
        retry_delay = 1.0
        while self._running:
            try:
                await self._listen()
                retry_delay = 1.0
            except asyncio.CancelledError:
                raise
            except ConnectionClosed as e:
                if not self._running:
                    break
                logger.warning(f"Binance WebSocket disconnected: {e}. Reconnecting in {retry_delay}s...")
            except Exception as e:
                if not self._running:
                    break
                logger.error(f"Unexpected feed error: {e}", exc_info=True)
            if self._running:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)

    def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            asyncio.ensure_future(self._ws.close())

    async def _listen(self) -> None:
        streams = [f"{sym.lower()}@kline_{self.timeframe}" for sym in self.symbols]
        ws_url = f"{WS_BASE_URL}/stream?streams={'/'.join(streams)}"
        logger.info(f"Connecting to Binance WS ({len(streams)} streams)")

        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, close_timeout=2) as ws:
            self._ws = ws
            logger.info("Binance WebSocket connected.")
            # Messages arriving during backfill are buffered by the socket and processed
            # afterwards; duplicates are discarded by start-time de-duplication below.
            await self._backfill_gaps()
            async for message in ws:
                if not self._running:
                    break
                await self._process_message(message)
        self._ws = None

    async def _process_message(self, message: str | bytes) -> None:
        data = json.loads(message)
        data = data.get("data", data)
        if data.get("e") != "kline":
            return
        bar = bar_from_ws_kline(data["k"])
        if not bar.is_closed:
            return   # intra-candle updates ignored
        await self._publish(bar, replay=False)

    async def _publish(self, bar, replay: bool) -> None:
        """Publishes a bar unless one with the same or a later start time was already published."""
        start_ms = int(bar.start_time.timestamp() * 1000)
        last = self._last_start_ms.get(bar.symbol)
        if last is not None and start_ms <= last:
            return   # already published (backfill/live overlap)
        self._last_start_ms[bar.symbol] = start_ms
        await self.bus.publish(BarClosed(bar=bar, replay=replay))

    async def _backfill_gaps(self) -> None:
        """On every (re)connect: REST-fetch closed bars after the last known one, publish as replay."""
        if self.rest is None:
            return
        step = interval_ms(self.timeframe)
        for symbol in self.symbols:
            last = self._last_start_ms.get(symbol)
            if last is None and self.get_latest_time_cb is not None:
                last = await self.get_latest_time_cb(symbol, self.timeframe)
            if last is None:
                logger.info(f"[BACKFILL] No history for {symbol}; skipping backfill.")
                continue
            try:
                bars = await self.rest.fetch_bars(symbol, self.timeframe, start_ms=last + step)
            except Exception as e:
                logger.error(f"[BACKFILL] Failed to recover data for {symbol}: {e}")
                continue
            for bar in bars:
                await self._publish(bar, replay=True)
            if bars:
                logger.info(f"[BACKFILL] Recovered {len(bars)} closed candle(s) for {symbol} (replay).")
