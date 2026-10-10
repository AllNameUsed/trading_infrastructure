"""
connectors/binance/quotes.py

Mainnet top-of-book (bookTicker) and trade tape (aggTrade) for the simulated
venue's matching engine.

Deliberately NOT routed through the Event Bus: these streams run at hundreds
of messages per second per liquid symbol, and the bus is the system's single
critical path. The stream calls a QuoteSink directly (adapter -> adapter,
wired in main.py); decision components keep consuming bars via the bus.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import List, Protocol

import websockets
from websockets.exceptions import ConnectionClosed

from connectors.binance.feed import WS_BASE_URL

logger = logging.getLogger("BinanceQuotes")


class QuoteSink(Protocol):
    """Receiver of top-of-book and trade prints (implemented by SimulatedVenue). Must not block."""
    def on_book_ticker(self, symbol: str, bid: float, bid_qty: float, ask: float, ask_qty: float) -> None: ...
    def on_trade(self, symbol: str, price: float, qty: float, is_buyer_maker: bool) -> None: ...


class BinanceQuoteStream:
    """
    Mainnet <sym>@bookTicker (+ <sym>@aggTrade) websocket -> QuoteSink callbacks.
    Bypasses the bus by design (message rate). Reconnects with backoff (1s .. 30s).
      bookTicker  {"s","b","B","a","A"}           -> sink.on_book_ticker(sym, bid, bid_qty, ask, ask_qty)
      aggTrade    {"s","p","q","m"} (m = buyer is maker, i.e. SELL aggressor) -> sink.on_trade(...)
    """
    def __init__(self, symbols: List[str], sink: QuoteSink, include_trades: bool = True):
        self.symbols = [s.lower() for s in symbols]
        self.sink = sink
        self.include_trades = include_trades
        self._running = False
        self._ws = None

    async def start(self) -> None:
        self._running = True
        delay = 1.0
        while self._running:
            try:
                await self._listen()
                delay = 1.0
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError) as e:
                if not self._running:
                    break
                logger.warning(f"Quote stream disconnected: {e}. Reconnecting in {delay}s...")
            except Exception:
                if not self._running:
                    break
                logger.exception("Quote stream error")
            if self._running:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    def stop(self) -> None:
        self._running = False
        if self._ws is not None:
            asyncio.ensure_future(self._ws.close())

    async def _listen(self) -> None:
        streams = [f"{s}@bookTicker" for s in self.symbols]
        if self.include_trades:
            streams += [f"{s}@aggTrade" for s in self.symbols]
        url = f"{WS_BASE_URL}/stream?streams={'/'.join(streams)}"
        async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=2, max_queue=4096) as ws:
            self._ws = ws
            logger.info(f"Quote stream connected ({len(streams)} streams).")
            async for message in ws:
                if not self._running:
                    break
                self._handle(message)
        self._ws = None

    def _handle(self, message: str | bytes) -> None:
        envelope = json.loads(message)
        stream: str = envelope.get("stream", "")
        d = envelope.get("data", envelope)
        try:
            if stream.endswith("@bookTicker"):
                self.sink.on_book_ticker(d["s"], float(d["b"]), float(d["B"]), float(d["a"]), float(d["A"]))
            elif stream.endswith("@aggTrade"):
                self.sink.on_trade(d["s"], float(d["p"]), float(d["q"]), bool(d["m"]))
        except Exception:
            logger.exception(f"Quote sink failed on {stream}")
