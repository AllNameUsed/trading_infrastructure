"""
connectors/binance/venue.py

Binance Spot execution adapter (TESTNET or LIVE), implementing the
ExecutionVenue port (connectors/base.py).

Contract highlights:
  * Never publishes to the bus and never mutates OMS state: it RETURNS
    VenueOrderUpdates. The OMS polls working orders through `query()`
    (needs_polling=True), which also fetches the individual trades so fees and
    partial fills of resting LIMIT orders are reported exactly.
  * Never sleeps inside a call. Rate limiting is a client-side guard: when the
    used request weight or a 429/418 back-off says "stop", calls are refused
    immediately with a definite REJECTED (the request never left the process).
  * Distinguishes DEFINITE outcomes (2xx, 4xx, connect failure) from UNKNOWN
    ones (timeout, disconnect mid-flight, 5xx) - the latter raise
    VenueUnknownOutcome so the OMS reconciles instead of guessing.
  * Prices/quantities are formatted with the instrument's tick/step filters.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from typing import Optional, Tuple
from urllib.parse import urlencode

import aiohttp

from connectors.base import UpdateHandler, VenueUnknownOutcome
from connectors.binance.codec import (
    VENUE_ID,
    error_message,
    fills_from_my_trades,
    update_from_order_response,
)
from messages.instruments import InstrumentRegistry
from messages.model import (
    LIMIT_PRICED_TYPES,
    ExecutionMode,
    OrderRequest,
    OrderSide,
    OrderStatus,
    VenueOrderUpdate,
)

logger = logging.getLogger("BinanceVenue")

BASE_URLS = {
    ExecutionMode.TESTNET: "https://testnet.binance.vision",
    ExecutionMode.LIVE: "https://api.binance.com",
}
_UNKNOWN_ORDER_CODES = {-2011, -2013}   # "Unknown order sent." / "Order does not exist."


class _Throttled(Exception):
    """Refused client-side (back-off, weight guard, missing keys): the request was NOT sent."""


class BinanceVenue:
    """
    ExecutionVenue adapter for Binance Spot (TESTNET or LIVE base URL by mode).

    State:
        _session          one aiohttp session, X-MBX-APIKEY header, total timeout
        current_weight    last X-MBX-USED-WEIGHT-1M seen (reset each minute)
        _backoff_until    epoch seconds; set from Retry-After on 418/429
    Port methods:
        submit(req)   POST /api/v3/order (FULL)    -> update with fills | REJECTED | raises VenueUnknownOutcome
        cancel(...)   DELETE /api/v3/order         -> CANCELED update | None (unknown -> OMS queries)
        query(...)    GET /api/v3/order (+ GET /api/v3/myTrades when executedQty > 0 for exact fills/fees)
                      -> update | None on -2011/-2013 "order does not exist"
    Outcome classification: 200 -> parse; 4xx -> REJECTED; 5xx / timeout / mid-flight
    disconnect -> VenueUnknownOutcome; connect failure / throttle -> REJECTED (not sent).
    needs_polling = True: no user-data stream yet; the OMS polls working orders.
    """
    venue_id = VENUE_ID
    needs_polling = True

    def __init__(self, api_key: str, api_secret: str, mode: ExecutionMode, instruments: InstrumentRegistry,
                 request_timeout: float = 10.0, weight_limit: int = 5000, recv_window: int = 5000):
        if mode not in BASE_URLS:
            raise ValueError(f"BinanceVenue supports TESTNET/LIVE, not {mode.name}")
        self.mode = mode
        self.base_url = BASE_URLS[mode]
        self.instruments = instruments
        self._api_key = api_key or ""
        self._api_secret = (api_secret or "").encode("utf-8")
        self._timeout = aiohttp.ClientTimeout(total=request_timeout)
        self._recv_window = recv_window
        self._session: Optional[aiohttp.ClientSession] = None
        self._handler: Optional[UpdateHandler] = None

        self.weight_limit = weight_limit
        self.current_weight = 0
        self._weight_minute = 0
        self._backoff_until = 0.0

    # =================================================================
    # Lifecycle
    # =================================================================
    async def start(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout, headers={"X-MBX-APIKEY": self._api_key})
        if not self._api_key or not self._api_secret:
            logger.error(f"[{self.mode.name}] Binance API keys are not configured; every order will be rejected.")

    async def stop(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def set_update_handler(self, handler: UpdateHandler) -> None:
        self._handler = handler   # polling venue: kept for a future user-data stream

    # =================================================================
    # Transport
    # =================================================================
    def _guard(self) -> None:
        """Raises _Throttled if a back-off is active or this minute's weight budget is spent."""
        now = time.time()
        if now < self._backoff_until:
            raise _Throttled(f"Binance back-off active for {self._backoff_until - now:.0f}s more")
        minute = int(now // 60)
        if minute != self._weight_minute:
            self._weight_minute, self.current_weight = minute, 0
        if self.current_weight >= self.weight_limit:
            raise _Throttled(f"Client weight guard: {self.current_weight}/{self.weight_limit} used this minute")

    def _track(self, resp: aiohttp.ClientResponse) -> None:
        """Records used weight from headers; arms the back-off on 418/429."""
        used = resp.headers.get("X-MBX-USED-WEIGHT-1M")
        if used:
            self.current_weight = int(used)
            self._weight_minute = int(time.time() // 60)
        if resp.status in (418, 429):
            retry_after = int(resp.headers.get("Retry-After", "60"))
            self._backoff_until = time.time() + retry_after
            logger.critical(f"[{self.mode.name}] HTTP {resp.status}: backing off {retry_after}s (calls refused meanwhile).")

    async def _signed(self, method: str, path: str, params: dict) -> Tuple[int, object]:
        """HMAC-SHA256-signed request (recvWindow + timestamp). Returns (http_status, json)."""
        if not self._api_key or not self._api_secret:
            raise _Throttled("API keys not configured")
        self._guard()
        if self._session is None:
            await self.start()
        assert self._session is not None
        params = {**params, "recvWindow": self._recv_window, "timestamp": int(time.time() * 1000)}
        query = urlencode(params)
        signature = hmac.new(self._api_secret, query.encode("utf-8"), hashlib.sha256).hexdigest()
        url = f"{self.base_url}{path}?{query}&signature={signature}"
        async with self._session.request(method, url) as resp:
            self._track(resp)
            data = await resp.json(content_type=None)
            return resp.status, data

    # =================================================================
    # ExecutionVenue port
    # =================================================================
    async def submit(self, request: OrderRequest) -> VenueOrderUpdate:
        sym = request.symbol
        params = {
            "symbol": sym,
            "side": request.side.name,
            "type": request.order_type.name,
            "newClientOrderId": request.client_order_id,
            "quantity": self.instruments.format_qty(sym, request.quantity),
            "newOrderRespType": "FULL",
        }
        if request.order_type in LIMIT_PRICED_TYPES:
            params["timeInForce"] = request.time_in_force.value
            params["price"] = self.instruments.format_price(sym, request.price)  # type: ignore[arg-type]
        if request.stop_price is not None:
            params["stopPrice"] = self.instruments.format_price(sym, request.stop_price)

        try:
            status, data = await self._signed("POST", "/api/v3/order", params)
        except _Throttled as e:
            return self._rejected(request, str(e))
        except aiohttp.ClientConnectorError as e:
            return self._rejected(request, f"Could not connect (order not sent): {e}")
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            raise VenueUnknownOutcome(f"{type(e).__name__} while submitting: {e}") from e

        if status == 200 and isinstance(data, dict):
            return update_from_order_response(data, request.side)
        if status >= 500:
            raise VenueUnknownOutcome(f"HTTP {status}: {error_message(data) or data}")
        return self._rejected(request, error_message(data) or f"HTTP {status}")

    async def cancel(self, symbol: str, client_order_id: str) -> Optional[VenueOrderUpdate]:
        try:
            status, data = await self._signed("DELETE", "/api/v3/order",
                                              {"symbol": symbol, "origClientOrderId": client_order_id})
        except _Throttled as e:
            logger.warning(f"Cancel of {client_order_id} refused: {e}")
            return None
        if status == 200 and isinstance(data, dict):
            # A cancel response's clientOrderId is the CANCEL request's id; ours is origClientOrderId.
            data["clientOrderId"] = data.get("origClientOrderId", client_order_id)
            return update_from_order_response(data, OrderSide[data.get("side", "BUY")])
        logger.warning(f"Cancel of {client_order_id} failed: {error_message(data) or status}; OMS will reconcile.")
        return None

    async def query(self, symbol: str, client_order_id: str) -> Optional[VenueOrderUpdate]:
        try:
            status, data = await self._signed("GET", "/api/v3/order",
                                              {"symbol": symbol, "origClientOrderId": client_order_id})
        except _Throttled as e:
            raise RuntimeError(str(e)) from e
        if status != 200 or not isinstance(data, dict):
            if isinstance(data, dict) and data.get("code") in _UNKNOWN_ORDER_CODES:
                return None
            raise RuntimeError(f"Query failed: {error_message(data) or status}")

        update = update_from_order_response(data, OrderSide[data["side"]])
        if float(data.get("executedQty", "0")) > 0:
            # The order endpoint has no per-trade detail; fetch the executions (fees, prices).
            t_status, trades = await self._signed("GET", "/api/v3/myTrades",
                                                  {"symbol": symbol, "orderId": data["orderId"]})
            if t_status == 200 and isinstance(trades, list):
                update = VenueOrderUpdate(
                    client_order_id=client_order_id, status=update.status, venue_order_id=update.venue_order_id,
                    fills=fills_from_my_trades(trades, client_order_id),
                    cumulative_filled_quantity=update.cumulative_filled_quantity,
                )
        return update

    @staticmethod
    def _rejected(request: OrderRequest, reason: str) -> VenueOrderUpdate:
        logger.error(f"Order {request.client_order_id} rejected: {reason}")
        return VenueOrderUpdate(client_order_id=request.client_order_id, status=OrderStatus.REJECTED, reason=reason)
