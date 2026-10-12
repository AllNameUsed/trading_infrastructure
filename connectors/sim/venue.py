"""
connectors/sim/venue.py

SimulatedVenue - an internal Binance-Spot-like matching engine driven by LIVE
MAINNET market data. This is where ExecutionMode.SIM strategies trade.

Why not the Binance testnet? The testnet runs its own, thin, unrelated order
book: MARKET orders fill at prices that can be far from what the strategy saw
on mainnet, so paper PnL measures the testnet, not the strategy. This engine
fills against the mainnet top-of-book and trade tape instead, with explicit,
tunable models for everything a real fill depends on.

What is simulated
-----------------
  Exchange rules
    * LOT_SIZE / MARKET_LOT_SIZE / PRICE_FILTER / NOTIONAL filters (real
      exchangeInfo via InstrumentRegistry) with Binance-style reject messages.
    * Spot balances: free/locked per asset. Buys need quote, sells need base -
      spot cannot short; "Account has insufficient balance" like Binance.
    * Funds are locked while LIMIT/STOP orders rest and released on cancel/expiry.
    * Duplicate client order ids rejected; order-rate limit (orders per 10s).
    * Stop orders that would trigger immediately are rejected (-2010).
    * Time in force: GTC rests, IOC expires the remainder, FOK all-or-nothing.
  Latency
    * Each order/cancel travels to the matching engine after a sampled
      one-way latency (lognormal), and is matched against the book AS IT IS
      ON ARRIVAL - not as it was when the strategy decided.
    * Responses and asynchronous execution reports travel back with latency,
      through one FIFO "wire", like a user-data stream.
  Liquidity & price impact (taker)
    * Fills walk a book whose level 0 is the real best bid/ask and size from
      the bookTicker stream; deeper levels are synthetic (configurable spacing
      and size growth). Large orders therefore fill across several prices and
      pay realistic slippage; each level is a separate fill/trade id.
    * Liquidity we take is removed from the top of book until the next
      market-data update (two back-to-back orders don't both get the touch).
  Passive fills (maker) - conservative queue model
    * A resting order joining the best price queues behind the size already
      displayed there; improving the price puts it at the front.
    * Trades AT our price (with the aggressor on the other side) first consume
      the queue ahead of us, then fill us. Trades THROUGH our price fill us fully.
    * A book update that crosses our price fills us up to the displayed size.
  Stops
    * Triggered by the last trade price (Binance semantics), then executed as
      MARKET or LIMIT against the book at trigger time - so stops slip in gaps.
  Fees
    * Taker/maker rates; by default charged in the RECEIVED asset like Binance
      without BNB (base on buys, quote on sells).
  Restarts
    * The simulator is in-memory and forgets everything when the process stops
      (survives_restart = False). Rehydration therefore treats the store as the
      truth: it rebuilds the balances from the fills ledger (restore_from_fills) and
      marks orders that were working at the stop as canceled. Trade and order ids
      carry a per-process tag, so they never repeat across restarts.
  Degraded mode
    * If no live quote/trade stream is attached, closed bars synthesize a book
      (close +/- a half-spread) and resting/stop orders are evaluated against
      bar high/low (strict trade-through, gaps fill at the open).
    * Orders are rejected when market data is stale - never filled at stale prices.

All state mutation is synchronous inside the event loop (no awaits between
read and write), so matching is atomic with respect to other coroutines.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import math
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

from connectors.base import UpdateHandler
from messages.instruments import InstrumentRegistry
from messages.model import (
    LIMIT_PRICED_TYPES,
    STOP_TYPES,
    Bar,
    ExecutionMode,
    Fill,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    VenueOrderUpdate,
    utc_now,
)

logger = logging.getLogger("SimulatedVenue")

_EPS = 1e-12


@dataclass
class SimConfig:
    """
    Every knob of the simulated market (see module docstring for the model).
      initial_balances           free balance per asset at start (spot: buys need quote, sells need base)
      taker/maker_fee_rate       fraction of notional (Binance VIP0 = 0.001)
      fee_in_received_asset      True: buy fee in base, sell fee in quote (Binance without BNB)
      latency_median_ms/sigma    one-way lognormal latency (orders, cancels, responses, reports)
      max_quote_age_seconds      reject orders if the live book is older than this
      max_bar_quote_age_seconds  same, when running on bar-synthesized books
      bar_half_spread_bps        synthetic half-spread in bar-only mode
      level_spacing_bps / level_size_growth / min_level_notional / max_book_levels
                                 synthetic depth beyond the real top of book
      stop_lock_buffer           extra quote locked for BUY stop-market orders (slippage headroom)
      max_orders_per_10s         order-rate limit (-1015)
      seed                       RNG seed for reproducible latency
    """
    initial_balances: Dict[str, float] = field(default_factory=lambda: {"USDT": 10_000.0})
    taker_fee_rate: float = 0.001
    maker_fee_rate: float = 0.001
    fee_in_received_asset: bool = True        # Binance default (no BNB discount)
    latency_median_ms: float = 40.0           # one-way, client <-> matching engine
    latency_sigma: float = 0.35               # lognormal shape: fat right tail
    max_quote_age_seconds: float = 5.0        # live quote freshness required to trade
    max_bar_quote_age_seconds: float = 150.0  # freshness when running on bars only
    bar_half_spread_bps: float = 1.0          # synthetic spread in bar-only mode
    level_spacing_bps: float = 0.5            # synthetic depth: distance between levels
    level_size_growth: float = 1.3            # synthetic depth: each level deeper is larger
    min_level_notional: float = 25_000.0      # floor for synthetic level size (quote ccy)
    max_book_levels: int = 50                 # beyond this a MARKET order's remainder expires
    stop_lock_buffer: float = 0.02            # extra quote locked for BUY stop-market orders
    max_orders_per_10s: int = 50
    seed: Optional[int] = None


@dataclass
class _Book:
    """Top of book per symbol. live=False when synthesized from a bar; depth_unit sizes synthetic levels."""
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float
    last: float
    updated: float            # time.monotonic() of last market data
    live: bool                # True = bookTicker/trade stream, False = synthesized from bars
    depth_unit: float = 0.0   # size of synthetic levels >= 1


@dataclass
class _SimOrder:
    """
    The simulator's own record of an order (the 'exchange side').
      fills / reported      all fills, and how many were already sent to the client
      triggered             False while a stop order waits for its trigger
      queue_ahead           resting size ahead of us at our price (None = unknown, conservative)
      lock_asset / lock_price / locked   funds reserved while resting
    """
    request: OrderRequest
    venue_order_id: str
    status: OrderStatus = OrderStatus.NEW
    filled: float = 0.0
    fills: List[Fill] = field(default_factory=list)
    reported: int = 0                      # fills already reported to the client
    triggered: bool = True                 # False for stops waiting for their trigger
    queue_ahead: Optional[float] = None    # size ahead of us at our price (None = unknown)
    lock_asset: str = ""
    lock_price: float = 0.0                # quote per unit locked (BUY) / 0 for SELL (locks base)
    locked: float = 0.0                    # amount of lock_asset still locked
    reason: str = ""

    @property
    def remaining(self) -> float:
        return max(self.request.quantity - self.filled, 0.0)

    @property
    def effective_type(self) -> OrderType:
        """What a triggered stop behaves as."""
        t = self.request.order_type
        if t in (OrderType.STOP_LOSS, OrderType.TAKE_PROFIT):
            return OrderType.MARKET
        if t in (OrderType.STOP_LOSS_LIMIT, OrderType.TAKE_PROFIT_LIMIT):
            return OrderType.LIMIT
        return t


class _Reject(Exception):
    """A Binance-style rejection message; becomes a REJECTED VenueOrderUpdate."""


class SimulatedVenue:
    """
    ExecutionVenue for ExecutionMode.SIM: matching engine on live mainnet quotes.

    Inputs (synchronous, wired in main.py):
        on_book_ticker(...)  <- BinanceQuoteStream     refresh book, fill crossed resting orders, queue position
        on_trade(...)        <- BinanceQuoteStream     trigger stops, queue consumption, trade-through fills
        on_bar(bar)          <- bus BarClosed (-15)    degraded mode only (no fresh live book)
    Port methods (async, with simulated latency):
        submit(req)   latency -> _accept (validate, lock funds, match) -> latency -> update
        cancel(...)   latency -> CANCELED (funds released) | None if unknown/terminal
        query(...)    full state incl. all fills | None
    Asynchronous reports: _emit() -> _wire (FIFO with latency) -> _dispatch_loop -> OMS handler.
    State:
        _books      Dict[symbol, _Book]
        _orders     Dict[coid, _SimOrder]   every order ever accepted
        _resting    Dict[symbol, Dict[coid, _SimOrder]]   working orders (incl. untriggered stops)
        _free / _locked   Dict[asset, float]   account balances
    Inspection: balances() -> {asset: {"free", "locked"}}
    Restart: restore_from_fills(fills) before start() (rehydration); fetch_balances() -> totals
    """
    venue_id = "BINANCE"          # simulates Binance Spot: same instruments and rules
    mode = ExecutionMode.SIM
    needs_polling = False         # pushes execution reports itself
    survives_restart = False      # in-memory: orders and balances die with the process

    def __init__(self, instruments: InstrumentRegistry, config: Optional[SimConfig] = None):
        self.instruments = instruments
        self.cfg = config or SimConfig()
        self._rng = random.Random(self.cfg.seed)
        self._books: Dict[str, _Book] = {}
        self._orders: Dict[str, _SimOrder] = {}            # every order ever accepted (query works after close)
        self._resting: Dict[str, Dict[str, _SimOrder]] = {}  # symbol -> working orders
        self._free: Dict[str, float] = dict(self.cfg.initial_balances)
        self._locked: Dict[str, float] = {}
        self._recent_orders: Deque[float] = deque()
        self._trade_ids = itertools.count(1)
        self._order_ids = itertools.count(1)
        self._run_tag = uuid.uuid4().hex[:6]               # ids stay unique across restarts
        self._handler: Optional[UpdateHandler] = None
        self._wire: asyncio.Queue[Tuple[float, VenueOrderUpdate]] = asyncio.Queue()
        self._dispatcher: Optional[asyncio.Task] = None

    # =================================================================
    # Lifecycle / port plumbing
    # =================================================================
    async def start(self) -> None:
        if self._dispatcher is None:
            self._dispatcher = asyncio.create_task(self._dispatch_loop(), name="sim-venue-wire")
        logger.info(f"SimulatedVenue started. Balances: {self.balances()}")

    async def stop(self) -> None:
        if self._dispatcher is not None:
            self._dispatcher.cancel()
            self._dispatcher = None
        logger.info(f"SimulatedVenue stopped. Final balances: {self.balances()}")

    def set_update_handler(self, handler: UpdateHandler) -> None:
        self._handler = handler

    def restore_from_fills(self, fills: List[Fill]) -> None:
        """
        Rehydration: the account = initial balances + every fill the system ever recorded for
        this venue (spot cash flows, fees in their own asset). All free: nothing rests after a restart.
        """
        if self._resting_count() or self._orders:
            raise RuntimeError("restore_from_fills() on a simulator that already has orders")
        free: Dict[str, float] = dict(self.cfg.initial_balances)
        for f in fills:
            spec = self.instruments.require(f.symbol)
            sign = f.side.sign
            free[spec.base_asset] = free.get(spec.base_asset, 0.0) + sign * f.quantity
            free[spec.quote_asset] = free.get(spec.quote_asset, 0.0) - sign * f.quantity * f.price
            free[f.fee_asset] = free.get(f.fee_asset, 0.0) - f.fee
        self._free = {a: q for a, q in free.items() if abs(q) > _EPS}
        self._locked = {}
        logger.info(f"SimulatedVenue restored from {len(fills)} recorded fill(s): {self.balances()}")

    def _resting_count(self) -> int:
        return sum(len(v) for v in self._resting.values())

    async def fetch_balances(self) -> Dict[str, float]:
        """Account totals (free + locked) per asset."""
        return {a: b["free"] + b["locked"] for a, b in self.balances().items()}

    def balances(self) -> Dict[str, Dict[str, float]]:
        assets = set(self._free) | set(self._locked)
        return {a: {"free": round(self._free.get(a, 0.0), 10), "locked": round(self._locked.get(a, 0.0), 10)}
                for a in sorted(assets)}

    # =================================================================
    # Market data inputs (synchronous; called from the quote stream / bar handler)
    # =================================================================
    def on_book_ticker(self, symbol: str, bid: float, bid_qty: float, ask: float, ask_qty: float) -> None:
        symbol = symbol.upper()
        if bid <= 0 or ask <= 0:
            return
        book = self._books.get(symbol)
        last = book.last if book else (bid + ask) / 2
        unit = max(bid_qty, ask_qty, self.cfg.min_level_notional / ((bid + ask) / 2))
        self._books[symbol] = book = _Book(bid=bid, bid_qty=bid_qty, ask=ask, ask_qty=ask_qty, last=last,
                                           updated=time.monotonic(), live=True, depth_unit=unit)
        self._match_on_book(symbol, book)

    def on_trade(self, symbol: str, price: float, qty: float, is_buyer_maker: bool) -> None:
        symbol = symbol.upper()
        book = self._books.get(symbol)
        if book is None:
            return   # need a book before trades mean anything
        book.last = price
        book.updated = time.monotonic()
        self._trigger_stops(symbol, price, price)
        self._match_on_trade(symbol, price, qty, is_buyer_maker)

    def on_bar(self, bar: Bar) -> None:
        """Degraded mode: only used for a symbol when no live stream is feeding it."""
        symbol = bar.symbol.upper()
        book = self._books.get(symbol)
        if book is not None and book.live and (time.monotonic() - book.updated) < self.cfg.max_quote_age_seconds:
            return
        half = bar.close * self.cfg.bar_half_spread_bps / 1e4
        tick = float(self.instruments.require(symbol).tick_size) if symbol in self.instruments else 0.0
        half = max(half, tick)
        unit = self.cfg.min_level_notional / bar.close
        book = _Book(bid=self._px(symbol, bar.close - half, -1), bid_qty=unit,
                     ask=self._px(symbol, bar.close + half, +1), ask_qty=unit,
                     last=bar.close, updated=time.monotonic(), live=False, depth_unit=unit)
        self._books[symbol] = book
        self._match_on_bar(symbol, bar)

    # =================================================================
    # ExecutionVenue port
    # =================================================================
    async def submit(self, request: OrderRequest) -> VenueOrderUpdate:
        await asyncio.sleep(self._latency())                 # client -> matching engine
        try:
            order = self._accept(request)
            update = self._report(order, mark_reported=True)
        except _Reject as e:
            logger.info(f"[SIM] Reject {request.client_order_id}: {e}")
            update = VenueOrderUpdate(client_order_id=request.client_order_id, status=OrderStatus.REJECTED,
                                      reason=str(e))
        await asyncio.sleep(self._latency())                 # matching engine -> client
        return update

    async def cancel(self, symbol: str, client_order_id: str) -> Optional[VenueOrderUpdate]:
        await asyncio.sleep(self._latency())
        order = self._orders.get(client_order_id)
        if order is None or order.status.is_terminal:
            await asyncio.sleep(self._latency())
            return None     # Binance: -2011 Unknown order sent.
        self._close(order, OrderStatus.CANCELED, "Canceled by client")
        update = self._report(order, mark_reported=True)
        await asyncio.sleep(self._latency())
        return update

    async def query(self, symbol: str, client_order_id: str) -> Optional[VenueOrderUpdate]:
        await asyncio.sleep(self._latency())
        order = self._orders.get(client_order_id)
        if order is None:
            return None
        return VenueOrderUpdate(client_order_id=client_order_id, status=order.status,
                                venue_order_id=order.venue_order_id, fills=tuple(order.fills),
                                cumulative_filled_quantity=order.filled, reason=order.reason)

    # =================================================================
    # Order acceptance (synchronous = atomic)
    # =================================================================
    def _accept(self, req: OrderRequest) -> _SimOrder:
        """
        The matching engine's front door (synchronous = atomic):
        duplicate id -> rate limit -> symbol -> fresh book -> mandatory params -> filters ->
        stops: reject if would trigger now, else lock funds and rest untriggered;
        MARKET: balance check, walk the book, expire any remainder;
        LIMIT: FOK pre-check, lock funds, take the marketable part, then IOC/FOK expire or rest.
        """
        sym = req.symbol.upper()
        if req.client_order_id in self._orders:
            raise _Reject("[-2010] Duplicate order sent.")
        self._check_rate_limit()
        if sym not in self.instruments:
            raise _Reject("[-1121] Invalid symbol.")
        book = self._fresh_book(sym)

        is_stop = req.order_type in STOP_TYPES
        market_like = req.order_type in (OrderType.MARKET, OrderType.STOP_LOSS, OrderType.TAKE_PROFIT)
        if req.order_type in LIMIT_PRICED_TYPES and req.price is None:
            raise _Reject("[-1102] Mandatory parameter 'price' was not sent.")
        if is_stop and req.stop_price is None:
            raise _Reject("[-1102] Mandatory parameter 'stopPrice' was not sent.")
        ref_price = (req.price if req.order_type in LIMIT_PRICED_TYPES
                     else req.stop_price if is_stop
                     else (book.ask if req.side is OrderSide.BUY else book.bid))
        err = self.instruments.validate(sym, req.quantity, ref_price, market=market_like)
        if err:
            raise _Reject(f"[-1013] {err}")

        order = _SimOrder(request=req, venue_order_id=f"SIM{self._run_tag}-{next(self._order_ids)}")
        spec = self.instruments.require(sym)

        if is_stop:
            if self._stop_triggered(req, book.last):
                raise _Reject("[-2010] Order would trigger immediately.")
            lock_px = req.price if req.price is not None else req.stop_price * (1 + self.cfg.stop_lock_buffer)  # type: ignore[operator]
            self._lock(order, spec.base_asset, spec.quote_asset, lock_px)
            order.triggered = False
            self._register(order)
            logger.info(f"[SIM] {req.client_order_id} stop resting: {req.side.name} {req.quantity} {sym} "
                        f"@ stop {req.stop_price}")
            return order

        if req.order_type is OrderType.MARKET:
            self._register(order)
            self._execute_market(order, book)
            return order

        # LIMIT
        if req.time_in_force is TimeInForce.FOK:
            plan = self._walk(sym, book, req.side, req.quantity, req.price)
            if sum(q for _, q in plan) + _EPS < req.quantity:
                order.status, order.reason = OrderStatus.EXPIRED, "FOK: insufficient liquidity at limit"
                self._orders[req.client_order_id] = order
                return order
        self._lock(order, spec.base_asset, spec.quote_asset, req.price)  # type: ignore[arg-type]
        self._register(order)
        self._execute_limit(order, book)
        return order

    def _register(self, order: _SimOrder) -> None:
        self._orders[order.request.client_order_id] = order
        self._resting.setdefault(order.request.symbol.upper(), {})[order.request.client_order_id] = order

    def _check_rate_limit(self) -> None:
        now = time.monotonic()
        while self._recent_orders and now - self._recent_orders[0] > 10.0:
            self._recent_orders.popleft()
        if len(self._recent_orders) >= self.cfg.max_orders_per_10s:
            raise _Reject(f"[-1015] Too many new orders; current limit is {self.cfg.max_orders_per_10s} "
                          f"orders per 10 SECOND.")
        self._recent_orders.append(now)

    def _fresh_book(self, sym: str) -> _Book:
        book = self._books.get(sym)
        if book is None:
            raise _Reject(f"[SIM] No market data for {sym} yet.")
        age = time.monotonic() - book.updated
        limit = self.cfg.max_quote_age_seconds if book.live else self.cfg.max_bar_quote_age_seconds
        if age > limit:
            raise _Reject(f"[SIM] Market data for {sym} is stale ({age:.1f}s old); refusing to fill blind.")
        return book

    # =================================================================
    # Balances
    # =================================================================
    def _lock(self, order: _SimOrder, base: str, quote: str, price: float) -> None:
        req = order.request
        if req.side is OrderSide.BUY:
            amount = req.quantity * price
            if self._free.get(quote, 0.0) + _EPS < amount:
                raise _Reject("[-2010] Account has insufficient balance for requested action.")
            order.lock_asset, order.lock_price = quote, price
        else:
            amount = req.quantity
            if self._free.get(base, 0.0) + _EPS < amount:
                raise _Reject("[-2010] Account has insufficient balance for requested action.")
            order.lock_asset = base
        self._free[order.lock_asset] = self._free.get(order.lock_asset, 0.0) - amount
        self._locked[order.lock_asset] = self._locked.get(order.lock_asset, 0.0) + amount
        order.locked = amount

    def _release(self, order: _SimOrder, amount: float) -> None:
        amount = min(amount, order.locked)
        if amount <= 0:
            return
        order.locked -= amount
        self._locked[order.lock_asset] = self._locked.get(order.lock_asset, 0.0) - amount
        self._free[order.lock_asset] = self._free.get(order.lock_asset, 0.0) + amount

    def _settle(self, order: _SimOrder, price: float, qty: float, maker: bool) -> None:
        """Moves balances for one execution (releasing the matching lock), charges the fee, records the Fill."""
        req = order.request
        spec = self.instruments.require(req.symbol)
        base, quote = spec.base_asset, spec.quote_asset
        rate = self.cfg.maker_fee_rate if maker else self.cfg.taker_fee_rate
        notional = price * qty

        if req.side is OrderSide.BUY:
            if order.locked > 0:
                self._release(order, qty * order.lock_price)
            self._free[quote] = self._free.get(quote, 0.0) - notional
            if self.cfg.fee_in_received_asset:
                fee, fee_asset = qty * rate, base
                self._free[base] = self._free.get(base, 0.0) + qty - fee
            else:
                fee, fee_asset = notional * rate, quote
                self._free[base] = self._free.get(base, 0.0) + qty
                self._free[quote] -= fee
            if self._free[quote] < -1e-6:
                logger.warning(f"[SIM] {quote} balance negative after stop slippage: {self._free[quote]:.6f}")
        else:
            if order.locked > 0:
                self._release(order, qty)
            self._free[base] = self._free.get(base, 0.0) - qty
            fee, fee_asset = notional * rate, quote
            self._free[quote] = self._free.get(quote, 0.0) + notional - fee

        order.filled = self.instruments.snap_qty(req.symbol, order.filled + qty)
        order.fills.append(Fill(
            client_order_id=req.client_order_id, strategy_id="", intent_id="", symbol=req.symbol,
            side=req.side, quantity=qty, price=price, fee=fee, fee_asset=fee_asset, is_maker=maker,
            exchange_trade_id=f"SIM-{self._run_tag}-T{next(self._trade_ids)}", timestamp=utc_now(),
        ))
        if order.remaining <= _EPS:
            self._close(order, OrderStatus.FILLED, "")
        else:
            order.status = OrderStatus.PARTIALLY_FILLED

    def _close(self, order: _SimOrder, status: OrderStatus, reason: str) -> None:
        order.status, order.reason = status, reason
        if order.locked > 0:
            self._release(order, order.locked)
        self._resting.get(order.request.symbol.upper(), {}).pop(order.request.client_order_id, None)

    # =================================================================
    # Taker execution (walks the book)
    # =================================================================
    def _walk(self, sym: str, book: _Book, side: OrderSide, qty: float,
              limit: Optional[float]) -> List[Tuple[float, float]]:
        """Plans (price, qty) executions against level 0 (real) and synthetic deeper levels."""
        plan: List[Tuple[float, float]] = []
        remaining = qty
        best = book.ask if side is OrderSide.BUY else book.bid
        top = book.ask_qty if side is OrderSide.BUY else book.bid_qty
        direction = 1 if side is OrderSide.BUY else -1
        prev_px = None
        for i in range(self.cfg.max_book_levels):
            px = self._px(sym, best * (1 + direction * i * self.cfg.level_spacing_bps / 1e4), direction)
            if prev_px is not None and px == prev_px:
                px = self._px(sym, px + direction * float(self.instruments.require(sym).tick_size), direction)
            prev_px = px
            if limit is not None and ((side is OrderSide.BUY and px > limit + _EPS) or
                                      (side is OrderSide.SELL and px < limit - _EPS)):
                break
            size = top if i == 0 else book.depth_unit * self.cfg.level_size_growth ** (i - 1)
            take = self.instruments.round_qty(sym, min(remaining, size))
            if take > 0:
                plan.append((px, take))
                remaining = self.instruments.snap_qty(sym, remaining - take)
            if remaining <= _EPS:
                break
        return plan

    def _consume_top(self, book: _Book, side: OrderSide, plan: List[Tuple[float, float]]) -> None:
        """Removes taken liquidity so it isn't available again until the next market-data update."""
        if not plan:
            return
        if side is OrderSide.BUY:
            if len(plan) == 1:
                book.ask_qty = max(book.ask_qty - plan[0][1], 0.0)
            else:
                book.ask, book.ask_qty = plan[-1][0], book.depth_unit * 0.5
        else:
            if len(plan) == 1:
                book.bid_qty = max(book.bid_qty - plan[0][1], 0.0)
            else:
                book.bid, book.bid_qty = plan[-1][0], book.depth_unit * 0.5

    def _execute_market(self, order: _SimOrder, book: _Book) -> None:
        req = order.request
        sym = req.symbol.upper()
        plan = self._walk(sym, book, req.side, order.remaining, None)
        spec = self.instruments.require(sym)
        if order.triggered and req.order_type is OrderType.MARKET:
            # Balance check at execution time (MARKET orders lock nothing up front).
            if req.side is OrderSide.BUY:
                cost = sum(p * q for p, q in plan) * (1 if self.cfg.fee_in_received_asset else 1 + self.cfg.taker_fee_rate)
                if self._free.get(spec.quote_asset, 0.0) + _EPS < cost:
                    self._orders.pop(req.client_order_id, None)
                    self._resting.get(sym, {}).pop(req.client_order_id, None)
                    raise _Reject("[-2010] Account has insufficient balance for requested action.")
            elif self._free.get(spec.base_asset, 0.0) + _EPS < req.quantity:
                self._orders.pop(req.client_order_id, None)
                self._resting.get(sym, {}).pop(req.client_order_id, None)
                raise _Reject("[-2010] Account has insufficient balance for requested action.")
        for px, q in plan:
            self._settle(order, px, q, maker=False)
        self._consume_top(book, req.side, plan)
        if not order.status.is_terminal:
            self._close(order, OrderStatus.EXPIRED, "Insufficient book depth; remainder expired")

    def _execute_limit(self, order: _SimOrder, book: _Book) -> None:
        req = order.request
        sym = req.symbol.upper()
        plan = self._walk(sym, book, req.side, order.remaining, req.price)
        for px, q in plan:
            self._settle(order, px, q, maker=False)
        self._consume_top(book, req.side, plan)
        if order.status.is_terminal:
            return
        if req.time_in_force in (TimeInForce.IOC, TimeInForce.FOK):
            self._close(order, OrderStatus.EXPIRED, f"{req.time_in_force.value}: remainder expired")
            return
        # Rests on the book: establish queue position.
        price = req.price
        if req.side is OrderSide.BUY:
            order.queue_ahead = 0.0 if price > book.bid + _EPS else (book.bid_qty if abs(price - book.bid) <= _EPS else None)
        else:
            order.queue_ahead = 0.0 if price < book.ask - _EPS else (book.ask_qty if abs(price - book.ask) <= _EPS else None)
        if order.status is OrderStatus.PARTIALLY_FILLED:
            order.queue_ahead = 0.0   # we were at the front: we just traded there

    # =================================================================
    # Passive matching
    # =================================================================
    def _working_limits(self, symbol: str) -> List[_SimOrder]:
        return [o for o in self._resting.get(symbol, {}).values()
                if o.triggered and o.effective_type is OrderType.LIMIT and not o.status.is_terminal]

    def _match_on_book(self, symbol: str, book: _Book) -> None:
        """Crossed resting orders fill (maker) up to displayed size; joins establish queue_ahead."""
        self._trigger_stops(symbol, book.last, book.last)
        for o in self._working_limits(symbol):
            p = o.request.price
            if o.request.side is OrderSide.BUY:
                if book.ask <= p + _EPS:                 # sellers crossed down to us
                    q = self.instruments.round_qty(symbol, min(o.remaining, book.ask_qty))
                    if q > 0:
                        self._settle(o, p, q, maker=True)
                        book.ask_qty = max(book.ask_qty - q, 0.0)
                        self._emit(o)
                elif o.queue_ahead is None and abs(book.bid - p) <= _EPS:
                    o.queue_ahead = book.bid_qty
                elif o.queue_ahead is None and book.bid < p - _EPS:
                    o.queue_ahead = 0.0
            else:
                if book.bid >= p - _EPS:
                    q = self.instruments.round_qty(symbol, min(o.remaining, book.bid_qty))
                    if q > 0:
                        self._settle(o, p, q, maker=True)
                        book.bid_qty = max(book.bid_qty - q, 0.0)
                        self._emit(o)
                elif o.queue_ahead is None and abs(book.ask - p) <= _EPS:
                    o.queue_ahead = book.ask_qty
                elif o.queue_ahead is None and book.ask > p + _EPS:
                    o.queue_ahead = 0.0

    def _match_on_trade(self, symbol: str, price: float, qty: float, is_buyer_maker: bool) -> None:
        """Opposite-side aggressor trades: AT our price consume queue_ahead first; THROUGH our price fill us fully."""
        for o in self._working_limits(symbol):
            p = o.request.price
            buy = o.request.side is OrderSide.BUY
            # Only aggressors on the OTHER side can hit a resting order.
            if buy != is_buyer_maker:
                continue
            through = price < p - _EPS if buy else price > p + _EPS
            at = abs(price - p) <= _EPS
            if through:
                fill_qty = o.remaining
            elif at and o.queue_ahead is not None:
                available = qty
                if o.queue_ahead > 0:
                    consumed = min(o.queue_ahead, available)
                    o.queue_ahead -= consumed
                    available -= consumed
                fill_qty = min(o.remaining, available)
            else:
                continue
            q = self.instruments.round_qty(symbol, fill_qty)
            if q > 0:
                self._settle(o, p, q, maker=True)
                self._emit(o)

    def _match_on_bar(self, symbol: str, bar: Bar) -> None:
        # Stops: gap through -> filled at the open; otherwise at the stop price +/- half-spread.
        self._trigger_stops(symbol, bar.low, bar.high, bar=bar)
        for o in self._working_limits(symbol):
            p = o.request.price
            if (o.request.side is OrderSide.BUY and bar.low < p - _EPS) or \
               (o.request.side is OrderSide.SELL and bar.high > p + _EPS):
                self._settle(o, p, o.remaining, maker=True)
                self._emit(o)

    # =================================================================
    # Stops
    # =================================================================
    @staticmethod
    def _stop_triggered(req: OrderRequest, price: float) -> bool:
        sp = req.stop_price
        if sp is None:
            return False
        loss = req.order_type in (OrderType.STOP_LOSS, OrderType.STOP_LOSS_LIMIT)
        if req.side is OrderSide.SELL:
            return price <= sp if loss else price >= sp
        return price >= sp if loss else price <= sp

    def _trigger_stops(self, symbol: str, low: float, high: float, bar: Optional[Bar] = None) -> None:
        book = self._books.get(symbol)
        if book is None:
            return
        for o in [o for o in self._resting.get(symbol, {}).values() if not o.triggered]:
            req = o.request
            # A price range triggers if either extreme satisfies the condition.
            if not (self._stop_triggered(req, low) or self._stop_triggered(req, high)):
                continue
            o.triggered = True
            logger.info(f"[SIM] Stop {req.client_order_id} triggered ({req.side.name} stop {req.stop_price}).")
            if bar is not None and o.effective_type is OrderType.MARKET:
                # Bar mode: fill at the stop, or at the open if the bar gapped through it.
                gapped = self._stop_triggered(req, bar.open)
                px = bar.open if gapped else req.stop_price
                slip = px * self.cfg.bar_half_spread_bps / 1e4
                px = self._px(symbol, px + slip if req.side is OrderSide.BUY else px - slip,
                              1 if req.side is OrderSide.BUY else -1)
                self._settle(o, px, o.remaining, maker=False)
            elif o.effective_type is OrderType.MARKET:
                self._execute_market(o, book)
            else:
                self._execute_limit(o, book)
            self._emit(o)

    # =================================================================
    # Reporting
    # =================================================================
    def _report(self, order: _SimOrder, mark_reported: bool) -> VenueOrderUpdate:
        new = order.fills[order.reported:]
        if mark_reported:
            order.reported = len(order.fills)
        return VenueOrderUpdate(client_order_id=order.request.client_order_id, status=order.status,
                                venue_order_id=order.venue_order_id, fills=tuple(new),
                                cumulative_filled_quantity=order.filled, reason=order.reason)

    def _emit(self, order: _SimOrder) -> None:
        """Queues an asynchronous execution report (delivered after wire latency, FIFO)."""
        update = self._report(order, mark_reported=True)
        self._wire.put_nowait((time.monotonic() + self._latency(), update))

    async def _dispatch_loop(self) -> None:
        while True:
            deliver_at, update = await self._wire.get()
            delay = deliver_at - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            if self._handler is not None:
                try:
                    await self._handler(update)
                except Exception:
                    logger.exception("[SIM] Update handler failed")

    # =================================================================
    # Helpers
    # =================================================================
    def _latency(self) -> float:
        median = self.cfg.latency_median_ms / 1000.0
        if median <= 0:
            return 0.0
        return self._rng.lognormvariate(math.log(median), self.cfg.latency_sigma)

    def _px(self, symbol: str, price: float, side_sign: int) -> float:
        """Tick-aligned price: asks/buys round up (+1), bids/sells round down (-1)."""
        if symbol not in self.instruments:
            return price
        return self.instruments.round_price(symbol, price, side_sign=-side_sign)
