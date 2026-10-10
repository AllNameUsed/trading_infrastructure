"""
execution/oms.py

The Order Management System.

Responsibilities (and invariants):
  1. Single writer of order state. Venues RETURN updates; only the OMS mutates
     the `Order` aggregate and only the OMS publishes order lifecycle events.
     Events carry frozen `OrderSnapshot`s, never the live aggregate.
  2. Every Intent ends in exactly one terminal outcome: an order is created,
     or `IntentRejected` is published (registry, routing, validation, risk,
     exception). Nothing is ever dropped silently - this is what lets the
     PositionLock roll back deterministically.
  3. No network I/O on the bus loop. The bus handler validates, rounds,
     risk-checks and durably persists the order (write-ahead), then hands the
     venue call to a background task and returns.
  4. Unknown outcomes are never treated as rejections. A timed-out submit
     becomes UNKNOWN: the order stays working (locks stay engaged) and the OMS
     reconciles it against the venue by client order id until resolved.
  5. Fills are de-duplicated by exchange trade id, so the same execution
     reported by a submit response, a poll and a push can only count once.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional, Set

from connectors.base import ExecutionVenue, VenueUnknownOutcome
from execution.order_ids import generate_client_order_id
from execution.router import VenueRouter
from messages.events import (
    CancelIssued,
    IntentIssued,
    IntentRejected,
    OrderAccepted,
    OrderCanceled,
    OrderEvent,
    OrderExpired,
    OrderFilled,
    OrderPartiallyFilled,
    OrderRejected,
    OrderStatusUnknown,
    RiskBreach,
)
from messages.instruments import InstrumentRegistry
from messages.model import (
    LIMIT_PRICED_TYPES,
    STOP_TYPES,
    ExecutionMode,
    Fill,
    Intent,
    OrderRequest,
    OrderSnapshot,
    OrderStatus,
    OrderType,
    Position,
    RejectStage,
    VenueOrderUpdate,
    utc_now,
)

logger = logging.getLogger("OMS")

_QTY_EPS = 1e-12


@dataclass(slots=True)
class Order:
    """
    The living execution aggregate. Owned exclusively by the OMS; never published
    (events carry `snapshot()`).
      status / filled_quantity / average_price / cumulative_fee   running totals
      venue_order_id      the exchange's id once known
      version             +1 per published transition (see OrderSnapshot.version)
      seen_trade_ids      fill de-duplication set
      not_found_count     consecutive "order does not exist" answers while polling
    """
    client_order_id: str
    strategy_id: str
    mode: ExecutionMode
    intent: Intent
    request: OrderRequest
    status: OrderStatus = OrderStatus.PENDING_SUBMIT
    filled_quantity: float = 0.0
    average_price: float = 0.0
    cumulative_fee: float = 0.0
    venue_order_id: Optional[str] = None
    version: int = 0
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    seen_trade_ids: Set[str] = field(default_factory=set)
    not_found_count: int = 0

    def snapshot(self) -> OrderSnapshot:
        return OrderSnapshot(
            client_order_id=self.client_order_id, strategy_id=self.strategy_id, mode=self.mode,
            intent=self.intent, request=self.request, status=self.status,
            filled_quantity=self.filled_quantity, average_price=self.average_price,
            cumulative_fee=self.cumulative_fee, venue_order_id=self.venue_order_id,
            version=self.version, created_at=self.created_at, updated_at=self.updated_at,
        )


class OrderManagementSystem:
    """
    OMS facade: the order-lifecycle state machine and single writer of order state.

    Subscribes:  IntentIssued -> on_intent_issued, CancelIssued -> on_cancel_issued
                 (plus venue push reports via ExecutionVenue.set_update_handler -> _on_venue_push)
    Publishes:   IntentRejected, OrderAccepted / OrderPartiallyFilled / OrderFilled /
                 OrderCanceled / OrderExpired / OrderRejected / OrderStatusUnknown, RiskBreach
    Depends on:  VenueRouter (ports), PreTradeRiskManager, InstrumentRegistry, SqliteStore,
                 market_view(symbol) -> dict          (DataProcessor read model, for risk)
                 position_view(sid, sym) -> Position  (PortfolioManager read model, for exits)

    State:
        _orders      Dict[client_order_id, Order]   WORKING orders only (removed when terminal)
        _tasks       Set[Task]                      in-flight submit/cancel venue calls
        _poll_task   Task                           _poll_loop
        _breached    Set[str]                       orders already reported as vanished
    API:
        start() / stop(timeout)                     start poller / cancel it and await in-flight calls
        working_orders() -> [OrderSnapshot]

    Intent pipeline (_process_intent, on the bus loop, no network I/O):
        1 registry  2 routing  3 instrument  4 exits: reduce-only clamp + cancel resting entries
        5 round to lot/tick  6 risk.validate  7 write-ahead (durable upsert)  8 spawn _submit task
        any failure -> _reject() -> IntentRejected(stage, rule, reason)
    Venue results (_apply_update, the ONLY mutator): de-dup fills by trade id, decide the
    effective status (venue wins unless fills are missing), one event per fill with its own
    snapshot, then a status event; persist fills + order; drop terminal orders from _orders.
    Unknown outcomes: _mark_unknown -> UNKNOWN; _poll_loop queries until the venue answers;
    "not found" 3x after unknown_resolution_seconds -> REJECTED.
    """

    def __init__(
        self,
        bus,
        store,
        router: VenueRouter,
        strategy_registry: Dict[str, ExecutionMode],
        risk_manager,
        instruments: InstrumentRegistry,
        market_view: Callable[[str], dict],
        position_view: Callable[[str, str], Optional[Position]],
        poll_interval: float = 3.0,
        unknown_resolution_seconds: float = 30.0,
    ):
        self.bus = bus
        self.store = store
        self.router = router
        self.registry = strategy_registry
        self.risk_manager = risk_manager
        self.instruments = instruments
        self.market_view = market_view
        self.position_view = position_view
        self.poll_interval = poll_interval
        self.unknown_resolution_seconds = unknown_resolution_seconds

        self._orders: Dict[str, Order] = {}        # WORKING orders only (non-terminal)
        self._tasks: Set[asyncio.Task] = set()
        self._poll_task: Optional[asyncio.Task] = None
        self._breached: Set[str] = set()

        self.bus.subscribe(IntentIssued, self.on_intent_issued)
        self.bus.subscribe(CancelIssued, self.on_cancel_issued)
        for venue in self.router.all():
            venue.set_update_handler(self._on_venue_push)

        logger.info(f"OMS initialized. Registered strategies: {sorted(self.registry)}")

    # =================================================================
    # Lifecycle
    # =================================================================
    async def start(self) -> None:
        self._poll_task = asyncio.create_task(self._poll_loop(), name="oms-poller")

    async def stop(self, timeout: float = 10.0) -> None:
        if self._poll_task:
            self._poll_task.cancel()
        if self._tasks:
            logger.info(f"Waiting up to {timeout}s for {len(self._tasks)} in-flight venue call(s)...")
            await asyncio.wait(list(self._tasks), timeout=timeout)

    def working_orders(self) -> List[OrderSnapshot]:
        return [o.snapshot() for o in self._orders.values()]

    # =================================================================
    # Intent intake (runs on the bus loop: no network I/O here)
    # =================================================================
    async def on_intent_issued(self, event: IntentIssued) -> None:
        """Bus entry point. Guarantees a terminal outcome even if processing raises."""
        intent = event.intent
        try:
            await self._process_intent(intent)
        except Exception as e:
            logger.exception(f"[OMS] Unhandled error processing intent {intent.intent_id}")
            await self._reject(intent, RejectStage.EXCEPTION, "OMS", f"{type(e).__name__}: {e}")

    async def _process_intent(self, intent: Intent) -> None:
        """Steps 1-8 of the intent pipeline (see class docstring). Returns after spawning the submit."""
        sid, symbol = intent.strategy_id, intent.symbol
        logger.info(f"[OMS] Intent {intent.kind.name} {intent.side.name} {intent.quantity} {symbol} from {sid}")

        # 1. Registration
        mode = self.registry.get(sid)
        if mode is None:
            return await self._reject(intent, RejectStage.REGISTRY, "Registry", f"Strategy {sid} is not registered")

        # 2. Routing
        venue = self.router.venue_for(mode)
        if venue is None:
            return await self._reject(intent, RejectStage.ROUTING, "Router", f"No venue configured for mode {mode.name}")

        # 3. Instrument
        spec = self.instruments.get(symbol)
        if spec is None:
            return await self._reject(intent, RejectStage.VALIDATION, "Instrument", f"Unknown instrument {symbol}")

        # 4. Exit sanity: an exit can only reduce an existing position, never flip/open one.
        quantity = abs(intent.quantity)
        if intent.kind.is_exit:
            pos = self.position_view(sid, symbol)
            if pos is None or self.instruments.is_qty_dust(symbol, pos.quantity):
                return await self._reject(intent, RejectStage.VALIDATION, "ReduceOnly", "No open position to reduce")
            if intent.side is not pos.side.opposite:  # type: ignore[union-attr]
                return await self._reject(intent, RejectStage.VALIDATION, "ReduceOnly",
                                          f"Exit side {intent.side.name} does not reduce a {pos.side.name} position")  # type: ignore[union-attr]
            quantity = min(quantity, abs(pos.quantity))
            self._cancel_working_entries(sid, symbol)

        # 5. Rounding to exchange filters
        is_market = intent.order_type in (OrderType.MARKET, OrderType.STOP_LOSS, OrderType.TAKE_PROFIT)
        qty = self.instruments.round_qty(symbol, quantity, market=is_market)
        if qty <= 0:
            return await self._reject(intent, RejectStage.VALIDATION, "LotSize",
                                      f"Quantity {intent.quantity} rounds to zero")
        price = None
        if intent.order_type in LIMIT_PRICED_TYPES:
            if intent.price is None:
                return await self._reject(intent, RejectStage.VALIDATION, "Price", "Limit order without price")
            price = self.instruments.round_price(symbol, intent.price, side_sign=intent.side.sign)
        stop_price = None
        if intent.order_type in STOP_TYPES:
            if intent.stop_price is None:
                return await self._reject(intent, RejectStage.VALIDATION, "Price", "Stop order without stop price")
            stop_price = self.instruments.round_price(symbol, intent.stop_price)

        client_order_id = generate_client_order_id(sid, intent.kind)
        request = OrderRequest(
            client_order_id=client_order_id, symbol=symbol, venue=venue.venue_id,
            side=intent.side, order_type=intent.order_type, quantity=qty,
            price=price, stop_price=stop_price, time_in_force=intent.time_in_force,
        )

        # 6. Pre-trade risk
        verdict = self.risk_manager.validate(intent, request, self.market_view(symbol) or {})
        if not verdict.approved:
            return await self._reject(intent, RejectStage.RISK, verdict.rule_name, verdict.detail)

        # 7. Write-ahead: the order is durable BEFORE anything leaves the process.
        order = Order(client_order_id=client_order_id, strategy_id=sid, mode=mode,
                      intent=intent, request=request)
        try:
            await self.store.upsert_order(order.snapshot(), durable=True)
        except Exception as e:
            return await self._reject(intent, RejectStage.EXCEPTION, "WriteAhead", f"Could not persist order: {e}")
        self._orders[client_order_id] = order
        await self.store.resolve_intent(intent.intent_id, "ORDERED", client_order_id)

        # 8. Hand the network call to a background task and return to the bus.
        logger.info(f"[OMS] {client_order_id} -> {mode.name} venue ({request.side.name} {qty} {symbol})")
        self._spawn(self._submit(order, venue), f"submit-{client_order_id}")

    async def _reject(self, intent: Intent, stage: RejectStage, rule_name: str, reason: str) -> None:
        """Publishes the terminal IntentRejected and records the outcome on the intent row."""
        logger.warning(f"[OMS REJECT] {intent.strategy_id} {intent.kind.name} {intent.symbol} "
                       f"at {stage.name}/{rule_name}: {reason}")
        await self.bus.publish(IntentRejected(intent=intent, stage=stage, rule_name=rule_name, reason=reason))
        try:
            await self.store.resolve_intent(intent.intent_id, f"REJECTED:{stage.name}", f"{rule_name}: {reason}")
        except Exception:
            logger.exception("Failed to journal intent rejection")

    # =================================================================
    # Cancels
    # =================================================================
    async def on_cancel_issued(self, event: CancelIssued) -> None:
        """Cancels a working order, only on behalf of the strategy that owns it."""
        req = event.intent
        order = self._orders.get(req.client_order_id)
        if order is None:
            logger.warning(f"[OMS CANCEL] {req.client_order_id} is not a working order; ignoring.")
            return
        if order.strategy_id != req.strategy_id:
            logger.error(f"[OMS CANCEL] {req.strategy_id} tried to cancel {req.client_order_id} "
                         f"owned by {order.strategy_id}; refused.")
            return
        self._request_cancel(order)

    def _cancel_working_entries(self, strategy_id: str, symbol: str) -> None:
        """Exits imply 'cancel my resting entries first' so they cannot add exposure mid-exit."""
        for order in list(self._orders.values()):
            if (order.strategy_id == strategy_id and order.request.symbol == symbol
                    and order.intent.kind.is_entry and order.status in (OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED)):
                logger.info(f"[OMS] Exit requested: cancelling working entry {order.client_order_id}")
                self._request_cancel(order)

    def _request_cancel(self, order: Order) -> None:
        venue = self.router.venue_for(order.mode)
        if venue is None:
            return
        self._spawn(self._cancel(order, venue), f"cancel-{order.client_order_id}")

    async def _cancel(self, order: Order, venue: ExecutionVenue) -> None:
        """Background task: cancel; if the venue doesn't recognise it, query and apply the truth."""
        try:
            update = await venue.cancel(order.request.symbol, order.client_order_id)
        except Exception as e:
            logger.error(f"[OMS CANCEL] {order.client_order_id} cancel failed: {e}; reconciling.")
            update = None
        if update is None:
            try:
                update = await venue.query(order.request.symbol, order.client_order_id)
            except Exception as e:
                logger.error(f"[OMS CANCEL] reconcile query failed for {order.client_order_id}: {e}")
                return
        if update is not None:
            await self._apply_update(order, update)

    # =================================================================
    # Venue interaction (background tasks)
    # =================================================================
    async def _submit(self, order: Order, venue: ExecutionVenue) -> None:
        """Background task: send the order; unknown outcomes (incl. adapter bugs) go to reconciliation."""
        try:
            update = await venue.submit(order.request)
        except VenueUnknownOutcome as e:
            await self._mark_unknown(order, str(e))
            return
        except Exception as e:
            # We cannot prove the request never left the process: treat as unknown, reconcile.
            logger.exception(f"[OMS] Venue adapter raised on submit of {order.client_order_id}")
            await self._mark_unknown(order, f"Adapter error: {type(e).__name__}: {e}")
            return
        await self._apply_update(order, update)

    async def _mark_unknown(self, order: Order, reason: str) -> None:
        """PENDING_SUBMIT -> UNKNOWN (publishes OrderStatusUnknown). Locks stay held."""
        if order.status.is_terminal or self._orders.get(order.client_order_id) is not order:
            return
        logger.critical(f"[OMS] {order.client_order_id} outcome UNKNOWN ({reason}). Locks held; reconciling.")
        await self._apply_update(order, VenueOrderUpdate(
            client_order_id=order.client_order_id, status=OrderStatus.UNKNOWN, reason=reason))

    async def _on_venue_push(self, update: VenueOrderUpdate) -> None:
        """Execution reports pushed by a venue (simulator matching, user-data streams)."""
        order = self._orders.get(update.client_order_id)
        if order is None:
            logger.debug(f"[OMS] Push for non-working order {update.client_order_id} ignored.")
            return
        await self._apply_update(order, update)

    async def _poll_loop(self) -> None:
        """Polls working orders on venues that cannot push, and every UNKNOWN order everywhere."""
        while True:
            try:
                await asyncio.sleep(self.poll_interval)
                for order in list(self._orders.values()):
                    venue = self.router.venue_for(order.mode)
                    if venue is None or order.status is OrderStatus.PENDING_SUBMIT:
                        continue   # submit still in flight - its own task will report
                    if order.status is OrderStatus.UNKNOWN or venue.needs_polling:
                        await self._poll_one(order, venue)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("[OMS] Poll loop iteration failed")

    async def _poll_one(self, order: Order, venue: ExecutionVenue) -> None:
        """Query one order; apply the answer, or count 'not found' toward REJECTED / RiskBreach."""
        try:
            update = await venue.query(order.request.symbol, order.client_order_id)
        except Exception as e:
            logger.warning(f"[OMS] Query failed for {order.client_order_id}: {e}")
            return
        if self._orders.get(order.client_order_id) is not order:
            return  # resolved while we awaited
        if update is not None:
            order.not_found_count = 0
            await self._apply_update(order, update)
            return

        # Venue says the order does not exist.
        order.not_found_count += 1
        age = (utc_now() - order.created_at).total_seconds()
        if order.status is OrderStatus.UNKNOWN and order.not_found_count >= 3 and age >= self.unknown_resolution_seconds:
            await self._apply_update(order, VenueOrderUpdate(
                client_order_id=order.client_order_id, status=OrderStatus.REJECTED,
                reason="Not found at venue after timeout - never reached the matching engine"))
        elif order.status is not OrderStatus.UNKNOWN and order.client_order_id not in self._breached:
            self._breached.add(order.client_order_id)
            await self.bus.publish(RiskBreach(
                rule_name="OrderVanished", symbol=order.request.symbol,
                detail=f"Working order {order.client_order_id} ({order.status.name}) not found at venue"))

    # =================================================================
    # The order state machine (single writer)
    # =================================================================
    async def _apply_update(self, order: Order, upd: VenueOrderUpdate) -> None:
        """
        The single mutator of order state. All mutation happens before the first
        await, so concurrent venue reports cannot interleave inside a transition.
        """
        if order.status.is_terminal:
            logger.debug(f"[OMS] Update for terminal order {order.client_order_id} ignored ({upd.status.name}).")
            return

        events: List[OrderEvent] = []
        new_fills: List[Fill] = []
        prev_status = order.status
        if upd.venue_order_id:
            order.venue_order_id = upd.venue_order_id

        # --- 1. Apply new fills as deltas (de-duplicated) ---
        for raw in upd.fills:
            if raw.exchange_trade_id in order.seen_trade_ids:
                continue
            order.seen_trade_ids.add(raw.exchange_trade_id)
            fill = dataclasses.replace(raw, client_order_id=order.client_order_id, strategy_id=order.strategy_id,
                                       intent_id=order.intent.intent_id, symbol=order.request.symbol,
                                       side=order.request.side)
            notional = order.average_price * order.filled_quantity + fill.price * fill.quantity
            order.filled_quantity += fill.quantity
            order.average_price = notional / order.filled_quantity if order.filled_quantity > 0 else 0.0
            order.cumulative_fee += fill.fee
            new_fills.append(fill)

        # --- 2. Decide the effective status (the venue wins, unless fills are missing) ---
        target = upd.status
        venue_cum = upd.cumulative_filled_quantity
        fills_missing = venue_cum is not None and venue_cum > order.filled_quantity + _QTY_EPS
        complete = order.filled_quantity + _QTY_EPS >= order.request.quantity
        if target is OrderStatus.FILLED and (fills_missing or not complete):
            target = OrderStatus.PARTIALLY_FILLED if order.filled_quantity > 0 else OrderStatus.NEW
        elif target in (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED) and fills_missing:
            target = OrderStatus.PARTIALLY_FILLED if order.filled_quantity > 0 else OrderStatus.NEW
        elif target is OrderStatus.NEW and order.filled_quantity > 0:
            target = OrderStatus.PARTIALLY_FILLED
        elif target is OrderStatus.UNKNOWN and prev_status is not OrderStatus.PENDING_SUBMIT:
            target = prev_status   # never downgrade known state to unknown
        if complete and target not in (OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED):
            target = OrderStatus.FILLED

        # --- 3. One event per fill, each with its own frozen snapshot ---
        now = utc_now()
        running_qty, running_notional, running_fee = (
            order.filled_quantity - sum(f.quantity for f in new_fills),
            order.average_price * order.filled_quantity - sum(f.price * f.quantity for f in new_fills),
            order.cumulative_fee - sum(f.fee for f in new_fills),
        )
        for i, fill in enumerate(new_fills):
            running_qty += fill.quantity
            running_notional += fill.price * fill.quantity
            running_fee += fill.fee
            is_last = i == len(new_fills) - 1
            status_i = target if is_last and target is OrderStatus.FILLED else OrderStatus.PARTIALLY_FILLED
            order.version += 1
            snap = dataclasses.replace(
                order.snapshot(), status=status_i, filled_quantity=running_qty,
                average_price=running_notional / running_qty if running_qty > 0 else 0.0,
                cumulative_fee=running_fee, updated_at=now)
            events.append(OrderFilled(order=snap, fill=fill) if status_i is OrderStatus.FILLED
                          else OrderPartiallyFilled(order=snap, fill=fill))

        # --- 4. Status-only transition event ---
        order.status = target
        order.updated_at = now
        if target is not OrderStatus.FILLED and (target is not prev_status or not new_fills):
            event_cls = {
                OrderStatus.NEW: OrderAccepted,
                OrderStatus.CANCELED: OrderCanceled,
                OrderStatus.EXPIRED: OrderExpired,
                OrderStatus.REJECTED: OrderRejected,
                OrderStatus.UNKNOWN: OrderStatusUnknown,
            }.get(target)
            if event_cls is not None and target is not prev_status:
                order.version += 1
                snap = order.snapshot()
                if event_cls is OrderAccepted:
                    events.append(OrderAccepted(order=snap))
                else:
                    events.append(event_cls(order=snap, reason=upd.reason or target.name))
        if not events and target is prev_status:
            return  # nothing changed (e.g. a poll with no news)

        if target.is_terminal:
            self._orders.pop(order.client_order_id, None)

        logger.info(f"[OMS STATE] {order.client_order_id}: {prev_status.name} -> {target.name} "
                    f"(filled {order.filled_quantity}/{order.request.quantity} @ {order.average_price:.8g})"
                    + (f" | {upd.reason}" if upd.reason else ""))

        # --- 5. Persist, then publish (mutation above is complete; nothing can interleave it) ---
        for fill in new_fills:
            await self.store.record_fill(fill)
        await self.store.upsert_order(order.snapshot())
        for ev in events:
            await self.bus.publish(ev)

    # =================================================================
    # Helpers
    # =================================================================
    def _spawn(self, coro, name: str) -> None:
        """Runs a venue call as a tracked background task (awaited by stop())."""
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.error(f"[OMS] Task {name} crashed: {t.exception()!r}")
        task.add_done_callback(_done)
