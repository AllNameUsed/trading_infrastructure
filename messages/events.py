"""
messages/events.py

The typed verbs of the trading system. Every message traversing the Central
Event Bus is an instance of an Event subclass defined here.

Rules:
1. Immutable all the way down: events are frozen AND only carry frozen
   payloads (OrderSnapshot, Position, Fill, Intent...). Never put a live,
   mutable aggregate on the bus.
2. Self-prioritizing: each event defines its default scheduling priority.
   Within one priority the bus is strictly FIFO (it assigns a sequence number
   at publish time), so ordering is deterministic and replayable.
3. Causality over urgency: every event about the SAME order or position uses
   the SAME priority (HIGH). Priority must never let a Fill overtake its own
   Accept, or a PositionClosed overtake the Fill that caused it.
4. Every Command (IntentIssued) ends in exactly one terminal outcome: either an
   order is created (OrderEvent stream) or IntentRejected is published.
5. Zero outward dependencies: only imports the standard library and messages.model.

Catalogue (publisher -> consumers, default priority):
  BarClosed            BinanceFeed -> DataProcessor, store(save_bar), SimulatedVenue(on_bar)   NORMAL
  ParamsReady          DataProcessor -> OPM (order -10), strategies (0), telemetry (100)       NORMAL
  IntentIssued         strategies / OPM -> Journal (-200), Lock (-100, audit), OMS             NORMAL / HIGH for exits
  IntentRejected       OMS -> Lock, Journal                                                    HIGH
  CancelIssued         (any decision maker) -> OMS                                             HIGH
  Order* (OrderEvent)  OMS -> Portfolio (-50, fills), Lock (-40), recorders                    HIGH
  Position*            PortfolioManager -> Lock (-100), OPM                                    HIGH
  RiskBreach           OMS (vanished orders), Rehydrator -> Journal, KillSwitch (HALT_ENTRIES) CRITICAL
  KillSwitch           KillSwitchController (every level change) -> Journal                    CRITICAL
  SignalGenerated, ResizeRequest, Heartbeat                                                    PLANNED
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import Optional

from messages.model import (
    Bar,
    CancelIntent,
    ExecutionMode,
    Fill,
    HaltLevel,
    Intent,
    MarketParams,
    OrderSnapshot,
    Position,
    RejectStage,
    Signal,
    new_id,
    utc_now,
)


class EventPriority(IntEnum):
    """
    Priority levels for the EventBus PriorityQueue.
    Lower numerical values indicate higher processing urgency.
    """
    CRITICAL = 0    # KillSwitch, RiskBreach
    HIGH = 1        # Everything about live orders/positions; exit intents
    NORMAL = 2      # BarClosed, ParamsReady, entry intents
    LOW = 3         # Heartbeats, telemetry


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    """
    Base class for all system events (frozen, slotted, keyword-only).
      timestamp   local UTC creation time - informational, NEVER used for ordering
      event_id    unique id (journal / tracing)
      priority    default per subclass; a publisher may override per instance
                  (the OPM publishes exit IntentIssued with HIGH)
    Queue ordering is (priority, bus sequence number) - see bus/event_bus.py.
    Subscribing to a base class (Event, OrderEvent) receives all subclasses.
    """
    timestamp: datetime = field(default_factory=utc_now)
    event_id: str = field(default_factory=new_id)
    priority: EventPriority = EventPriority.NORMAL


# =====================================================================
# 1. Market Data & Pipeline Events
# =====================================================================

@dataclass(frozen=True, slots=True, kw_only=True)
class BarClosed(Event):
    """
    Published by the feed when a candle finalizes.
    `replay=True` marks bars recovered by REST backfill: they update market
    state but must never trigger trading decisions.
    """
    bar: Bar
    replay: bool = False
    priority: EventPriority = EventPriority.NORMAL


@dataclass(frozen=True, slots=True, kw_only=True)
class ParamsReady(Event):
    """
    Published by data/processor.py after computing rolling features.
    Consumed by the OPM (first) and strategies (second). `replay` is inherited
    from the BarClosed that produced it.
    """
    params: MarketParams
    replay: bool = False
    priority: EventPriority = EventPriority.NORMAL


# =====================================================================
# 2. Strategy & Decision Events
# =====================================================================

@dataclass(frozen=True, slots=True, kw_only=True)
class SignalGenerated(Event):
    """Emitted by a Strategy; consumed by risk/allocator.py (future)."""
    signal: Signal
    priority: EventPriority = EventPriority.NORMAL


@dataclass(frozen=True, slots=True, kw_only=True)
class ResizeRequest(Event):
    """Emitted by the OPM when scaling is triggered on an active trade."""
    strategy_id: str
    symbol: str
    target_quantity: float
    reason: str
    priority: EventPriority = EventPriority.HIGH


@dataclass(frozen=True, slots=True, kw_only=True)
class IntentIssued(Event):
    """
    Command: a decision component wants an order. Entry intents ride NORMAL,
    exit intents should be published with priority=HIGH.
    """
    intent: Intent
    priority: EventPriority = EventPriority.NORMAL


@dataclass(frozen=True, slots=True, kw_only=True)
class IntentRejected(Event):
    """
    Terminal outcome of an Intent that never became an order (registry,
    routing, validation, risk, or an exception while processing it).
    The PositionLock rolls back on this.
    """
    intent: Intent
    stage: RejectStage
    rule_name: str
    reason: str
    priority: EventPriority = EventPriority.HIGH


@dataclass(frozen=True, slots=True, kw_only=True)
class CancelIssued(Event):
    """A request to pull a resting order. Routed to the OMS."""
    intent: CancelIntent
    priority: EventPriority = EventPriority.HIGH


# =====================================================================
# 3. Order Lifecycle Events (published ONLY by the OMS)
# =====================================================================

@dataclass(frozen=True, slots=True, kw_only=True)
class OrderEvent(Event):
    """Base class: subscribe to OrderEvent to receive every lifecycle event."""
    order: OrderSnapshot
    priority: EventPriority = EventPriority.HIGH


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderAccepted(OrderEvent):
    """The venue confirmed placement (OrderStatus.NEW)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderPartiallyFilled(OrderEvent):
    """One execution that did not complete the order. `order` reflects state AFTER this fill."""
    fill: Fill


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderFilled(OrderEvent):
    """The execution that completed the order (OrderStatus.FILLED)."""
    fill: Fill


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderCanceled(OrderEvent):
    """The venue confirmed cancellation."""
    reason: str = "Canceled"


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderExpired(OrderEvent):
    """IOC/FOK remainder expired, or venue-side expiry."""
    reason: str = "Expired"


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRejected(OrderEvent):
    """The venue rejected the order submission."""
    reason: str


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderStatusUnknown(OrderEvent):
    """
    The submit outcome is unknown (timeout / 5xx). The order stays WORKING:
    locks stay engaged and the OMS reconciles against the venue until resolved.
    """
    reason: str


# =====================================================================
# 4. Position & Portfolio Accounting Events (published ONLY by PortfolioManager)
# =====================================================================

@dataclass(frozen=True, slots=True, kw_only=True)
class PositionOpened(Event):
    """Ledger transitioned 0 -> non-zero."""
    position: Position
    priority: EventPriority = EventPriority.HIGH


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionChanged(Event):
    """Ledger size/price changed but stayed open (partial fills, scaling)."""
    position: Position
    delta_quantity: float
    priority: EventPriority = EventPriority.HIGH


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionClosed(Event):
    """Ledger is flat. `realized_pnl` is the round trip, net of fees."""
    position: Position
    realized_pnl: float
    exit_reason: str
    priority: EventPriority = EventPriority.HIGH


# =====================================================================
# 5. Risk, Supervisor & Operational Events (Critical / Out-of-Band)
# =====================================================================

@dataclass(frozen=True, slots=True, kw_only=True)
class RiskBreach(Event):
    """
    A genuine breach (unresolved order state, ledger divergence).
    NOT used for routine pre-trade rejections - those are IntentRejected.
    The kill switch answers every breach with HALT_ENTRIES on `mode` (all modes when None).
    """
    rule_name: str
    symbol: Optional[str]
    detail: str
    mode: Optional[ExecutionMode] = None
    priority: EventPriority = EventPriority.CRITICAL


@dataclass(frozen=True, slots=True, kw_only=True)
class KillSwitch(Event):
    """
    The halt level of one execution mode changed (published only by the
    KillSwitchController, risk/kill_switch.py): an escalation, or an operator reset
    back to NONE. Informational - the controller itself enforces the halt.
      halt_id   the episode id the operator must quote in config/kill_switch.ini to reset
    """
    mode: ExecutionMode
    level: HaltLevel
    previous_level: HaltLevel
    halt_id: str
    source: str
    reason: str
    priority: EventPriority = EventPriority.CRITICAL


@dataclass(frozen=True, slots=True, kw_only=True)
class Heartbeat(Event):
    """Emitted periodically by long-running background tasks."""
    source: str
    sequence: int
    priority: EventPriority = EventPriority.LOW
