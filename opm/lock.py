"""
opm/lock.py

PositionLock: the state machine that hands control of one (strategy_id, symbol)
between the Strategy (entries) and the Open Position Manager (exits).

    IDLE ──try_begin_entry──▶ PENDING_ENTRY ──PositionOpened──▶ HOLDING
     ▲                          │ (entry dies with no fill)        │
     └──────────────────────────┘                     try_begin_exit
     ▲                                                             ▼
     └────────── PositionClosed (and no working orders) ──── PENDING_EXIT
                                   (exit dies, position remains) ──▶ HOLDING

Why acquisition is a synchronous compare-and-set:
  The lock used to transition when the bus *dispatched* IntentIssued. Between
  a strategy publishing an intent and that dispatch, any number of further
  ParamsReady events could reach the strategy, all seeing IDLE (e.g. a burst
  of backfilled bars) - the duplicate-order race the lock exists to prevent.
  `try_begin_entry` / `try_begin_exit` check AND set in one synchronous call
  (no await in between), so in asyncio exactly one caller can win.

Release rules (never guesses, only reacts to terminal facts):
  * IntentRejected for the pending intent             -> settle
  * terminal order event for the pending intent       -> settle
  * PositionOpened                                    -> HOLDING (unless an exit is pending)
  * PositionClosed                                    -> settle
  * OrderStatusUnknown                                -> stays pending (the order may have filled)
  settle() derives the resting state from facts:
    has position -> HOLDING; working orders remain -> PENDING_ENTRY; else IDLE.

Position facts come from the PortfolioManager (`position_view`), the single
writer of positions. It handles fill events BEFORE the lock (order=-50 vs -40)
and applies them synchronously, so when a filled entry order settles the lock,
the ledger already shows the position: PENDING_ENTRY -> HOLDING directly, with
no transient IDLE in between.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Dict, Optional, Set, Tuple

from messages.events import IntentIssued, IntentRejected, OrderEvent, PositionClosed, PositionOpened
from messages.model import Intent, Position

logger = logging.getLogger("PositionLock")

Key = Tuple[str, str]


class LockState(Enum):
    """Who may act on one (strategy_id, symbol): the strategy (IDLE), the OPM (HOLDING), or nobody (PENDING_*)."""
    IDLE = auto()           # No position. Strategy armed. OPM on ice.
    PENDING_ENTRY = auto()  # Entry in flight. Strategy iced. OPM on ice.
    HOLDING = auto()        # Position open. Strategy iced. OPM armed.
    PENDING_EXIT = auto()   # Exit in flight. Strategy iced. OPM on ice.


@dataclass(slots=True)
class _Slot:
    """
    Per-key lock record.
      pending_intent_id   the intent whose outcome will release PENDING_*
      working             non-terminal client_order_ids for this key (could still add exposure)
      has_position        fallback position flag when no position_view is wired
    """
    state: LockState = LockState.IDLE
    pending_intent_id: Optional[str] = None
    working: Set[str] = field(default_factory=set)   # client_order_ids not yet terminal
    has_position: bool = False


class LockManager:
    """
    The PositionLock table: one state machine per (strategy_id, symbol).

    Subscribes:  IntentIssued (-100, audit only), IntentRejected (-100), OrderEvent (-40),
                 PositionOpened (-100), PositionClosed (-100)
    Publishes:   nothing (pure observer + synchronous gate)
    Depends on:  position_view(sid, sym) -> Position|None   (PortfolioManager.get_position)

    State:
        _slots: Dict[(strategy_id, symbol), _Slot]
    API (all synchronous - check-and-set cannot be interleaved by another coroutine):
        state(sid, sym) -> LockState
        can_strategy_enter(sid, sym)    cheap pre-check (IDLE?) before running inference
        can_opm_manage(sid, sym)        HOLDING?
        try_begin_entry(intent) -> bool IDLE & no working orders & no position -> PENDING_ENTRY
        try_begin_exit(intent) -> bool  HOLDING -> PENDING_EXIT
    Caller contract: acquire with try_begin_* IMMEDIATELY before publishing the
    IntentIssued (no await in between); a False return means drop the intent.
    """
    def __init__(self, bus, position_view: Optional[Callable[[str, str], Optional[Position]]] = None):
        """
        :param position_view: PortfolioManager.get_position. Without it the lock falls
                              back to tracking PositionOpened/Closed events itself.
        """
        self.bus = bus
        self.position_view = position_view
        self._slots: Dict[Key, _Slot] = {}

        bus.subscribe(IntentIssued, self._on_intent, order=-100)
        bus.subscribe(IntentRejected, self._on_intent_rejected, order=-100)
        # order=-40: AFTER the PortfolioManager (-50) has applied the fill to the ledger.
        bus.subscribe(OrderEvent, self._on_order_event, order=-40)
        bus.subscribe(PositionOpened, self._on_position_opened, order=-100)
        bus.subscribe(PositionClosed, self._on_position_closed, order=-100)

    # =================================================================
    # Query / acquire APIs (synchronous: check-and-set cannot interleave)
    # =================================================================
    def state(self, strategy_id: str, symbol: str) -> LockState:
        """Current state; unknown keys are IDLE."""
        slot = self._slots.get((strategy_id, symbol))
        return slot.state if slot else LockState.IDLE

    def can_strategy_enter(self, strategy_id: str, symbol: str) -> bool:
        """Cheap pre-check so strategies can skip work. NOT sufficient on its own - use try_begin_entry."""
        return self.state(strategy_id, symbol) is LockState.IDLE

    def can_opm_manage(self, strategy_id: str, symbol: str) -> bool:
        """True only while HOLDING (no exit already in flight)."""
        return self.state(strategy_id, symbol) is LockState.HOLDING

    def try_begin_entry(self, intent: Intent) -> bool:
        """Atomic IDLE -> PENDING_ENTRY for this intent. False = someone else holds the key."""
        slot = self._slot((intent.strategy_id, intent.symbol))
        if slot.state is not LockState.IDLE or slot.working or self._has_position((intent.strategy_id, intent.symbol), slot):
            return False
        self._transition((intent.strategy_id, intent.symbol), slot, LockState.PENDING_ENTRY, "entry acquired")
        slot.pending_intent_id = intent.intent_id
        return True

    def try_begin_exit(self, intent: Intent) -> bool:
        """Atomic HOLDING -> PENDING_EXIT for this intent."""
        slot = self._slot((intent.strategy_id, intent.symbol))
        if slot.state is not LockState.HOLDING:
            return False
        self._transition((intent.strategy_id, intent.symbol), slot, LockState.PENDING_EXIT, "exit acquired")
        slot.pending_intent_id = intent.intent_id
        return True

    # =================================================================
    # Observers
    # =================================================================
    def _on_intent(self, event: IntentIssued) -> None:
        """Audit only: every intent must have acquired the lock first."""
        intent = event.intent
        slot = self._slot((intent.strategy_id, intent.symbol))
        if slot.pending_intent_id != intent.intent_id:
            logger.error(f"[LOCK] Intent {intent.intent_id} ({intent.kind.name} {intent.strategy_id}/{intent.symbol}) "
                         f"was issued without acquiring the lock (state={slot.state.name}).")

    def _on_intent_rejected(self, event: IntentRejected) -> None:
        intent = event.intent
        key = (intent.strategy_id, intent.symbol)
        slot = self._slot(key)
        if slot.pending_intent_id == intent.intent_id:
            self._settle(key, slot, f"intent rejected at {event.stage.name}")

    def _on_order_event(self, event: OrderEvent) -> None:
        """Tracks working orders; a terminal event for the pending intent settles the lock."""
        snap = event.order
        key = (snap.strategy_id, snap.request.symbol)
        slot = self._slot(key)
        if snap.status.is_terminal:
            slot.working.discard(snap.client_order_id)
            if slot.pending_intent_id == snap.intent.intent_id:
                self._settle(key, slot, f"order {snap.status.name}")
        else:
            slot.working.add(snap.client_order_id)

    def _on_position_opened(self, event: PositionOpened) -> None:
        """Exposure exists -> HOLDING (fail-safe even if it arrives unexpectedly while IDLE)."""
        pos = event.position
        key = (pos.strategy_id, pos.symbol)
        slot = self._slot(key)
        slot.has_position = True
        if slot.state is LockState.IDLE:
            logger.error(f"[LOCK] Position opened on {key} while IDLE (unexpected fill). Forcing HOLDING.")
        if slot.state is not LockState.PENDING_EXIT:
            if slot.state is LockState.PENDING_ENTRY:
                slot.pending_intent_id = None
            self._transition(key, slot, LockState.HOLDING, "position opened")

    def _on_position_closed(self, event: PositionClosed) -> None:
        """Flat -> settle, unless the exit order is still working (its terminal event settles)."""
        pos = event.position
        key = (pos.strategy_id, pos.symbol)
        slot = self._slot(key)
        slot.has_position = False
        if slot.state is LockState.PENDING_EXIT and slot.pending_intent_id is not None:
            # The exit order's own terminal event settles us (it may still be working).
            return
        self._settle(key, slot, "position closed")

    # =================================================================
    # Internals
    # =================================================================
    def _slot(self, key: Key) -> _Slot:
        slot = self._slots.get(key)
        if slot is None:
            slot = self._slots[key] = _Slot()
        return slot

    def _has_position(self, key: Key, slot: _Slot) -> bool:
        if self.position_view is not None:
            pos = self.position_view(*key)
            return pos is not None and not pos.is_flat
        return slot.has_position

    def _settle(self, key: Key, slot: _Slot, why: str) -> None:
        """Ends the pending operation; derives HOLDING / PENDING_ENTRY / IDLE from facts."""
        slot.pending_intent_id = None
        if self._has_position(key, slot):
            target = LockState.HOLDING
        elif slot.working:
            target = LockState.PENDING_ENTRY   # orders that could still create exposure
        else:
            target = LockState.IDLE
        self._transition(key, slot, target, why)

    @staticmethod
    def _transition(key: Key, slot: _Slot, target: LockState, why: str) -> None:
        if slot.state is not target:
            logger.info(f"[LOCK] {key}: {slot.state.name} -> {target.name} ({why})")
            slot.state = target
