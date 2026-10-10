"""
opm/manager.py

The Open Position Manager (OPM).
1. Mirrors positions from the PortfolioManager's Position events (it never
   builds or mutates positions itself - the portfolio is the single writer).
2. On every live ParamsReady, evaluates exits for positions in that symbol.
3. Issues CLOSE intents - only after winning the PositionLock
   (try_begin_exit), and at HIGH priority so exits outrank queued entries.
"""

import logging
import random
from typing import Callable, Dict, Optional, Tuple

from messages.events import EventPriority, IntentIssued, ParamsReady, PositionChanged, PositionClosed, PositionOpened
from messages.model import Intent, IntentKind, MarketParams, OrderType, Position
from opm.lock import LockManager

logger = logging.getLogger("OpenPositionManager")


class OpenPositionManager:
    """
    OPM facade: the sole authority for exits on open positions.

    Subscribes:  PositionOpened / PositionChanged / PositionClosed (mirror),
                 ParamsReady (order=-10: before strategies)
    Publishes:   IntentIssued(kind=CLOSE, MARKET, priority=HIGH)
    Depends on:  LockManager (try_begin_exit), strategy_evaluators (strategy-directed exits)

    State:
        _active_positions  Dict[(sid, sym), Position]   latest snapshot per open ledger
        _bars_held         Dict[(sid, sym), int]        live+replay bars since open (ExitContext seed)
    Decision flow per live ParamsReady, per open position on that symbol:
        skip if replay or lock not HOLDING
        ROUTE A  strategy in evaluators -> ask the strategy (placeholder RNG today)
        ROUTE B  otherwise -> _independent_risk_check (placeholder RNG; trailing stop /
                 take-profit / time-stop engines go here - see opm/engines.py, planned)
        exit -> build CLOSE intent for |quantity| -> lock.try_begin_exit -> publish
    """
    def __init__(
        self,
        bus,
        lock_manager: LockManager,
        strategy_evaluators: Optional[Dict[str, Callable[[Position, MarketParams], bool]]] = None,
    ):
        self.bus = bus
        self.lock = lock_manager
        self.evaluators = strategy_evaluators or {}
        self._active_positions: Dict[Tuple[str, str], Position] = {}
        self._bars_held: Dict[Tuple[str, str], int] = {}

        bus.subscribe(PositionOpened, self.on_position_opened)
        bus.subscribe(PositionChanged, self.on_position_changed)
        bus.subscribe(PositionClosed, self.on_position_closed)
        # order=-10: the OPM sees every bar BEFORE strategies (order=0): exits outrank entries.
        bus.subscribe(ParamsReady, self.on_params_ready, order=-10)

    # =================================================================
    # 1. Position mirror
    # =================================================================
    def on_position_opened(self, event: PositionOpened) -> None:
        key = (event.position.strategy_id, event.position.symbol)
        self._active_positions[key] = event.position
        self._bars_held[key] = 0

    def on_position_changed(self, event: PositionChanged) -> None:
        key = (event.position.strategy_id, event.position.symbol)
        self._active_positions[key] = event.position

    def on_position_closed(self, event: PositionClosed) -> None:
        key = (event.position.strategy_id, event.position.symbol)
        self._active_positions.pop(key, None)
        held = self._bars_held.pop(key, 0)
        logger.info(f"[OPM] {key} closed after {held} bar(s). Realized {event.realized_pnl:+.4f}.")

    # =================================================================
    # 2. Exit evaluation
    # =================================================================
    async def on_params_ready(self, event: ParamsReady) -> None:
        symbol = event.params.symbol
        for key, pos in list(self._active_positions.items()):
            if key[1] != symbol:
                continue
            self._bars_held[key] = self._bars_held.get(key, 0) + 1
            if event.replay:
                continue  # never trade on backfilled history
            if not self.lock.can_opm_manage(pos.strategy_id, pos.symbol):
                continue

            should_exit, exit_reason = False, ""
            if pos.strategy_id in self.evaluators:
                # ROUTE A: strategy-directed exit (placeholder RNG kept from the soak test).
                if random.random() > 0.05:  # self.evaluators[pos.strategy_id](pos, event.params)
                    should_exit, exit_reason = True, "Strategy advised an Exit"
            else:
                # ROUTE B: independent OPM evaluation.
                should_exit, exit_reason = self._independent_risk_check(pos, event.params)

            if should_exit:
                await self._trigger_exit(pos, exit_reason)

    def _independent_risk_check(self, pos: Position, params: MarketParams) -> Tuple[bool, str]:
        """Placeholder: trailing stop / take-profit / time-stop math goes here."""
        if random.random() < 0.10:
            return True, "OPM Independent RNG Check triggered."
        return False, ""

    async def _trigger_exit(self, pos: Position, reason: str) -> None:
        if pos.side is None:
            return
        intent = Intent(
            strategy_id=pos.strategy_id,
            symbol=pos.symbol,
            venue=pos.venue,
            kind=IntentKind.CLOSE,
            side=pos.side.opposite,
            order_type=OrderType.MARKET,
            quantity=abs(pos.quantity),
            reason=reason,
        )
        if not self.lock.try_begin_exit(intent):
            return
        logger.info(f"[OPM] Exit for {pos.strategy_id}/{pos.symbol}: {reason}")
        await self.bus.publish(IntentIssued(intent=intent, priority=EventPriority.HIGH))
