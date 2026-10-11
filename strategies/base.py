"""
strategies/base.py

Abstract Base Class for all strategies. Handles the bus subscription, the
replay guard and lock acquisition, so concrete strategies only implement
`analyze_opportunity`.
"""

import dataclasses
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from messages.events import IntentIssued, ParamsReady
from messages.model import Intent, MarketParams
from opm.lock import LockManager

logger = logging.getLogger("Strategy")


class BaseStrategy(ABC):
    """
    Template for all strategies (GoF Strategy + Template Method).

    Subscribes:  ParamsReady (order=0, after the OPM)
    Publishes:   IntentIssued(kind=OPEN)
    Depends on:  LockManager
    on_params_ready flow: own symbol? -> not replay? -> lock IDLE? -> analyze_opportunity()
        -> stamp reference_price (the close it decided on) -> lock.try_begin_entry(intent)
        (atomic, AFTER the await) -> publish
    Subclass contract:
        analyze_opportunity(params) -> Intent | None    the whole job; must be stateless
        w.r.t. market data (read params.features, never keep price arrays).
    To add a strategy: subclass, register its id in config/settings.STRATEGY_REGISTRY
    with an ExecutionMode, construct it in main.py.
    """
    def __init__(self, bus, lock_manager: LockManager, strategy_id: str, symbols: List[str]):
        self.bus = bus
        self.lock = lock_manager
        self.strategy_id = strategy_id
        self.symbols = {s.upper() for s in symbols}
        # order=0: runs after the OPM (order=-10) has evaluated exits for this bar.
        bus.subscribe(ParamsReady, self.on_params_ready, order=0)
        logger.info(f"Strategy [{self.strategy_id}] initialized and listening for {self.symbols}.")

    async def on_params_ready(self, event: ParamsReady) -> None:
        params = event.params
        symbol = params.symbol

        if symbol not in self.symbols:
            return
        # Never trade on backfilled history.
        if event.replay:
            return
        # Cheap pre-check to skip inference while in a trade.
        if not self.lock.can_strategy_enter(self.strategy_id, symbol):
            return

        intent = await self.analyze_opportunity(params)
        if intent is None:
            return
        if intent.reference_price is None:
            # The price the decision was made on: journaled on the trade for slippage analysis.
            intent = dataclasses.replace(intent, reference_price=params.latest_bar.close)

        # Atomic acquisition AFTER the await: state may have changed while we were inferring.
        if not self.lock.try_begin_entry(intent):
            logger.info(f"[{self.strategy_id}] Signal on {symbol} dropped: lock no longer IDLE.")
            return
        logger.info(f"[{self.strategy_id}] Signal on {symbol}: issuing {intent.side.name} intent.")
        await self.bus.publish(IntentIssued(intent=intent))

    @abstractmethod
    async def analyze_opportunity(self, params: MarketParams) -> Optional[Intent]:
        """Return an entry Intent (IntentKind.OPEN) if a trade is desired, else None."""
