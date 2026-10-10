"""
risk/pretrade.py

Chain of Responsibility for pre-trade risk validation.
Evaluates the rounded OrderRequest (and the Intent that produced it) before
the OMS sends anything to a venue.

Design notes:
  * Rules are pure and synchronous. They return nothing / raise RiskRejection;
    the orchestrator returns a RiskVerdict. Publishing is the OMS's job (as a
    terminal IntentRejected) - a routine rejection is NOT a RiskBreach.
  * Rules FAIL CLOSED: if a rule needs a reference price and none is
    available, the order is rejected rather than waved through.
  * Exits are treated differently from entries: an exit reduces risk, so only
    rules that declare `applies_to_exits = True` are run on them. Blocking a
    stop-out with a random or notional rule is how small losses become large.
"""

from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from messages.model import Intent, OrderRequest, OrderType

logger = logging.getLogger("PreTradeRisk")


class RiskRejection(Exception):
    """Raised by a RiskRule; caught by PreTradeRiskManager and turned into a RiskVerdict."""
    def __init__(self, rule_name: str, detail: str):
        self.rule_name = rule_name
        self.detail = detail
        super().__init__(detail)


@dataclass(frozen=True, slots=True)
class RiskVerdict:
    """Result of the chain: approved, or (rule_name, detail) of the FIRST rule that fired."""
    approved: bool
    rule_name: str = ""
    detail: str = ""


APPROVED = RiskVerdict(True)


def reference_price(request: OrderRequest, market_state: dict) -> Optional[float]:
    """
    Limit price for priced orders, otherwise market_state["close"] (the DataProcessor's
    latest closed bar). None means "unknown" - rules must then FAIL CLOSED.
    """
    if request.order_type is not OrderType.MARKET and request.price:
        return request.price
    px = market_state.get("close")
    return float(px) if px else None


# =====================================================================
# 1. The Abstract Base Rule
# =====================================================================

class RiskRule(ABC):
    """
    One link of the Chain of Responsibility. Subclass, implement evaluate(), add to
    the rules list in main.py - that is the whole job of adding a risk check.
      applies_to_exits   False (default) -> skipped for CLOSE/SCALE_OUT/FLATTEN intents
      name               used as rule_name in IntentRejected and the risk_ledger
      evaluate(intent, request, market_state)   raise RiskRejection to block; return to pass
    `request` is the ROUNDED OrderRequest; `market_state` is DataProcessor.get_latest_market_state().
    """
    applies_to_exits: bool = False

    @property
    def name(self) -> str:
        return type(self).__name__

    @abstractmethod
    def evaluate(self, intent: Intent, request: OrderRequest, market_state: dict) -> None:
        """Raise RiskRejection if the order violates the rule."""


# =====================================================================
# 2. Concrete Risk Gates
# =====================================================================

class FatFingerRule(RiskRule):
    """Absolute notional ceiling per order. Applies to exits too (a 100x exit is still a bug)."""
    applies_to_exits = True

    def __init__(self, max_notional_value: float):
        self.max_notional = max_notional_value

    def evaluate(self, intent: Intent, request: OrderRequest, market_state: dict) -> None:
        price = reference_price(request, market_state)
        if price is None or price <= 0:
            raise RiskRejection(self.name, f"No valid reference price for {request.symbol}; failing closed.")
        notional = request.quantity * price
        if notional > self.max_notional:
            raise RiskRejection(self.name, f"Notional ${notional:,.2f} exceeds hard limit ${self.max_notional:,.2f}.")


class MinNotionalRule(RiskRule):
    """Rejects orders the exchange would reject anyway (wastes rate limit)."""

    def __init__(self, min_notional: float = 5.00):
        self.min_notional = min_notional

    def evaluate(self, intent: Intent, request: OrderRequest, market_state: dict) -> None:
        price = reference_price(request, market_state)
        if price is None:
            raise RiskRejection(self.name, f"No reference price for {request.symbol}; failing closed.")
        notional = request.quantity * price
        if notional < self.min_notional:
            raise RiskRejection(self.name, f"Notional ${notional:.2f} below minimum ${self.min_notional:.2f}.")


class DuplicateIntentRule(RiskRule):
    """Blocks the 'machine gun' bug: identical entry intents for the same strategy+symbol within a window."""

    def __init__(self, window_seconds: float = 2.0):
        self.window = window_seconds
        self._last: Dict[Tuple[str, str], Tuple[tuple, float]] = {}

    def evaluate(self, intent: Intent, request: OrderRequest, market_state: dict) -> None:
        key = (intent.strategy_id, intent.symbol)
        footprint = (intent.kind, intent.side, request.order_type, request.quantity, request.price)
        now = time.monotonic()
        last = self._last.get(key)
        if last and last[0] == footprint and (now - last[1]) < self.window:
            raise RiskRejection(self.name, f"Identical intent for {key} within {self.window}s.")
        self._last[key] = (footprint, now)


class RandomMockRejectionRule(RiskRule):
    """Soak-test rule: randomly rejects a fraction of ENTRY orders to exercise rejection routing."""

    def __init__(self, probability: float = 0.20):
        self.probability = probability

    def evaluate(self, intent: Intent, request: OrderRequest, market_state: dict) -> None:
        if random.random() < self.probability:
            raise RiskRejection(self.name, f"Simulated risk rejection for {request.symbol}.")


# =====================================================================
# 3. The Orchestrator
# =====================================================================

class PreTradeRiskManager:
    """
    Runs the rule chain for the OMS (step 6 of the intent pipeline).
      rules                                   evaluated in list order
      validate(intent, request, state) -> RiskVerdict   first rejection wins; exits run
                                              only rules with applies_to_exits=True
    Pure and synchronous: no bus, no I/O. Publishing the outcome is the OMS's job.
    """
    def __init__(self, rules: List[RiskRule]):
        self.rules = rules

    def validate(self, intent: Intent, request: OrderRequest, market_state: dict) -> RiskVerdict:
        """First rejection wins. Pure: no I/O, no publishing."""
        for rule in self.rules:
            if intent.kind.is_exit and not rule.applies_to_exits:
                continue
            try:
                rule.evaluate(intent, request, market_state)
            except RiskRejection as e:
                logger.warning(f"RISK REJECTION [{e.rule_name}] {intent.strategy_id} {request.symbol}: {e.detail}")
                return RiskVerdict(False, e.rule_name, e.detail)
        return APPROVED
