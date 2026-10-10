"""
ops/journal.py

Audit journal. Passively subscribes to decisions and outcomes and records them
through the store's single writer (it never opens its own connection and never
writes order state - the OMS owns the orders table, the portfolio owns positions).

Records: every intent, every intent rejection (risk_ledger), genuine risk breaches.
"""

import logging

from messages.events import IntentIssued, IntentRejected, RiskBreach

logger = logging.getLogger("Journal")


class DatabaseJournal:
    """
    Append-only audit trail (the future training set for exit models).

    Subscribes:  IntentIssued (order=-200: row exists before the OMS resolves it),
                 IntentRejected, RiskBreach
    Writes via:  SqliteStore.log_intent / log_risk_event (never its own connection)
    Does NOT write orders, fills or positions - their owners (OMS, Portfolio) do.
    """
    def __init__(self, bus, store):
        self.store = store
        # order=-200: the intent row exists before the OMS resolves its outcome.
        bus.subscribe(IntentIssued, self.log_intent, order=-200)
        bus.subscribe(IntentRejected, self.log_rejection)
        bus.subscribe(RiskBreach, self.log_risk_breach)

    async def log_intent(self, event: IntentIssued) -> None:
        await self.store.log_intent(event.intent)

    async def log_rejection(self, event: IntentRejected) -> None:
        await self.store.log_risk_event(
            rule_name=event.rule_name, symbol=event.intent.symbol,
            action=f"REJECT_INTENT:{event.stage.name}",
            detail=f"{event.intent.strategy_id} {event.intent.kind.name} {event.intent.side.name}: {event.reason}",
            timestamp=event.timestamp,
        )

    async def log_risk_breach(self, event: RiskBreach) -> None:
        logger.critical(f"[RISK BREACH] {event.rule_name} {event.symbol}: {event.detail}")
        await self.store.log_risk_event(rule_name=event.rule_name, symbol=event.symbol, action="BREACH",
                                        detail=event.detail, timestamp=event.timestamp)
