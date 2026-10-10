"""
strategies/mock_strat.py

Soak-test strategy: random entries to exercise the full order pipeline.
Note: on a SPOT venue, SELL entries require holding the base asset, so the
simulator (correctly) rejects them with "insufficient balance" unless you
seed a base-asset balance in SIM_INITIAL_BALANCES.
"""

import random

from messages.model import Intent, IntentKind, MarketParams, OrderSide, OrderType
from strategies.base import BaseStrategy


class MockStrat(BaseStrategy):
    """Soak test: 5% chance per live bar of a random-side MARKET entry of 0.01 base units."""

    async def analyze_opportunity(self, params: MarketParams) -> Intent | None:
        # 5% chance per bar to "find an edge".
        if random.random() > 0.05:
            return None
        return Intent(
            strategy_id=self.strategy_id,
            symbol=params.symbol,
            venue="BINANCE",
            kind=IntentKind.OPEN,
            side=random.choice([OrderSide.BUY, OrderSide.SELL]),
            order_type=OrderType.MARKET,
            quantity=0.01,
            reason="Random soak test entry",
        )
