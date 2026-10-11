"""Backfill correctness (no forming candles, replay flag, de-dup) and strategies refusing to trade replay."""

import asyncio
import time
import unittest
from datetime import datetime, timedelta, timezone

from bus.event_bus import EventBus
from connectors.binance.feed import BinanceFeed
from connectors.binance.rest import BinanceMarketDataREST
from data.processor import DataProcessor
from messages.events import BarClosed, IntentIssued, ParamsReady
from messages.model import Bar
from opm.lock import LockManager, LockState
from strategies.base import BaseStrategy
from tests.helpers import intent

MIN = 60_000


def kline(start_ms: int, close: float = 100.0):
    return [start_ms, "100", "101", "99", str(close), "1", start_ms + MIN - 1]


class FakeREST(BinanceMarketDataREST):
    def __init__(self, rows):
        super().__init__()
        self.rows = rows

    async def _get(self, path, params):
        return [r for r in self.rows if r[0] >= params["startTime"]]


def bar(i: int, replay=False) -> BarClosed:
    t = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i)
    return BarClosed(bar=Bar(symbol="BTCUSDT", venue="BINANCE", interval="1m", start_time=t,
                             close_time=t + timedelta(seconds=59.999), open=1, high=1, low=1, close=100 + i,
                             volume=1, is_closed=True), replay=replay)


class MarketDataTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_never_returns_the_forming_candle(self):
        """Regression (HIGH #7)."""
        now_minute = int(time.time() * 1000) // MIN * MIN
        rows = [kline(now_minute - 3 * MIN), kline(now_minute - 2 * MIN), kline(now_minute - MIN), kline(now_minute)]
        bars = await FakeREST(rows).fetch_bars("BTCUSDT", "1m", now_minute - 3 * MIN)
        self.assertEqual(len(bars), 3)
        self.assertTrue(all(b.close_time.timestamp() * 1000 < time.time() * 1000 for b in bars))

    async def test_feed_backfill_is_replay_and_deduplicated(self):
        now_minute = int(time.time() * 1000) // MIN * MIN
        rows = [kline(now_minute - k * MIN) for k in (5, 4, 3, 2, 1)]
        bus = EventBus()
        published = []
        bus.subscribe(BarClosed, published.append)

        async def latest(symbol, interval):
            return now_minute - 5 * MIN
        feed = BinanceFeed(bus, ["BTCUSDT"], rest=FakeREST(rows), get_latest_time_cb=latest)
        await feed._backfill_gaps()
        await feed._backfill_gaps()                       # reconnect: nothing new -> nothing republished
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        self.assertEqual(len(published), 4)
        self.assertTrue(all(e.replay for e in published))

    async def test_processor_discards_duplicates_and_propagates_replay(self):
        bus = EventBus()
        proc = DataProcessor(bus, {"BTCUSDT"}, max_lookback=3)
        out = []
        bus.subscribe(ParamsReady, out.append)
        for e in (bar(0), bar(1), bar(2, replay=True), bar(2), bar(1), bar(3)):
            await bus.publish(e)
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        self.assertEqual([p.params.latest_bar.close for p in out], [102, 103])
        self.assertEqual([p.replay for p in out], [True, False])
        self.assertEqual(proc.get_latest_market_state("BTCUSDT")["close"], 103)
        self.assertIn("sma_50", proc.get_latest_market_state("BTCUSDT"))   # _latest_params now populated

    async def test_strategy_ignores_replay_and_issues_one_intent_under_a_burst(self):
        """Regression (HIGH #6): a burst of bars yields at most one entry while the lock is pending."""
        bus = EventBus()
        lock = LockManager(bus)
        proc = DataProcessor(bus, {"BTCUSDT"}, max_lookback=2)

        class Always(BaseStrategy):
            async def analyze_opportunity(self, params):
                await asyncio.sleep(0)   # inference yields to the loop
                return intent(strategy_id=self.strategy_id)
        Always(bus, lock, "TEST_STRAT", ["BTCUSDT"])
        intents = []
        bus.subscribe(IntentIssued, intents.append)
        for i in range(10):
            await bus.publish(bar(i, replay=True))
        for i in range(10, 40):
            await bus.publish(bar(i))
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        self.assertEqual(len(intents), 1)
        # The decision price is stamped for slippage analysis, and the lock still owns that intent.
        self.assertIsNotNone(intents[0].intent.reference_price)
        self.assertIs(lock.state("TEST_STRAT", "BTCUSDT"), LockState.PENDING_ENTRY)


if __name__ == "__main__":
    unittest.main()
