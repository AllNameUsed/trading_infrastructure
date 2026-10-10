"""The simulated venue's market model, tested directly through its port."""

import time
import unittest
from datetime import datetime, timedelta, timezone

from connectors.sim.venue import SimConfig, SimulatedVenue
from messages.model import Bar, OrderRequest, OrderSide, OrderStatus, OrderType, TimeInForce, VenueOrderUpdate
from tests.helpers import make_instruments

_ids = iter(range(1, 10_000))


def req(side=OrderSide.BUY, qty=0.01, order_type=OrderType.LIMIT, price=None, stop=None, tif=TimeInForce.GTC):
    return OrderRequest(client_order_id=f"T.O.{next(_ids)}", symbol="BTCUSDT", venue="BINANCE", side=side,
                        order_type=order_type, quantity=qty, price=price, stop_price=stop, time_in_force=tif)


class SimVenueTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.venue = SimulatedVenue(make_instruments(), SimConfig(
            latency_median_ms=0, seed=1, initial_balances={"USDT": 100_000.0, "BTC": 1.0}))
        self.pushed: list[VenueOrderUpdate] = []

        async def handler(u):
            self.pushed.append(u)
        self.venue.set_update_handler(handler)
        await self.venue.start()
        self.venue.on_book_ticker("BTCUSDT", 50_000.00, 2.0, 50_000.01, 2.0)

    async def asyncTearDown(self):
        await self.venue.stop()

    async def flush_wire(self):
        import asyncio
        for _ in range(5):
            await asyncio.sleep(0)

    async def test_limit_rests_then_fills_after_queue_ahead_is_consumed(self):
        u = await self.venue.submit(req(price=50_000.00))            # joins the bid behind 2.0 BTC
        self.assertIs(u.status, OrderStatus.NEW)
        self.assertEqual(self.venue.balances()["USDT"]["locked"], 500.0)
        self.venue.on_trade("BTCUSDT", 50_000.00, 1.5, is_buyer_maker=True)   # 1.5 of 2.0 ahead consumed
        await self.flush_wire()
        self.assertEqual(self.pushed, [])
        self.venue.on_trade("BTCUSDT", 50_000.00, 0.505, is_buyer_maker=True)  # 0.5 ahead + 0.005 to us
        await self.flush_wire()
        self.assertIs(self.pushed[-1].status, OrderStatus.PARTIALLY_FILLED)
        self.assertAlmostEqual(self.pushed[-1].fills[0].quantity, 0.005)
        self.assertTrue(self.pushed[-1].fills[0].is_maker)
        self.venue.on_trade("BTCUSDT", 49_999.00, 0.0001, is_buyer_maker=True)  # trades THROUGH our price
        await self.flush_wire()
        self.assertIs(self.pushed[-1].status, OrderStatus.FILLED)
        self.assertEqual(self.venue.balances()["USDT"]["locked"], 0.0)

    async def test_buyer_aggressor_trades_do_not_fill_resting_bids(self):
        await self.venue.submit(req(price=50_000.00))
        self.venue.on_trade("BTCUSDT", 50_000.00, 10.0, is_buyer_maker=False)
        await self.flush_wire()
        self.assertEqual(self.pushed, [])

    async def test_marketable_limit_takes_with_price_improvement(self):
        u = await self.venue.submit(req(price=50_010.00))
        self.assertIs(u.status, OrderStatus.FILLED)
        self.assertEqual(u.fills[0].price, 50_000.01)
        self.assertFalse(u.fills[0].is_maker)

    async def test_ioc_remainder_expires_and_fok_is_all_or_nothing(self):
        self.venue.on_book_ticker("BTCUSDT", 50_000.00, 0.004, 50_000.01, 0.004)
        u = await self.venue.submit(req(qty=0.01, price=50_000.01, tif=TimeInForce.IOC))
        self.assertIs(u.status, OrderStatus.EXPIRED)
        self.assertAlmostEqual(sum(f.quantity for f in u.fills), 0.004)
        self.venue.on_book_ticker("BTCUSDT", 50_000.00, 0.004, 50_000.01, 0.004)
        u = await self.venue.submit(req(qty=0.01, price=50_000.01, tif=TimeInForce.FOK))
        self.assertIs(u.status, OrderStatus.EXPIRED)
        self.assertEqual(u.fills, ())

    async def test_large_market_order_walks_the_book(self):
        self.venue.on_book_ticker("BTCUSDT", 50_000.00, 0.5, 50_000.01, 0.5)   # thin touch
        u = await self.venue.submit(req(qty=1.5, order_type=OrderType.MARKET))
        self.assertIs(u.status, OrderStatus.FILLED)
        self.assertGreater(len(u.fills), 1)
        vwap = sum(f.price * f.quantity for f in u.fills) / sum(f.quantity for f in u.fills)
        self.assertGreater(vwap, 50_000.01)
        # Taken liquidity is gone until the next book update: the ask moved up.
        u2 = await self.venue.submit(req(qty=0.01, order_type=OrderType.MARKET))
        self.assertGreater(u2.fills[0].price, 50_000.01)

    async def test_filters_and_balances(self):
        self.assertIn("LOT_SIZE", (await self.venue.submit(req(qty=0.000001, price=50_000.0))).reason)
        self.assertIn("PRICE_FILTER", (await self.venue.submit(req(price=50_000.001))).reason)
        self.assertIn("NOTIONAL", (await self.venue.submit(req(qty=0.00001, price=50_000.0))).reason)
        self.assertIn("insufficient balance", (await self.venue.submit(req(qty=5.0, price=50_000.0))).reason)
        self.assertIn("insufficient balance",
                      (await self.venue.submit(req(side=OrderSide.SELL, qty=2.0, order_type=OrderType.MARKET))).reason)
        dup = req(price=49_000.0)
        await self.venue.submit(dup)
        self.assertIn("Duplicate", (await self.venue.submit(dup)).reason)

    async def test_stop_loss_triggers_on_trade_and_slips(self):
        rej = await self.venue.submit(req(side=OrderSide.SELL, qty=0.1, order_type=OrderType.STOP_LOSS, stop=50_100.0))
        self.assertIn("trigger immediately", rej.reason)
        u = await self.venue.submit(req(side=OrderSide.SELL, qty=0.1, order_type=OrderType.STOP_LOSS, stop=49_900.0))
        self.assertIs(u.status, OrderStatus.NEW)
        self.assertAlmostEqual(self.venue.balances()["BTC"]["locked"], 0.1)
        self.venue.on_book_ticker("BTCUSDT", 49_850.00, 0.05, 49_850.01, 1.0)   # market gaps down
        self.venue.on_trade("BTCUSDT", 49_850.00, 0.2, is_buyer_maker=True)
        await self.flush_wire()
        self.assertIs(self.pushed[-1].status, OrderStatus.FILLED)
        self.assertLess(max(f.price for f in self.pushed[-1].fills), 49_900.0)  # filled below the stop: slippage

    async def test_cancel_releases_funds_and_unknown_cancel_returns_none(self):
        r = req(price=49_000.0)
        await self.venue.submit(r)
        u = await self.venue.cancel("BTCUSDT", r.client_order_id)
        self.assertIs(u.status, OrderStatus.CANCELED)
        self.assertEqual(self.venue.balances()["USDT"]["locked"], 0.0)
        self.assertIsNone(await self.venue.cancel("BTCUSDT", r.client_order_id))
        self.assertIsNone(await self.venue.query("BTCUSDT", "nope"))

    async def test_stale_market_data_is_refused(self):
        self.venue._books["BTCUSDT"].updated = time.monotonic() - 60
        u = await self.venue.submit(req(qty=0.01, order_type=OrderType.MARKET))
        self.assertIs(u.status, OrderStatus.REJECTED)
        self.assertIn("stale", u.reason)

    async def test_bar_only_mode(self):
        venue = SimulatedVenue(make_instruments(), SimConfig(latency_median_ms=0, seed=1))
        await venue.start()
        t0 = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        bar = Bar(symbol="BTCUSDT", venue="BINANCE", interval="1m", start_time=t0 - timedelta(minutes=1),
                  close_time=t0 - timedelta(milliseconds=1), open=50_000, high=50_050, low=49_950, close=50_000,
                  volume=10, is_closed=True)
        venue.on_bar(bar)
        u = await venue.submit(req(qty=0.01, order_type=OrderType.MARKET))
        self.assertIs(u.status, OrderStatus.FILLED)
        self.assertGreater(u.fills[0].price, 50_000)      # paid the synthetic half-spread
        await venue.stop()


if __name__ == "__main__":
    unittest.main()
