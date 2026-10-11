"""Trade journal and sub-lot residual ("dust") carry: ledger == venue, trades reconcile with fills."""

import dataclasses
import sqlite3
import unittest
from contextlib import closing
from datetime import timedelta

from messages.events import BarClosed, OrderFilled, PositionClosed, PositionOpened
from messages.model import (
    Bar,
    ExecutionMode,
    Fill,
    IntentKind,
    OrderRequest,
    OrderSide,
    OrderSnapshot,
    OrderStatus,
    OrderType,
    new_id,
    utc_now,
)
from opm.lock import LockState
from state.portfolio import PortfolioManager
from tests.helpers import STRAT, System, intent, make_instruments
from tools.reconcile_trades import reconcile

ETH = "ETHUSDT"
ETH_STEP = 0.0001


def eth_entry(**kw):
    return dataclasses.replace(intent(symbol=ETH, qty=0.01), **kw)


def eth_exit(**kw):
    # qty is clamped to the position by the OMS
    return dataclasses.replace(intent(kind=IntentKind.CLOSE, side=OrderSide.SELL, qty=1.0, symbol=ETH), **kw)


async def round_trip(s: System, entry_px: float, exit_px: float, entry_kw=None, exit_kw=None) -> PositionClosed:
    s.quote(ETH, bid=entry_px - 0.01, ask=entry_px, bid_qty=10, ask_qty=10)
    assert await s.enter(eth_entry(**(entry_kw or {})))
    await s.settle()
    s.quote(ETH, bid=exit_px, ask=exit_px + 0.01, bid_qty=10, ask_qty=10)
    assert await s.exit(eth_exit(**(exit_kw or {})))
    await s.settle()
    return s.recorder.of(PositionClosed)[-1]


class DustCarryTests(unittest.IsolatedAsyncioTestCase):
    async def test_residual_is_kept_valued_and_flat_for_trading(self):
        async with System() as s:
            closed = await round_trip(s, 3000.00, 3010.00)

            # Buy 0.01, fee 0.00001 ETH -> hold 0.00999; the exit can only sell 0.0099.
            self.assertAlmostEqual(closed.position.residual_quantity, 0.00009, places=12)
            self.assertAlmostEqual(s.portfolio.get_residual(STRAT, ETH), 0.00009, places=12)
            self.assertAlmostEqual(s.sim.balances()["ETH"]["free"], 0.00009, places=12)
            # ...but it is flat for trading: no position, lock released, entries allowed.
            self.assertIsNone(s.portfolio.get_position(STRAT, ETH))
            self.assertIs(s.lock.state(STRAT, ETH), LockState.IDLE)

            # The trade's PnL is complete: cash moved + the residual valued at the exit price.
            exit_px = s.recorder.of(OrderFilled)[-1].fill.price
            cash = s.sim.balances()["USDT"]["free"] - 10_000.0
            self.assertAlmostEqual(closed.realized_pnl, cash + 0.00009 * exit_px, places=8)

    async def test_next_trade_absorbs_residual_so_dust_never_accumulates(self):
        async with System() as s:
            prices = [3000.0, 3004.5, 2998.2, 3001.0, 3007.3, 3002.2, 2995.0, 3000.4,
                      3003.3, 2999.9, 3006.1, 3001.7, 2997.6, 3002.8, 3000.0, 3005.5]
            total = 0.0
            for i in range(len(prices) - 1):
                closed = await round_trip(s, prices[i], prices[i + 1])
                total += closed.realized_pnl
                held = s.sim.balances().get("ETH", {}).get("free", 0.0)
                # The ledger always equals what the venue holds, and stays below one lot step.
                self.assertAlmostEqual(s.portfolio.net_quantity(ETH), held, places=12)
                self.assertLess(held, ETH_STEP)

            # Second trade: 0.00009 carried + 0.00999 bought = 0.01008 -> sells 0.0100, keeps 0.00008.
            exits = [e for e in s.recorder.of(OrderFilled) if e.order.request.side is OrderSide.SELL]
            self.assertAlmostEqual(exits[1].order.request.quantity, 0.0100, places=10)

            # Across all trades: sum of realized PnL == cash moved + what is still held, at the last price.
            last_exit = exits[-1].fill.price
            cash = s.sim.balances()["USDT"]["free"] - 10_000.0
            self.assertAlmostEqual(total, cash + s.portfolio.net_quantity(ETH) * last_exit, places=8)

            await s.store.flush()
            failures, warnings, summary = reconcile(s.store.db_path)
            self.assertEqual(failures, [])
            self.assertEqual(warnings, [])
            self.assertEqual(summary["closed"], len(prices) - 1)
            self.assertAlmostEqual(summary["realized_pnl"], total, places=8)

            with closing(sqlite3.connect(s.store.db_path)) as db:
                rows = db.execute("SELECT carried_in_qty, residual_qty FROM trades ORDER BY entry_time").fetchall()
                self.assertEqual(rows[0][0], 0.0)
                for prev, cur in zip(rows, rows[1:]):   # each trade inherits the previous residual
                    self.assertAlmostEqual(cur[0], prev[1], places=12)
                (n_links,) = db.execute("SELECT COUNT(*) FROM trade_fills").fetchone()
                self.assertEqual(n_links, 2 * (len(prices) - 1))
                (unlinked,) = db.execute("SELECT COUNT(*) FROM positions p LEFT JOIN trades t USING (trade_id) "
                                         "WHERE t.trade_id IS NULL").fetchone()
                self.assertEqual(unlinked, 0)

                # The reconciler is not vacuous: a tampered journal row is caught.
                db.execute("UPDATE trades SET realized_pnl = realized_pnl + 0.01, gross_pnl = gross_pnl + 0.01 "
                           "WHERE rowid = (SELECT MIN(rowid) FROM trades)")
                db.commit()
            failures, _, _ = reconcile(s.store.db_path)
            self.assertTrue(any("fills give" in f for f in failures), failures)


class TradeJournalTests(unittest.IsolatedAsyncioTestCase):
    async def test_trade_row_records_prices_references_and_excursions(self):
        async with System() as s:
            s.quote(ETH, bid=2999.99, ask=3000.01, bid_qty=10, ask_qty=10)
            self.assertTrue(await s.enter(eth_entry(reference_price=2999.50, reason="test entry")))
            await s.settle()
            pos = s.recorder.of(PositionOpened)[-1].position
            self.assertIsNotNone(pos.trade_id)

            # Price path while open (bars that start after the entry): high 3050, low 2980.
            start = utc_now() + timedelta(seconds=1)
            await s.bus.publish(BarClosed(bar=Bar(
                symbol=ETH, venue="BINANCE", interval="1m", start_time=start,
                close_time=start + timedelta(seconds=59.999), open=3001.0, high=3050.0, low=2980.0,
                close=3010.0, volume=1.0, is_closed=True)))
            await s.settle()

            s.quote(ETH, bid=3010.00, ask=3010.02, bid_qty=10, ask_qty=10)
            self.assertTrue(await s.exit(eth_exit(reference_price=3011.00, reason="take profit")))
            await s.settle()
            await s.store.flush()

            with closing(sqlite3.connect(s.store.db_path)) as db:
                db.row_factory = sqlite3.Row
                t = db.execute("SELECT * FROM trades").fetchone()
            self.assertEqual(t["trade_id"], pos.trade_id)
            self.assertEqual((t["direction"], t["status"]), ("LONG", "CLOSED"))
            self.assertAlmostEqual(t["entry_qty"], 0.01, places=12)
            self.assertAlmostEqual(t["exit_qty"], 0.0099, places=12)
            self.assertAlmostEqual(t["max_quantity"], 0.00999, places=12)
            self.assertAlmostEqual(t["avg_entry_price"], 3000.01, places=8)
            self.assertAlmostEqual(t["avg_exit_price"], 3010.00, places=8)
            self.assertAlmostEqual(t["residual_qty"], 0.00009, places=12)
            self.assertEqual((t["entry_ref_price"], t["exit_ref_price"]), (2999.50, 3011.00))
            self.assertEqual(t["exit_reason"], "take profit")
            self.assertAlmostEqual(t["mfe"], 3050.0 - 3000.01, places=8)
            self.assertAlmostEqual(t["mae"], 2980.0 - 3000.01, places=8)
            self.assertAlmostEqual(t["realized_pnl"], t["gross_pnl"] - t["fees_paid"], places=10)
            self.assertEqual(t["num_fills"], 2)
            self.assertGreaterEqual(t["holding_ms"], 0)


class _FakeBus:
    def __init__(self):
        self.events = []

    def subscribe(self, *_, **__):
        pass

    async def publish(self, event):
        self.events.append(event)


class _FakeStore:
    def __init__(self):
        self.trades, self.links = {}, []

    async def save_position(self, pos, status):
        pass

    async def save_trade(self, record):
        self.trades[record.trade_id] = record

    async def link_trade_fill(self, trade_id, fill):
        self.links.append((trade_id, fill.quantity, fill.fee))


def _filled(side: OrderSide, qty: float, price: float, fee: float, trade_no: str):
    it = intent(side=side, qty=qty)
    req = OrderRequest(client_order_id=new_id(), symbol="BTCUSDT", venue="BINANCE", side=side,
                       order_type=OrderType.MARKET, quantity=qty)
    now = utc_now()
    order = OrderSnapshot(client_order_id=req.client_order_id, strategy_id=STRAT, mode=ExecutionMode.SIM,
                          intent=it, request=req, status=OrderStatus.FILLED, filled_quantity=qty,
                          average_price=price, cumulative_fee=fee, venue_order_id=None, version=1,
                          created_at=now, updated_at=now)
    fill = Fill(client_order_id=req.client_order_id, strategy_id=STRAT, intent_id=it.intent_id, symbol="BTCUSDT",
                side=side, quantity=qty, price=price, fee=fee, fee_asset="USDT", is_maker=False,
                exchange_trade_id=trade_no, timestamp=now)
    return order, fill


class FlipTests(unittest.IsolatedAsyncioTestCase):
    async def test_flip_through_zero_splits_the_fill_across_two_trades(self):
        bus, store = _FakeBus(), _FakeStore()
        pm = PortfolioManager(bus, make_instruments(), store)
        await pm.apply_fill(*_filled(OrderSide.BUY, 1.0, 100.0, 0.100, "t1"))
        await pm.apply_fill(*_filled(OrderSide.SELL, 1.5, 110.0, 0.165, "t2"))    # long 1.0 -> short 0.5
        await pm.apply_fill(*_filled(OrderSide.BUY, 0.5, 105.0, 0.0525, "t3"))   # cover

        kinds = [type(e).__name__ for e in bus.events]
        self.assertEqual(kinds, ["PositionOpened", "PositionClosed", "PositionOpened", "PositionClosed"])
        long_t, short_t = sorted(store.trades.values(), key=lambda t: t.entry_time)
        self.assertEqual((long_t.direction, short_t.direction), ("LONG", "SHORT"))
        # The flipping fill is split 1.0 / 0.5, its fee pro rata 0.11 / 0.055.
        self.assertAlmostEqual(long_t.realized_pnl, 1.0 * 10 - 0.100 - 0.110, places=10)
        self.assertAlmostEqual(short_t.realized_pnl, 0.5 * 5 - 0.055 - 0.0525, places=10)
        self.assertAlmostEqual(short_t.entry_qty, 0.5, places=12)
        legs = sorted((q, f) for _, q, f in store.links)
        self.assertEqual(len(legs), 4)
        self.assertAlmostEqual(legs[1][0], 0.5, places=12)
        self.assertAlmostEqual(legs[1][1], 0.055, places=12)
        self.assertIsNone(pm.get_position(STRAT, "BTCUSDT"))
        self.assertEqual(pm.get_residual(STRAT, "BTCUSDT"), 0.0)


if __name__ == "__main__":
    unittest.main()
