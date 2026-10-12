"""Restart rehydration: stop a System, boot a new one on the same database, and carry on exactly."""

import asyncio
import dataclasses
import json
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing

from bus.event_bus import EventBus
from connectors.binance.codec import balances_from_account
from connectors.binance.feed import BinanceFeed
from messages.events import BarClosed, IntentRejected, OrderFilled, PositionClosed, PositionOpened
from messages.model import ExecutionMode, HaltLevel, IntentKind, OrderSide, OrderType, RejectStage
from opm.lock import LockState
from state.stores import SCHEMA_VERSION, SqliteStore, to_ms
from tests.helpers import STRAT, System, intent
from tests.test_market_data import MIN, FakeREST, kline
from tools.reconcile_trades import reconcile

ETH = "ETHUSDT"


def eth(kind=IntentKind.OPEN, side=OrderSide.BUY, qty=0.01, reference_price=None, **kw):
    return dataclasses.replace(intent(kind=kind, side=side, qty=qty, symbol=ETH, **kw),
                               reference_price=reference_price)


def quote_eth(s: System, mid: float):
    s.quote(ETH, bid=mid - 0.01, ask=mid, bid_qty=10, ask_qty=10)


class RehydrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_open_position_survives_restart_and_closes_cleanly(self):
        async with System() as s1:
            quote_eth(s1, 3000.0)
            self.assertTrue(await s1.enter(eth(reference_price=2999.5)))
            await s1.settle()
            before = s1.portfolio.get_position(STRAT, ETH)
            usdt_before = s1.sim.balances()["USDT"]["free"]
        self.assertIsNotNone(before)

        async with System(db_dir=s1.tmp) as s2:
            self.assertEqual(s2.rehydration_problems, {})
            pos = s2.portfolio.get_position(STRAT, ETH)
            self.assertIsNotNone(pos)
            for f in ("quantity", "average_entry_price", "realized_pnl", "fees_paid", "trade_id"):
                self.assertEqual(getattr(pos, f), getattr(before, f), f)
            self.assertEqual(to_ms(pos.opened_at), to_ms(before.opened_at))   # same positions row key (ms)
            self.assertIs(s2.lock.state(STRAT, ETH), LockState.HOLDING)
            self.assertFalse(s2.lock.try_begin_entry(eth()))                  # no double entry after restart
            self.assertIn((STRAT, ETH), s2.opm._active_positions)
            # The simulator forgot everything; it is rebuilt from the fills ledger.
            self.assertAlmostEqual(s2.sim.balances()["ETH"]["free"], pos.quantity, places=12)
            self.assertAlmostEqual(s2.sim.balances()["USDT"]["free"], usdt_before, places=9)
            self.assertIs(s2.kill_switch.level(ExecutionMode.SIM), HaltLevel.NONE)

            s2.quote(ETH, bid=3010.0, ask=3010.01, bid_qty=10, ask_qty=10)
            self.assertTrue(await s2.exit(eth(kind=IntentKind.CLOSE, side=OrderSide.SELL, qty=1.0)))
            await s2.settle()
            closed = s2.recorder.of(PositionClosed)
            self.assertEqual(len(closed), 1)
            self.assertEqual(closed[0].position.trade_id, before.trade_id)
            await s2.store.flush()

        failures, warnings, summary = reconcile(os.path.join(s1.tmp, "test.db"))
        self.assertEqual((failures, warnings), ([], []))
        self.assertEqual(summary["closed"], 1)
        with closing(sqlite3.connect(os.path.join(s1.tmp, "test.db"))) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM positions").fetchone()[0], 1)   # same row, no duplicate
            t = db.execute("SELECT entry_ref_price, status FROM trades").fetchone()
        self.assertEqual(t, (2999.5, "CLOSED"))

    async def test_residual_is_carried_across_a_restart(self):
        async with System() as s1:
            quote_eth(s1, 3000.0)
            await s1.enter(eth())
            await s1.settle()
            s1.quote(ETH, bid=3005.0, ask=3005.01, bid_qty=10, ask_qty=10)
            await s1.exit(eth(kind=IntentKind.CLOSE, side=OrderSide.SELL, qty=1.0))
            await s1.settle()
            self.assertAlmostEqual(s1.portfolio.get_residual(STRAT, ETH), 0.00009, places=12)

        async with System(db_dir=s1.tmp) as s2:
            self.assertAlmostEqual(s2.portfolio.get_residual(STRAT, ETH), 0.00009, places=12)
            self.assertAlmostEqual(s2.sim.balances()["ETH"]["free"], 0.00009, places=12)
            self.assertIs(s2.lock.state(STRAT, ETH), LockState.IDLE)
            self.assertEqual(s2.portfolio.get_strategy_metrics(STRAT).total_closed, 1)
            quote_eth(s2, 3001.0)
            await s2.enter(eth())
            await s2.settle()
            self.assertAlmostEqual(s2.recorder.of(PositionOpened)[-1].position.quantity, 0.01008, places=12)
            s2.quote(ETH, bid=3002.0, ask=3002.01, bid_qty=10, ask_qty=10)
            await s2.exit(eth(kind=IntentKind.CLOSE, side=OrderSide.SELL, qty=1.0))
            await s2.settle()
            await s2.store.flush()
            self.assertAlmostEqual(s2.portfolio.net_quantity(ETH), s2.sim.balances()["ETH"]["free"], places=12)

        # Before rehydration a restart broke the residual chain (a reconcile warning). Not any more.
        failures, warnings, summary = reconcile(os.path.join(s1.tmp, "test.db"))
        self.assertEqual((failures, warnings, summary["closed"]), ([], [], 2))

    async def test_simulator_working_order_is_canceled_on_restart(self):
        async with System() as s1:
            quote_eth(s1, 3000.0)
            await s1.enter(eth(order_type=OrderType.LIMIT, price=2900.0))
            await s1.settle()
            self.assertEqual(len(s1.oms.working_orders()), 1)
            coid = s1.oms.working_orders()[0].client_order_id
            self.assertIs(s1.lock.state(STRAT, ETH), LockState.PENDING_ENTRY)

        async with System(db_dir=s1.tmp) as s2:
            self.assertEqual(s2.oms.working_orders(), [])
            self.assertIs(s2.lock.state(STRAT, ETH), LockState.IDLE)
            self.assertEqual(s2.sim.balances()["USDT"], {"free": 10_000.0, "locked": 0.0})   # lock died with it
            await s2.store.flush()
            with closing(sqlite3.connect(os.path.join(s1.tmp, "test.db"))) as db:
                status = db.execute("SELECT status FROM orders WHERE client_order_id = ?", (coid,)).fetchone()[0]
            self.assertEqual(status, "CANCELED")

    async def test_order_on_a_surviving_venue_is_reconciled_after_filling_while_down(self):
        async with System() as s1:
            s1.sim.survives_restart = True                 # play a real exchange: it keeps our orders
            quote_eth(s1, 3000.0)
            await s1.enter(eth(order_type=OrderType.LIMIT, price=2990.0))
            await s1.settle()
            self.assertEqual(len(s1.oms.working_orders()), 1)
        venue = s1.sim
        venue.on_book_ticker(ETH, 2989.0, 10, 2989.99, 10)   # fills while we are down

        async with System(db_dir=s1.tmp, sim=venue) as s2:
            for _ in range(40):
                await asyncio.sleep(0.02)
                await s2.settle()
                if s2.recorder.of(PositionOpened):
                    break
            self.assertEqual(len(s2.recorder.of(OrderFilled)), 1)
            self.assertIs(s2.lock.state(STRAT, ETH), LockState.HOLDING)
            pos = s2.portfolio.get_position(STRAT, ETH)
            self.assertAlmostEqual(pos.quantity, 0.00999, places=12)
            self.assertAlmostEqual(pos.average_entry_price, 2990.0, places=8)

    async def test_integrity_problem_starts_the_mode_halted(self):
        async with System() as s1:
            quote_eth(s1, 3000.0)
            await s1.enter(eth())
            await s1.settle()
        with closing(sqlite3.connect(os.path.join(s1.tmp, "test.db"))) as db:
            db.execute("UPDATE positions SET quantity = quantity + 0.001")     # the store contradicts its fills
            db.commit()

        async with System(db_dir=s1.tmp) as s2:
            self.assertTrue(s2.rehydration_problems)
            self.assertIs(s2.kill_switch.level(ExecutionMode.SIM), HaltLevel.HALT_ENTRIES)
            self.assertEqual(s2.kill_switch.active(ExecutionMode.SIM).source, "rehydration")
            self.assertIs(s2.lock.state(STRAT, ETH), LockState.HOLDING)       # still managed
            s2.quote()
            self.assertTrue(await s2.enter(intent()))                      # a BTC entry: rejected, halted
            await s2.settle()
            rejected = s2.recorder.of(IntentRejected)
            self.assertEqual(rejected[-1].stage, RejectStage.HALTED)


class MigrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_v3_database_is_migrated_in_place(self):
        tmp = tempfile.mkdtemp(prefix="trading_test_")
        path = os.path.join(tmp, "v3.db")
        store = SqliteStore(db_path=path)
        await store.start()
        await store.stop()
        with closing(sqlite3.connect(path)) as db:       # turn it back into a v3 file with history
            for table in ("halts", "strategy_pnl", "portfolio_equity"):
                db.execute(f"DROP TABLE {table}")
            db.execute("ALTER TABLE intents DROP COLUMN reference_price")
            db.execute("INSERT INTO intents (intent_id, created_at, strategy_id, symbol, venue, kind, side, "
                       "order_type, quantity) VALUES ('old', 1, 'S', 'ETHUSDT', 'BINANCE', 'OPEN', 'BUY', 'MARKET', 1)")
            db.execute("PRAGMA user_version = 3")
            db.commit()

        store = SqliteStore(db_path=path)
        with self.assertLogs("SqliteStore", level="WARNING"):
            await store.start()
        await store.stop()
        with closing(sqlite3.connect(path)) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue({"halts", "strategy_pnl", "portfolio_equity"} <= tables)
            self.assertEqual(db.execute("SELECT intent_id, reference_price FROM intents").fetchall(), [("old", None)])


class LiveGapTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_gap_is_healed_as_replay_before_the_live_bar(self):
        now_minute = int(time.time() * 1000) // MIN * MIN
        base = now_minute - 10 * MIN
        bus = EventBus()
        published = []
        bus.subscribe(BarClosed, published.append)
        feed = BinanceFeed(bus, ["BTCUSDT"], rest=FakeREST([kline(base + k * MIN) for k in range(0, 6)]))
        feed._last_start_ms["BTCUSDT"] = base                    # we had bar 0; bars 1-2 are lost
        live = {"e": "kline", "k": {"s": "BTCUSDT", "i": "1m", "t": base + 3 * MIN, "T": base + 4 * MIN - 1,
                                    "o": "1", "h": "1", "l": "1", "c": "1", "v": "1", "x": True}}
        with self.assertLogs("BinanceFeed", level="WARNING"):
            await feed._process_message(json.dumps({"data": live}))
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        starts = [(int(e.bar.start_time.timestamp() * 1000 - base) // MIN, e.replay) for e in published]
        self.assertEqual(starts, [(1, True), (2, True), (3, False)])


class CodecTests(unittest.TestCase):
    def test_account_balances(self):
        data = {"balances": [{"asset": "BTC", "free": "0.001", "locked": "0.0005"},
                             {"asset": "BNB", "free": "0.00000000", "locked": "0.00000000"},
                             {"asset": "USDT", "free": "12.5", "locked": "0"}]}
        self.assertEqual(balances_from_account(data), {"BTC": 0.0015, "USDT": 12.5})


if __name__ == "__main__":
    unittest.main()
