"""Kill switch levels, operator control file, flatten, persistence; drawdown rules; per-strategy PnL rows."""

import asyncio
import dataclasses
import sqlite3
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone

from messages.events import (
    BarClosed,
    IntentRejected,
    KillSwitch,
    OrderCanceled,
    PositionClosed,
    RiskBreach,
)
from messages.model import Bar, ExecutionMode, HaltLevel, IntentKind, OrderSide, OrderType, RejectStage
from opm.lock import LockState
from risk.pretrade import RiskRejection, RiskRule
from tests.helpers import STRAT, System, intent

SIM = ExecutionMode.SIM
BTC, ETH = "BTCUSDT", "ETHUSDT"


async def wait_for(s: System, cond, rounds: int = 100) -> bool:
    for _ in range(rounds):
        await s.settle(rounds=2)
        if cond():
            return True
        await asyncio.sleep(0.01)
    return cond()


def write_control(s: System, halt: str = "NONE", reset: str = "") -> None:
    s.control_file.write_text(f"[kill_switch]\nhalt = {halt}\nreset = {reset}\n", encoding="utf-8")


async def open_btc(s: System, qty: float = 0.01) -> None:
    s.quote()
    assert await s.enter(intent(qty=qty))
    await s.settle()
    assert s.portfolio.get_position(STRAT, BTC) is not None


def live_bar(symbol: str, close: float, minutes_from_now: int = 0, replay: bool = False) -> BarClosed:
    t = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=minutes_from_now)
    return BarClosed(bar=Bar(symbol=symbol, venue="BINANCE", interval="1m", start_time=t,
                             close_time=t + timedelta(seconds=59.999), open=close, high=close, low=close,
                             close=close, volume=1.0, is_closed=True), replay=replay)


class _BlockAllExits(RiskRule):
    applies_to_exits = True

    def evaluate(self, intent, request, market_state):
        if intent.kind.is_exit:
            raise RiskRejection(self.name, "no exits allowed")


class HaltLevelTests(unittest.IsolatedAsyncioTestCase):
    async def test_halt_entries_rejects_and_journals_entries_but_exits_still_run(self):
        async with System() as s:
            await open_btc(s)
            self.assertTrue(await s.kill_switch.trigger(SIM, HaltLevel.HALT_ENTRIES, "test", "unit test"))

            s.quote(ETH, bid=2999.99, ask=3000.0, bid_qty=10, ask_qty=10)
            self.assertTrue(await s.enter(intent(symbol=ETH)))      # the strategy still decides...
            await s.settle()
            rejected = s.recorder.of(IntentRejected)[-1]           # ...and the OMS refuses, terminally
            self.assertEqual((rejected.stage, rejected.rule_name), (RejectStage.HALTED, "KillSwitch"))
            self.assertIs(s.lock.state(STRAT, ETH), LockState.IDLE)

            s.quote(bid=50_100.0, ask=50_100.02)                   # exits are never gated
            self.assertTrue(await s.exit(intent(kind=IntentKind.CLOSE, side=OrderSide.SELL, qty=1.0)))
            await s.settle()
            self.assertEqual(len(s.recorder.of(PositionClosed)), 1)
            self.assertIs(s.kill_switch.level(SIM), HaltLevel.HALT_ENTRIES)   # still latched
            await s.store.flush()
            with closing(sqlite3.connect(s.store.db_path)) as db:
                outcomes = dict(db.execute("SELECT symbol, outcome FROM intents WHERE kind = 'OPEN'").fetchall())
                halts = db.execute("SELECT level, source, cleared_at FROM halts").fetchall()
            self.assertEqual(outcomes[ETH], "REJECTED:HALTED")
            self.assertEqual(halts, [("HALT_ENTRIES", "test", None)])

    async def test_halt_complete_cancels_working_orders_and_flattens_positions(self):
        async with System() as s:
            await open_btc(s)
            s.quote(ETH, bid=2999.99, ask=3000.0, bid_qty=10, ask_qty=10)
            await s.enter(intent(symbol=ETH, order_type=OrderType.LIMIT, price=2900.0))   # resting entry
            await s.settle()
            self.assertEqual(len(s.oms.working_orders()), 1)

            s.quote(bid=49_000.0, ask=49_000.02)
            await s.kill_switch.trigger(SIM, HaltLevel.HALT_COMPLETE, "test", "flatten everything")
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.active(SIM).flattened_at is not None))

            self.assertEqual(len(s.recorder.of(OrderCanceled)), 1)
            closed = s.recorder.of(PositionClosed)
            self.assertEqual(len(closed), 1)
            self.assertTrue(closed[0].exit_reason.startswith("Kill switch SIM-"))
            self.assertIsNone(s.portfolio.get_position(STRAT, BTC))
            self.assertEqual(s.oms.working_orders(), [])
            # System-attributed exposure is gone; only the sub-lot residual remains, matching the venue.
            self.assertAlmostEqual(s.sim.balances().get("BTC", {}).get("free", 0.0),
                                   s.portfolio.net_quantity(BTC), places=12)
            self.assertLess(s.portfolio.net_quantity(BTC), 0.00001)
            self.assertEqual(s.sim.balances()["USDT"]["locked"], 0.0)
            self.assertTrue(await wait_for(s, lambda: "flattened_at = 20" in s.status_file.read_text(encoding="utf-8")))

    async def test_flatten_is_never_blocked_by_a_risk_rule(self):
        async with System(rules=[_BlockAllExits()]) as s:
            await open_btc(s)
            s.quote(bid=50_100.0, ask=50_100.02)
            await s.exit(intent(kind=IntentKind.CLOSE, side=OrderSide.SELL, qty=1.0))
            await s.settle()
            self.assertEqual(s.recorder.of(IntentRejected)[-1].rule_name, "_BlockAllExits")   # a CLOSE is blocked
            await s.kill_switch.trigger(SIM, HaltLevel.HALT_COMPLETE, "test", "flatten")
            self.assertTrue(await wait_for(s, lambda: s.portfolio.get_position(STRAT, BTC) is None))

    async def test_risk_breach_halts_entries(self):
        async with System() as s:
            await s.bus.publish(RiskBreach(rule_name="OrderVanished", symbol=BTC, detail="gone", mode=SIM))
            await s.settle()
            self.assertIs(s.kill_switch.level(SIM), HaltLevel.HALT_ENTRIES)
            self.assertEqual(s.kill_switch.active(SIM).source, "breach:OrderVanished")


class ControlFileTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_halt_and_reset_by_halt_id(self):
        async with System() as s:
            self.assertIn("halt = NONE", s.control_file.read_text(encoding="utf-8"))   # template created
            write_control(s, "HALT_ENTRIES")
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.level(SIM) is HaltLevel.HALT_ENTRIES))
            halt_id = s.kill_switch.active(SIM).halt_id
            self.assertTrue(await wait_for(s, lambda: f"reset = {halt_id}" in s.status_file.read_text(encoding="utf-8")))

            write_control(s, "HALT_ENTRIES", halt_id)          # still requested -> reset refused
            await asyncio.sleep(0.1)
            self.assertIs(s.kill_switch.level(SIM), HaltLevel.HALT_ENTRIES)
            write_control(s, "NONE", "SIM-19990101-000000-dead")   # wrong id -> nothing
            await asyncio.sleep(0.1)
            self.assertIs(s.kill_switch.level(SIM), HaltLevel.HALT_ENTRIES)
            write_control(s, "NONE", halt_id)
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.level(SIM) is HaltLevel.NONE))

            write_control(s, "HALT_COMPLETE", halt_id)          # a stale reset id cannot release a NEW halt
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.level(SIM) is HaltLevel.HALT_COMPLETE))
            self.assertNotEqual(s.kill_switch.active(SIM).halt_id, halt_id)
            self.assertTrue(await wait_for(s, lambda: len(s.recorder.of(KillSwitch)) == 3))
            levels = [(e.previous_level, e.level) for e in s.recorder.of(KillSwitch)]
            self.assertEqual(levels, [(HaltLevel.NONE, HaltLevel.HALT_ENTRIES), (HaltLevel.HALT_ENTRIES, HaltLevel.NONE),
                                      (HaltLevel.NONE, HaltLevel.HALT_COMPLETE)])

    async def test_garbage_in_the_control_file_changes_nothing(self):
        async with System() as s:
            write_control(s, "PANIC")
            with self.assertLogs("KillSwitch", level="ERROR"):
                await s.kill_switch.poll_control_file()
            self.assertIs(s.kill_switch.level(SIM), HaltLevel.NONE)

    async def test_halt_survives_a_restart(self):
        async with System() as s1:
            await s1.kill_switch.trigger(SIM, HaltLevel.HALT_ENTRIES, "test", "before restart")
            halt_id = s1.kill_switch.active(SIM).halt_id
        async with System(db_dir=s1.tmp) as s2:
            self.assertIs(s2.kill_switch.level(SIM), HaltLevel.HALT_ENTRIES)
            self.assertEqual(s2.kill_switch.active(SIM).halt_id, halt_id)
            write_control(s2, "NONE", halt_id)
            self.assertTrue(await wait_for(s2, lambda: s2.kill_switch.level(SIM) is HaltLevel.NONE))
        async with System(db_dir=s1.tmp) as s3:
            self.assertIs(s3.kill_switch.level(SIM), HaltLevel.NONE)


class DrawdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_window_drawdown_trips_halt_complete_and_reset_rebaselines(self):
        # Capital 100 USDT, long 0.001 BTC (~50 USDT): a 12 % price drop is a ~6 % book drawdown.
        async with System(capital={SIM: 100.0}) as s:
            await open_btc(s, qty=0.001)
            await s.bus.publish(live_bar(BTC, 50_000.0))
            await s.settle()
            self.assertIs(s.kill_switch.level(SIM), HaltLevel.NONE)

            s.market[BTC] = {"close": 44_000.0}
            s.quote(bid=44_000.0, ask=44_000.02)
            await s.bus.publish(live_bar(BTC, 44_000.0, minutes_from_now=1))
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.level(SIM) is HaltLevel.HALT_COMPLETE))
            rec = s.kill_switch.active(SIM)
            self.assertEqual(rec.source, "drawdown_window")
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.active(SIM).flattened_at is not None))

            write_control(s, "NONE", rec.halt_id)
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.level(SIM) is HaltLevel.NONE))
            await s.bus.publish(live_bar(BTC, 44_000.0, minutes_from_now=2))
            await s.settle()
            self.assertIs(s.kill_switch.level(SIM), HaltLevel.NONE)          # the loss was accepted: no re-trip

    async def test_high_water_mark_backstop(self):
        async with System(capital={SIM: 100.0}, max_drawdown_window=1.0, max_drawdown_hwm=0.05) as s:
            await open_btc(s, qty=0.001)
            s.market[BTC] = {"close": 44_000.0}
            await s.bus.publish(live_bar(BTC, 44_000.0))
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.level(SIM) is HaltLevel.HALT_COMPLETE))
            self.assertEqual(s.kill_switch.active(SIM).source, "drawdown_hwm")

    async def test_books_are_per_mode_a_sim_loss_never_halts_another_mode(self):
        registry = {STRAT: SIM, "OTHER": ExecutionMode.TESTNET}
        async with System(registry=registry, capital={SIM: 100.0, ExecutionMode.TESTNET: 100.0}) as s:
            await open_btc(s, qty=0.001)
            s.market[BTC] = {"close": 44_000.0}
            await s.bus.publish(live_bar(BTC, 44_000.0))
            self.assertTrue(await wait_for(s, lambda: s.kill_switch.level(SIM) is HaltLevel.HALT_COMPLETE))
            self.assertIs(s.kill_switch.level(ExecutionMode.TESTNET), HaltLevel.NONE)


class StrategyPnLTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_bars_record_pnl_per_strategy_and_symbol(self):
        async with System() as s:
            await open_btc(s)
            pos = s.portfolio.get_position(STRAT, BTC)
            s.market[BTC] = {"close": 50_500.0}
            await s.bus.publish(live_bar(BTC, 50_500.0))
            await s.bus.publish(live_bar(BTC, 50_500.0, minutes_from_now=-5, replay=True))   # history: not recorded
            await s.settle()
            await s.store.flush()
            with closing(sqlite3.connect(s.store.db_path)) as db:
                db.row_factory = sqlite3.Row
                rows = db.execute("SELECT * FROM strategy_pnl").fetchall()
                eq = db.execute("SELECT * FROM portfolio_equity").fetchall()
            self.assertEqual(len(rows), 1)
            r = rows[0]
            self.assertEqual((r["mode"], r["strategy_id"], r["symbol"]), ("SIM", STRAT, BTC))
            self.assertAlmostEqual(r["unrealized_pnl"], pos.quantity * (50_500.0 - pos.average_entry_price), places=8)
            self.assertAlmostEqual(r["realized_pnl"], pos.realized_pnl, places=10)      # the entry fee so far
            self.assertAlmostEqual(r["total_pnl"], r["realized_pnl"] + r["unrealized_pnl"], places=10)
            self.assertEqual(len(eq), 1)
            self.assertAlmostEqual(eq[0]["equity"], 10_000.0 + r["total_pnl"], places=8)

    async def test_pnl_survives_restart_and_closed_trades_are_counted_once(self):
        async with System() as s1:
            await open_btc(s1)
            s1.quote(bid=50_300.0, ask=50_300.02)              # +0.6 %: covers the 0.2 % round-trip fees
            await s1.exit(intent(kind=IntentKind.CLOSE, side=OrderSide.SELL, qty=1.0))
            await s1.settle()
            realized = s1.recorder.of(PositionClosed)[-1].realized_pnl
            line = next(x for x in s1.portfolio.pnl_at_cost() if x.symbol == BTC)
            self.assertEqual((line.closed_trades, line.winning_trades), (1, 1))
            self.assertAlmostEqual(line.realized_pnl, realized, places=10)
        async with System(db_dir=s1.tmp) as s2:
            line2 = next(x for x in s2.portfolio.pnl_at_cost() if x.symbol == BTC)
            self.assertEqual(dataclasses.replace(line2, timestamp=line.timestamp), line)


if __name__ == "__main__":
    unittest.main()
