"""End-to-end: Intent -> risk -> OMS -> venue -> fills -> portfolio -> lock, on the simulator."""

import asyncio
import unittest
from typing import Optional

import aiosqlite

from connectors.base import VenueUnknownOutcome
from messages.events import (
    IntentRejected,
    OrderFilled,
    OrderPartiallyFilled,
    OrderRejected,
    OrderStatusUnknown,
    PositionClosed,
    PositionOpened,
)
from messages.model import (
    ExecutionMode,
    Fill,
    IntentKind,
    OrderRequest,
    OrderSide,
    OrderStatus,
    RejectStage,
    VenueOrderUpdate,
    utc_now,
)
from opm.lock import LockState
from risk.pretrade import FatFingerRule, RandomMockRejectionRule
from tests.helpers import STRAT, System, intent


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip_market_buy_then_close(self):
        async with System() as s:
            s.quote()
            self.assertTrue(await s.enter(intent(qty=0.01)))
            await s.settle()

            opened = s.recorder.of(PositionOpened)
            self.assertEqual(len(opened), 1)
            pos = opened[0].position
            # Fee charged in BTC on a buy (Binance default): the ledger holds what the account holds.
            self.assertAlmostEqual(pos.quantity, 0.01 * (1 - 0.001), places=10)
            self.assertIs(s.lock.state(STRAT, "BTCUSDT"), LockState.HOLDING)
            self.assertAlmostEqual(s.sim.balances()["BTC"]["free"], pos.quantity, places=10)

            s.quote(bid=50_100.00, ask=50_100.02)
            self.assertTrue(await s.exit(intent(kind=IntentKind.CLOSE, side=OrderSide.SELL, qty=1.0)))  # clamped
            await s.settle()

            closed = s.recorder.of(PositionClosed)
            self.assertEqual(len(closed), 1)
            self.assertIs(s.lock.state(STRAT, "BTCUSDT"), LockState.IDLE)
            self.assertIsNone(s.portfolio.get_position(STRAT, "BTCUSDT"))
            # Net PnL = gross - buy fee (0.00001 BTC valued at the fill) - sell fee (0.1% in USDT).
            # A 0.2% move does NOT cover a 0.2% round-trip fee: the ledger must show the small loss.
            gross = 0.00999 * (50_100.00 - 50_000.01)
            fees = 0.00001 * 50_000.01 + 0.00999 * 50_100.00 * 0.001
            self.assertAlmostEqual(closed[0].realized_pnl, gross - fees, places=8)
            self.assertAlmostEqual(closed[0].position.fees_paid, fees, places=8)

    async def test_lock_never_passes_through_idle_while_a_position_exists(self):
        """Regression found in the live run: PENDING_ENTRY -> IDLE -> HOLDING on a filled entry."""
        async with System() as s:
            s.quote()
            with self.assertLogs("PositionLock", level="INFO") as logs:
                await s.enter(intent())
                await s.settle()
                s.quote(bid=50_100.00, ask=50_100.02)
                await s.exit(intent(kind=IntentKind.CLOSE, side=OrderSide.SELL))
                await s.settle()
            transitions = [line.split(": ", 2)[-1].split(" (")[0] for line in logs.output if "->" in line]
            self.assertEqual(transitions, ["IDLE -> PENDING_ENTRY", "PENDING_ENTRY -> HOLDING",
                                           "HOLDING -> PENDING_EXIT", "PENDING_EXIT -> IDLE"])
            self.assertFalse([line for line in logs.output if line.startswith("ERROR")])

    async def test_unregistered_strategy_is_rejected_and_lock_released(self):
        """Regression (CRITICAL #1/#2): an unregistered strategy used to leave the lock PENDING forever."""
        async with System(registry={}) as s:
            s.quote()
            await s.enter(intent())
            await s.settle()
            rej = s.recorder.of(IntentRejected)
            self.assertEqual(rej[0].stage, RejectStage.REGISTRY)
            self.assertIs(s.lock.state(STRAT, "BTCUSDT"), LockState.IDLE)

    async def test_risk_rejection_releases_lock_and_exits_bypass_entry_rules(self):
        async with System(rules=[RandomMockRejectionRule(probability=1.0)]) as s:
            s.quote()
            await s.enter(intent())
            await s.settle()
            self.assertEqual(s.recorder.of(IntentRejected)[0].stage, RejectStage.RISK)
            self.assertIs(s.lock.state(STRAT, "BTCUSDT"), LockState.IDLE)

    async def test_fat_finger_fails_closed_without_price(self):
        async with System(rules=[FatFingerRule(1_000_000)]) as s:
            s.market.clear()
            s.quote()
            await s.enter(intent())
            await s.settle()
            self.assertEqual(s.recorder.of(IntentRejected)[0].rule_name, "FatFingerRule")

    async def test_spot_short_is_rejected_by_venue_and_lock_released(self):
        async with System() as s:
            s.quote()
            await s.enter(intent(side=OrderSide.SELL))
            await s.settle()
            rej = s.recorder.of(OrderRejected)
            self.assertEqual(len(rej), 1)
            self.assertIn("insufficient balance", rej[0].reason)
            self.assertIs(s.lock.state(STRAT, "BTCUSDT"), LockState.IDLE)

    async def test_close_without_position_is_rejected(self):
        async with System() as s:
            s.quote()
            from messages.events import IntentIssued
            await s.bus.publish(IntentIssued(intent=intent(kind=IntentKind.CLOSE, side=OrderSide.SELL)))
            await s.settle()
            self.assertEqual(s.recorder.of(IntentRejected)[0].rule_name, "ReduceOnly")

    async def test_events_carry_immutable_snapshots(self):
        """Regression (HIGH #8): each fill event shows the order state AFTER that fill, not the final state."""
        async with System() as s:
            s.quote(ask_qty=0.004)    # 0.01 BTC must walk several levels -> several fills
            await s.enter(intent(qty=0.01))
            await s.settle()
            partials = s.recorder.of(OrderPartiallyFilled)
            final = s.recorder.of(OrderFilled)
            self.assertGreaterEqual(len(partials), 1)
            self.assertEqual(len(final), 1)
            cum = [e.order.filled_quantity for e in partials] + [final[0].order.filled_quantity]
            self.assertEqual(cum, sorted(cum))
            self.assertTrue(all(e.order.status is OrderStatus.PARTIALLY_FILLED for e in partials))
            self.assertAlmostEqual(cum[-1], 0.01, places=10)
            prices = [e.fill.price for e in partials] + [final[0].fill.price]
            self.assertGreater(prices[-1], prices[0])   # walked the book: slippage

    async def test_order_write_ahead_and_epoch_ms_persistence(self):
        async with System() as s:
            s.quote()
            await s.enter(intent())
            await s.settle()
            await s.store.flush()
            async with aiosqlite.connect(s.store.db_path) as db:
                row = await (await db.execute(
                    "SELECT status, strategy_id, typeof(created_at), filled_quantity FROM orders")).fetchone()
                n_fills = (await (await db.execute("SELECT COUNT(*) FROM fills")).fetchone())[0]
                pos = await (await db.execute("SELECT status FROM positions")).fetchone()
            self.assertEqual(row[0], "FILLED")
            self.assertEqual(row[1], STRAT)
            self.assertEqual(row[2], "integer")
            self.assertEqual(n_fills, 1)
            self.assertEqual(pos[0], "OPEN")


class _FlakyVenue:
    """Times out on submit (the order DID reach the exchange), then reports it filled on query."""
    venue_id = "BINANCE"
    mode = ExecutionMode.TESTNET
    needs_polling = True

    def __init__(self, exists: bool):
        self.exists = exists
        self.request: Optional[OrderRequest] = None

    async def start(self): ...
    async def stop(self): ...
    def set_update_handler(self, h): ...

    async def submit(self, request):
        self.request = request
        raise VenueUnknownOutcome("timeout")

    async def cancel(self, symbol, coid):
        return None

    async def query(self, symbol, coid):
        if not self.exists:
            return None
        fill = Fill(client_order_id=coid, strategy_id="", intent_id="", symbol=symbol, side=OrderSide.BUY,
                    quantity=self.request.quantity, price=50_000.0, fee=0.0, fee_asset="USDT", is_maker=False,
                    exchange_trade_id="T1", timestamp=utc_now())
        return VenueOrderUpdate(client_order_id=coid, status=OrderStatus.FILLED, fills=(fill,),
                                cumulative_filled_quantity=self.request.quantity)


class UnknownOutcomeTests(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_keeps_lock_then_reconciles_to_filled(self):
        """Regression (CRITICAL #3): a timeout must NOT re-arm the strategy (double-entry risk)."""
        venue = _FlakyVenue(exists=True)
        async with System(registry={STRAT: ExecutionMode.TESTNET}, extra_venues=[venue]) as s:
            await s.enter(intent())
            for _ in range(20):
                await asyncio.sleep(0.005)
                if s.recorder.of(OrderStatusUnknown):
                    break
            self.assertTrue(s.recorder.of(OrderStatusUnknown))
            self.assertIs(s.lock.state(STRAT, "BTCUSDT"), LockState.PENDING_ENTRY)
            self.assertFalse(s.lock.try_begin_entry(intent()))      # cannot double-enter
            await asyncio.sleep(0.2)                                # poller reconciles
            await s.settle()
            self.assertEqual(len(s.recorder.of(PositionOpened)), 1)
            self.assertIs(s.lock.state(STRAT, "BTCUSDT"), LockState.HOLDING)

    async def test_timeout_never_reached_exchange_resolves_to_rejected(self):
        venue = _FlakyVenue(exists=False)
        async with System(registry={STRAT: ExecutionMode.TESTNET}, extra_venues=[venue]) as s:
            await s.enter(intent())
            await asyncio.sleep(0.4)
            await s.settle()
            self.assertEqual(len(s.recorder.of(OrderRejected)), 1)
            self.assertIs(s.lock.state(STRAT, "BTCUSDT"), LockState.IDLE)


if __name__ == "__main__":
    unittest.main()
