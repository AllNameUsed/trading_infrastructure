"""Event bus ordering guarantees and PositionLock state machine."""

import asyncio
import unittest

from bus.event_bus import EventBus
from messages.events import Event, Heartbeat, IntentRejected, KillSwitch
from messages.model import ExecutionMode, HaltLevel, IntentKind, OrderSide, RejectStage
from opm.lock import LockManager, LockState
from tests.helpers import STRAT, intent


class BusTests(unittest.IsolatedAsyncioTestCase):
    async def test_fifo_within_priority_and_priority_across(self):
        bus = EventBus()
        seen = []
        bus.subscribe(Heartbeat, lambda e: seen.append(("hb", e.sequence)))
        bus.subscribe(KillSwitch, lambda e: seen.append(("kill", e.reason)))
        for i in range(50):
            await bus.publish(Heartbeat(source="t", sequence=i))
        await bus.publish(KillSwitch(mode=ExecutionMode.SIM, level=HaltLevel.HALT_COMPLETE,
                                     previous_level=HaltLevel.NONE, halt_id="h", source="t", reason="x"))
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        self.assertEqual(seen[0], ("kill", "x"))                       # CRITICAL pre-empts the backlog
        self.assertEqual([s for _, s in seen[1:]], list(range(50)))   # strictly FIFO otherwise

    async def test_explicit_handler_order_and_base_class_dispatch(self):
        bus = EventBus()
        calls = []
        bus.subscribe(Heartbeat, lambda e: calls.append("late"), order=10)
        bus.subscribe(Event, lambda e: calls.append("base"), order=5)
        bus.subscribe(Heartbeat, lambda e: calls.append("early"), order=-10)
        await bus.publish(Heartbeat(source="t", sequence=0))
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        self.assertEqual(calls, ["early", "base", "late"])

    async def test_stop_wakes_an_idle_loop(self):
        bus = EventBus()
        task = asyncio.create_task(bus.run())
        await asyncio.sleep(0.01)
        bus.stop()
        await asyncio.wait_for(task, timeout=1.0)   # previously hung until the next event

    async def test_handler_crash_does_not_kill_loop(self):
        bus = EventBus()
        got = []
        bus.subscribe(Heartbeat, lambda e: 1 / 0)
        bus.subscribe(Heartbeat, lambda e: got.append(e.sequence))
        await bus.publish(Heartbeat(source="t", sequence=1))
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        self.assertEqual(got, [1])


class LockTests(unittest.IsolatedAsyncioTestCase):
    async def test_try_begin_entry_is_exclusive(self):
        """Regression (HIGH #6): N bars arriving before the first intent is dispatched -> exactly ONE entry."""
        bus = EventBus()
        lock = LockManager(bus)
        wins = [lock.try_begin_entry(intent()) for _ in range(50)]
        self.assertEqual(sum(wins), 1)
        self.assertIs(lock.state(STRAT, "BTCUSDT"), LockState.PENDING_ENTRY)

    async def test_rejected_intent_releases_lock(self):
        """Regression (CRITICAL #2): a rejection anywhere before the venue rolls the lock back."""
        bus = EventBus()
        lock = LockManager(bus)
        it = intent()
        self.assertTrue(lock.try_begin_entry(it))
        await bus.publish(IntentRejected(intent=it, stage=RejectStage.RISK, rule_name="R", reason="no"))
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        self.assertIs(lock.state(STRAT, "BTCUSDT"), LockState.IDLE)

    async def test_rejection_of_a_different_intent_is_ignored(self):
        bus = EventBus()
        lock = LockManager(bus)
        held = intent()
        self.assertTrue(lock.try_begin_entry(held))
        await bus.publish(IntentRejected(intent=intent(), stage=RejectStage.RISK, rule_name="R", reason="other"))
        task = asyncio.create_task(bus.run())
        await bus.drain()
        bus.stop()
        await task
        self.assertIs(lock.state(STRAT, "BTCUSDT"), LockState.PENDING_ENTRY)

    async def test_exit_requires_holding(self):
        bus = EventBus()
        lock = LockManager(bus)
        self.assertFalse(lock.try_begin_exit(intent(kind=IntentKind.CLOSE, side=OrderSide.SELL)))


if __name__ == "__main__":
    unittest.main()
