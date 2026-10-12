"""Shared fixtures: a fully wired in-process system on the SimulatedVenue, no network."""

import asyncio
import os
import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

from bus.event_bus import EventBus
from connectors.sim.venue import SimConfig, SimulatedVenue
from execution.oms import OrderManagementSystem
from execution.router import VenueRouter
from messages.events import Event
from messages.instruments import InstrumentRegistry, InstrumentSpec
from messages.model import ExecutionMode, Intent, IntentKind, OrderSide, OrderType, TimeInForce
from opm.lock import LockManager
from opm.manager import OpenPositionManager
from ops.journal import DatabaseJournal
from risk.kill_switch import KillSwitchController
from risk.pretrade import PreTradeRiskManager, RiskRule
from risk.supervisor import RiskSupervisor
from state.portfolio import PortfolioManager
from state.rehydration import Rehydrator
from state.stores import SqliteStore

STRAT = "TEST_STRAT"


def make_instruments() -> InstrumentRegistry:
    return InstrumentRegistry([
        InstrumentSpec(symbol="BTCUSDT", venue="BINANCE", base_asset="BTC", quote_asset="USDT",
                       tick_size=Decimal("0.01"), step_size=Decimal("0.00001"), min_qty=Decimal("0.00001"),
                       max_qty=Decimal("9000"), min_notional=Decimal("5")),
        InstrumentSpec(symbol="ETHUSDT", venue="BINANCE", base_asset="ETH", quote_asset="USDT",
                       tick_size=Decimal("0.01"), step_size=Decimal("0.0001"), min_qty=Decimal("0.0001"),
                       max_qty=Decimal("9000"), min_notional=Decimal("5")),
    ])


def intent(kind=IntentKind.OPEN, side=OrderSide.BUY, qty=0.01, symbol="BTCUSDT", order_type=OrderType.MARKET,
           price=None, stop_price=None, tif=TimeInForce.GTC, strategy_id=STRAT) -> Intent:
    return Intent(strategy_id=strategy_id, symbol=symbol, venue="BINANCE", kind=kind, side=side,
                  order_type=order_type, quantity=qty, price=price, stop_price=stop_price, time_in_force=tif)


class Recorder:
    """Subscribes to Event (base class) and records everything, in dispatch order."""

    def __init__(self, bus: EventBus):
        self.events: List[Event] = []
        bus.subscribe(Event, self.events.append, order=1000)

    def of(self, cls):
        return [e for e in self.events if isinstance(e, cls)]


class System:
    """
    The whole core wired like main.py (OMS, portfolio, lock, OPM, kill switch, risk supervisor,
    rehydration) on a zero-latency simulator. Pass `db_dir` of a previous System to RESTART on
    its database (and `sim=` to keep a venue that survives the restart).
    """
    def __init__(self, rules: Optional[List[RiskRule]] = None, sim_config: Optional[SimConfig] = None,
                 registry=None, extra_venues=(), db_dir: Optional[str] = None, sim: Optional[SimulatedVenue] = None,
                 capital: Optional[Dict[ExecutionMode, float]] = None, max_drawdown_window: float = 0.05,
                 max_drawdown_hwm: float = 0.10):
        self.tmp = db_dir or tempfile.mkdtemp(prefix="trading_test_")
        self.bus = EventBus()
        self.store = SqliteStore(db_path=os.path.join(self.tmp, "test.db"))
        DatabaseJournal(self.bus, self.store)
        self.instruments = make_instruments()
        self.sim = sim or SimulatedVenue(self.instruments, sim_config or SimConfig(latency_median_ms=0, seed=7))
        self.router = VenueRouter([self.sim, *extra_venues])
        self.portfolio = PortfolioManager(self.bus, self.instruments, self.store)
        self.lock = LockManager(self.bus, position_view=self.portfolio.get_position)
        self.recorder = Recorder(self.bus)
        self.market = {"BTCUSDT": {"close": 50_000.0}, "ETHUSDT": {"close": 3_000.0}}
        self.registry = registry if registry is not None else {STRAT: ExecutionMode.SIM}
        self.oms = OrderManagementSystem(
            bus=self.bus, store=self.store, router=self.router, strategy_registry=self.registry,
            risk_manager=PreTradeRiskManager(rules or []), instruments=self.instruments,
            market_view=lambda s: self.market.get(s, {}), position_view=self.portfolio.get_position,
            poll_interval=0.05, unknown_resolution_seconds=0.0,
        )
        self.opm = OpenPositionManager(self.bus, self.lock)
        self.control_file = Path(self.tmp) / "kill_switch.ini"
        self.status_file = Path(self.tmp) / "kill_switch_status.txt"
        modes = {*self.registry.values(), ExecutionMode.SIM}
        self.kill_switch = KillSwitchController(
            self.bus, self.store, oms=self.oms, portfolio=self.portfolio, lock=self.lock, router=self.router,
            instruments=self.instruments, market_view=lambda s: self.market.get(s, {}), modes=modes,
            control_path=self.control_file, status_path=self.status_file,
            poll_seconds=0.02, flatten_retry_seconds=0.02)
        self.oms.halt_view = self.kill_switch.level
        self.supervisor = RiskSupervisor(
            self.bus, self.store, pnl_at_cost=self.portfolio.pnl_at_cost,
            market_view=lambda s: self.market.get(s, {}), kill_switch=self.kill_switch,
            capital=capital or {m: 10_000.0 for m in modes},
            max_drawdown_window=max_drawdown_window, max_drawdown_hwm=max_drawdown_hwm)
        self.rehydration_problems = None
        self._bus_task: Optional[asyncio.Task] = None

    async def __aenter__(self):
        await self.store.start()
        for v in self.router.all():
            await v.start()
        self.rehydration_problems = await Rehydrator(
            self.store, portfolio=self.portfolio, oms=self.oms, lock=self.lock, opm=self.opm, router=self.router,
            instruments=self.instruments, kill_switch=self.kill_switch, supervisor=self.supervisor).run()
        await self.kill_switch.start()
        await self.oms.start()
        self._bus_task = asyncio.create_task(self.bus.run())
        return self

    async def __aexit__(self, *exc):
        await self.settle()
        await self.kill_switch.stop()
        await self.oms.stop(timeout=1)
        for v in self.router.all():
            await v.stop()
        self.bus.stop()
        if self._bus_task:
            await self._bus_task
        await self.store.stop()

    async def settle(self, rounds: int = 50) -> None:
        """Runs until the bus, the OMS's venue tasks and the sim wire are all quiet."""
        quiet = 0
        for _ in range(rounds * 4):
            await self.bus.drain()
            await asyncio.sleep(0.005)
            busy = self.bus.depth or self.oms._tasks or not self.sim._wire.empty()
            quiet = 0 if busy else quiet + 1
            if quiet >= 3:
                return

    def quote(self, symbol="BTCUSDT", bid=49_999.99, ask=50_000.01, bid_qty=1.0, ask_qty=1.0):
        self.sim.on_book_ticker(symbol, bid, bid_qty, ask, ask_qty)

    async def enter(self, it: Intent) -> bool:
        """What a strategy does: acquire the lock synchronously, then publish."""
        from messages.events import IntentIssued
        if not self.lock.try_begin_entry(it):
            return False
        await self.bus.publish(IntentIssued(intent=it))
        return True

    async def exit(self, it: Intent) -> bool:
        from messages.events import EventPriority, IntentIssued
        if not self.lock.try_begin_exit(it):
            return False
        await self.bus.publish(IntentIssued(intent=it, priority=EventPriority.HIGH))
        return True
