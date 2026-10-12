"""
risk/supervisor.py

The RiskSupervisor: portfolio-wide, real-time risk. The SINGLE WRITER of the
`strategy_pnl` and `portfolio_equity` tables.

Books. One book per execution mode (SIM, TESTNET, LIVE), each with its own capital, so a
paper loss can never halt real money:
    equity(mode) = capital(mode) + sum of total_pnl over the mode's (strategy, symbol) pairs
    total_pnl    = realized (closed trades + the open trade's realized part, net of fees)
                   + unrealized (ledger quantity, sub-lot residual included, x (mark - avg entry))
The unmarked totals come from PortfolioManager.pnl_at_cost(), which changes only inside the
portfolio's synchronous fill mutation - a closed trade is never counted twice or not at all.
The mark is the DataProcessor's latest close for the symbol.

Rules (evaluated on every bar and every position change; both trip HALT_COMPLETE):
    drawdown_window   equity is >= max_drawdown_window below its PEAK within the trailing
                      `window_hours` (peak-to-now, so a rally given back inside the window counts)
    drawdown_hwm      equity is >= max_drawdown_hwm below its all-time high-water mark
                      (the slow-bleed backstop a rolling window cannot see)
Both peaks restart from the current equity when the operator resets a halt (the operator has
accepted the loss) - otherwise a reset would re-trip at once.

Tracking (from day one, used or not): on every live bar close of a symbol, one strategy_pnl row
per pair on that symbol and one portfolio_equity row per book, so "which strategy is
underperforming on which asset" is a query, for the operator and for future rules.
"""

from __future__ import annotations

import dataclasses
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Deque, Dict, List, Optional, Tuple

from messages.events import BarClosed, PositionChanged, PositionClosed, PositionOpened
from messages.model import EquitySnapshot, ExecutionMode, HaltLevel, StrategyPnL, utc_now

logger = logging.getLogger("RiskSupervisor")


@dataclass
class _Book:
    """One execution mode's equity tracking."""
    capital: float
    window: Deque[Tuple[float, float]] = field(default_factory=deque)   # (epoch s, equity), decreasing
    hwm: Optional[float] = None
    last: Optional[EquitySnapshot] = None

    def observe(self, ts: float, equity: float, horizon_s: float) -> Tuple[float, float]:
        """Adds a sample; returns (peak within the window, high-water mark)."""
        while self.window and self.window[-1][1] <= equity:
            self.window.pop()                 # monotonic: the front is always the window's max
        self.window.append((ts, equity))
        while self.window[0][0] < ts - horizon_s:
            self.window.popleft()
        self.hwm = equity if self.hwm is None else max(self.hwm, equity)
        return self.window[0][1], self.hwm


class RiskSupervisor:
    """
    Portfolio-wide drawdown rules + per-(mode, strategy, symbol) PnL tracking.

    Subscribes:  BarClosed (order=50: after the DataProcessor updated the mark),
                 PositionOpened / PositionChanged / PositionClosed (order=50)
    Writes:      strategy_pnl, portfolio_equity (via the store's single writer)
    Depends on:  pnl_at_cost() -> [StrategyPnL]    (PortfolioManager)
                 market_view(symbol) -> {"close"}  (DataProcessor read model)
                 kill_switch                       (trigger, level, last_reset, add_reset_listener)
    API:
        rehydrate()            startup: rebuild each book's window and high-water mark from history
        evaluate() -> {mode: EquitySnapshot}       mark, update peaks, trip rules
        marked_pnl() -> [StrategyPnL]              the current per-pair report
        rebaseline(mode)       peaks restart at the current equity (kill switch reset listener)
    """

    def __init__(self, bus, store, *, pnl_at_cost: Callable[[], List[StrategyPnL]],
                 market_view: Callable[[str], dict], kill_switch, capital: Dict[ExecutionMode, float],
                 window_hours: float = 24.0, max_drawdown_window: float = 0.05,
                 max_drawdown_hwm: float = 0.10, bar_interval_ms: int = 60_000):
        for mode, cap in capital.items():
            if cap <= 0:
                raise ValueError(f"Risk capital for {mode.name} must be > 0 (got {cap})")
        self.bus = bus
        self.store = store
        self.pnl_at_cost = pnl_at_cost
        self.market_view = market_view
        self.kill_switch = kill_switch
        self.window_s = window_hours * 3600.0
        self.max_dd_window = max_drawdown_window
        self.max_dd_hwm = max_drawdown_hwm
        self.bar_interval_ms = bar_interval_ms
        self._books: Dict[ExecutionMode, _Book] = {m: _Book(capital=c) for m, c in capital.items()}
        self._untracked: set = set()

        bus.subscribe(BarClosed, self.on_bar, order=50)
        for event_type in (PositionOpened, PositionChanged, PositionClosed):
            bus.subscribe(event_type, self.on_position, order=50)
        kill_switch.add_reset_listener(self.rebaseline)
        logger.info(f"RiskSupervisor: books {', '.join(f'{m.name} capital {c:,.2f}' for m, c in capital.items())}; "
                    f"HALT_COMPLETE at {max_drawdown_window:.1%} below the {window_hours:g}h peak or "
                    f"{max_drawdown_hwm:.1%} below the high-water mark.")

    # =================================================================
    # Marking
    # =================================================================
    def _mark(self, line: StrategyPnL) -> StrategyPnL:
        px = (self.market_view(line.symbol) or {}).get("close")
        if not px or not line.position_qty:
            return dataclasses.replace(line, mark_price=float(px) if px else None)
        unrealized = line.position_qty * (float(px) - line.avg_entry_price)
        return dataclasses.replace(line, mark_price=float(px), unrealized_pnl=unrealized,
                                   total_pnl=line.realized_pnl + unrealized)

    def marked_pnl(self) -> List[StrategyPnL]:
        lines = [self._mark(line) for line in self.pnl_at_cost()]
        for line in lines:
            if line.mode not in self._books and line.mode not in self._untracked:
                self._untracked.add(line.mode)
                logger.warning(f"PnL in {line.mode.name} is not tracked by any book (no risk capital configured).")
        return lines

    # =================================================================
    # Evaluation
    # =================================================================
    async def evaluate(self, lines: Optional[List[StrategyPnL]] = None,
                       now: Optional[datetime] = None) -> Dict[ExecutionMode, EquitySnapshot]:
        lines = self.marked_pnl() if lines is None else lines
        now = now or utc_now()
        out: Dict[ExecutionMode, EquitySnapshot] = {}
        for mode, book in self._books.items():
            total = sum(line.total_pnl for line in lines if line.mode is mode)
            equity = book.capital + total
            peak, hwm = book.observe(now.timestamp(), equity, self.window_s)
            dd_window = max(0.0, 1.0 - equity / peak) if peak > 0 else 0.0
            dd_hwm = max(0.0, 1.0 - equity / hwm) if hwm > 0 else 0.0
            snap = EquitySnapshot(timestamp=now, mode=mode, capital=book.capital, total_pnl=total, equity=equity,
                                  peak_window=peak, high_watermark=hwm, drawdown_window=dd_window,
                                  drawdown_hwm=dd_hwm, halt_level=self.kill_switch.level(mode))
            book.last = out[mode] = snap
            if self.kill_switch.level(mode) is HaltLevel.HALT_COMPLETE:
                continue
            if dd_window >= self.max_dd_window:
                await self.kill_switch.trigger(
                    mode, HaltLevel.HALT_COMPLETE, "drawdown_window",
                    f"equity {equity:,.2f} is {dd_window:.2%} below its {self.window_s / 3600:g}h peak "
                    f"{peak:,.2f} (limit {self.max_dd_window:.1%})")
            elif dd_hwm >= self.max_dd_hwm:
                await self.kill_switch.trigger(
                    mode, HaltLevel.HALT_COMPLETE, "drawdown_hwm",
                    f"equity {equity:,.2f} is {dd_hwm:.2%} below its high-water mark {hwm:,.2f} "
                    f"(limit {self.max_dd_hwm:.1%})")
        return out

    def rebaseline(self, mode: ExecutionMode) -> None:
        """Operator reset: both peaks restart from the current equity."""
        book = self._books.get(mode)
        if book is None:
            return
        total = sum(line.total_pnl for line in self.marked_pnl() if line.mode is mode)
        equity = book.capital + total
        book.window.clear()
        book.hwm = None
        book.observe(utc_now().timestamp(), equity, self.window_s)
        logger.warning(f"[RISK] {mode.name}: drawdown peaks re-baselined at equity {equity:,.2f} after the reset.")

    # =================================================================
    # Bus handlers
    # =================================================================
    async def on_bar(self, event: BarClosed) -> None:
        lines = self.marked_pnl()
        snaps = await self.evaluate(lines)
        if event.replay:
            return     # history: evaluated, but the time series only records live closes
        ts = event.bar.start_time + timedelta(milliseconds=self.bar_interval_ms)
        for line in lines:
            if line.symbol == event.bar.symbol:
                await self.store.save_strategy_pnl(dataclasses.replace(line, timestamp=ts))
        for snap in snaps.values():
            await self.store.save_equity(dataclasses.replace(snap, timestamp=ts))

    async def on_position(self, event) -> None:
        snaps = await self.evaluate()
        if isinstance(event, PositionClosed):
            snap = snaps.get(event.position.mode)
            if snap is not None:
                logger.info(f"[RISK] {snap.mode.name} book: equity {snap.equity:,.4f} (PnL {snap.total_pnl:+.4f}) | "
                            f"drawdown {snap.drawdown_window:.3%} vs {self.window_s / 3600:g}h peak, "
                            f"{snap.drawdown_hwm:.3%} vs high-water mark")

    # =================================================================
    # Startup
    # =================================================================
    async def rehydrate(self) -> None:
        """Rebuilds each book's trailing window and high-water mark from portfolio_equity (since the last reset)."""
        now = utc_now()
        for mode, book in self._books.items():
            reset = self.kill_switch.last_reset(mode)
            reset_ms = int(reset.timestamp() * 1000) if reset else 0
            since_ms = max(int((now.timestamp() - self.window_s) * 1000), reset_ms)
            for r in await self.store.get_equity_history(mode, since_ms):
                book.observe(r["timestamp"] / 1000.0, book.capital + r["total_pnl"], self.window_s)
            best = await self.store.get_max_total_pnl(mode, reset_ms)
            if best is not None:
                book.hwm = max(book.hwm or -float("inf"), book.capital + best)
        snaps = await self.evaluate(now=now)
        for snap in snaps.values():
            logger.info(f"[RISK] {snap.mode.name} book rehydrated: equity {snap.equity:,.4f}, "
                        f"{self.window_s / 3600:g}h peak {snap.peak_window:,.4f} ({snap.drawdown_window:.3%} below), "
                        f"high-water mark {snap.high_watermark:,.4f} ({snap.drawdown_hwm:.3%} below)")
