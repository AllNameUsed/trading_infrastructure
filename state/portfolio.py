"""
state/portfolio.py

The PortfolioManager: the SINGLE WRITER of positions and of the trade journal.

Positions are built from fills (OrderPartiallyFilled / OrderFilled), not from
order completion, so partial fills, partially-filled-then-canceled entries,
and partial exits are all just signed deltas on a virtual ledger keyed by
(strategy_id, symbol). Every change is published as a frozen `Position`
snapshot: PositionOpened / PositionChanged / PositionClosed.

Fee handling mirrors spot exchanges:
  * fee in the BASE asset (Binance default on buys) reduces the quantity
    actually received - the ledger holds what the account really holds, so
    the eventual exit never tries to sell more than exists;
  * fee in the QUOTE asset is converted 1:1;
  * fee in any other asset (e.g. BNB) cannot be valued here and is logged.
Realized PnL is reported net of fees.

Sub-lot residuals ("dust"):
  A base-asset fee leaves a holding that is not a multiple of the lot step
  (buy 0.01 ETH, fee 0.00001 ETH -> hold 0.00999; the exit can only sell 0.0099).
  The ledger NEVER discards it:
    * |holding| < one lot step is FLAT for trading: no Position, the lock goes
      IDLE - but the leftover stays on the ledger;
    * at close it is valued at the closing fill price (so the trade's realized
      PnL is complete) and carried at that price;
    * the next trade starts from it (carried_in_*), and its exit sells everything
      held rounded down, so the residual stays below one step forever instead of
      accumulating.
  Invariant: each ledger equals what its fills put into the account, so the sum of
  ledgers reconciles with the venue balance.

Trade journal (`trades` + `trade_fills`):
  One TradeRecord per round trip. A trade opens with the first fill applied while
  no trade is open on the ledger and closes when the ledger drops below one lot
  step. Every fill is linked to its trade; a fill that flips a position through
  zero is split into a closing leg and an opening leg, each with its share of the
  quantity and fee.
"""

from __future__ import annotations

import dataclasses
import logging
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Dict, List, Optional, Tuple

from messages.events import (
    BarClosed,
    OrderFilled,
    OrderPartiallyFilled,
    PositionChanged,
    PositionClosed,
    PositionOpened,
)
from messages.instruments import InstrumentRegistry
from messages.model import ExecutionMode, Fill, OrderSnapshot, Position, TradeRecord, new_id, utc_now

logger = logging.getLogger("PortfolioManager")

Key = Tuple[str, str]


@dataclass
class PerformanceMetrics:
    """
    Running round-trip statistics for one scope (global or one strategy).
    update(realized_pnl) is called once per PositionClosed; break-even counts as a loss.
    """
    total_closed: int = 0
    winners: int = 0
    losers: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0

    @property
    def win_rate(self) -> float:
        return (self.winners / self.total_closed) * 100.0 if self.total_closed else 0.0

    @property
    def avg_winner(self) -> float:
        return self.gross_profit / self.winners if self.winners else 0.0

    @property
    def avg_loser(self) -> float:
        return self.gross_loss / self.losers if self.losers else 0.0

    def update(self, realized_pnl: float) -> None:
        self.total_closed += 1
        if realized_pnl > 0:
            self.winners += 1
            self.gross_profit += realized_pnl
        else:
            self.losers += 1
            self.gross_loss += abs(realized_pnl)


@dataclass
class _OpenTrade:
    """
    Mutable working record of the trade currently open on one ledger (private to
    the portfolio). `record()` freezes it into the TradeRecord that is persisted.
    """
    trade_id: str
    strategy_id: str
    symbol: str
    venue: str
    mode: ExecutionMode
    direction: int                      # +1 LONG / -1 SHORT (sign of the opening fill)
    entry_time: datetime
    entry_intent_id: str
    entry_ref_price: Optional[float]
    carried_in_qty: float = 0.0
    carried_in_price: float = 0.0
    entry_qty: float = 0.0
    entry_notional: float = 0.0
    exit_qty: float = 0.0
    exit_notional: float = 0.0
    max_quantity: float = 0.0
    num_fills: int = 0
    high: float = -math.inf             # price extremes seen while open (fills + bars)
    low: float = math.inf
    exit_intent_id: Optional[str] = None
    exit_ref_price: Optional[float] = None
    exit_reason: Optional[str] = None

    def observe(self, high: float, low: float) -> None:
        self.high = max(self.high, high)
        self.low = min(self.low, low)

    def add_leg(self, order: OrderSnapshot, fill: Fill, holding_after: float) -> None:
        if fill.side.sign == self.direction:
            self.entry_qty += fill.quantity
            self.entry_notional += fill.quantity * fill.price
        else:
            self.exit_qty += fill.quantity
            self.exit_notional += fill.quantity * fill.price
            self.exit_intent_id = fill.intent_id
            self.exit_ref_price = order.intent.reference_price
            self.exit_reason = order.intent.reason or order.intent.kind.name
        self.num_fills += 1
        self.max_quantity = max(self.max_quantity, abs(holding_after))
        self.observe(fill.price, fill.price)

    def record(self, *, status: str, realized: float, fees: float, residual_qty: float = 0.0,
               residual_price: Optional[float] = None, exit_time: Optional[datetime] = None) -> TradeRecord:
        avg_entry = self.entry_notional / self.entry_qty if self.entry_qty else 0.0
        mae = mfe = 0.0
        if avg_entry and self.low <= self.high:
            if self.direction > 0:
                mfe, mae = max(0.0, self.high - avg_entry), min(0.0, self.low - avg_entry)
            else:
                mfe, mae = max(0.0, avg_entry - self.low), min(0.0, avg_entry - self.high)
        return TradeRecord(
            trade_id=self.trade_id, strategy_id=self.strategy_id, symbol=self.symbol, venue=self.venue,
            mode=self.mode, direction="LONG" if self.direction > 0 else "SHORT", status=status,
            entry_qty=self.entry_qty, exit_qty=self.exit_qty, max_quantity=self.max_quantity,
            avg_entry_price=avg_entry,
            avg_exit_price=self.exit_notional / self.exit_qty if self.exit_qty else None,
            carried_in_qty=self.carried_in_qty, carried_in_price=self.carried_in_price,
            residual_qty=residual_qty, residual_price=residual_price,
            gross_pnl=realized + fees, fees_paid=fees, realized_pnl=realized,
            entry_time=self.entry_time, exit_time=exit_time, num_fills=self.num_fills,
            entry_intent_id=self.entry_intent_id, exit_intent_id=self.exit_intent_id,
            exit_reason=self.exit_reason, entry_ref_price=self.entry_ref_price,
            exit_ref_price=self.exit_ref_price, mae=mae, mfe=mfe,
        )


class PortfolioManager:
    """
    The single writer of positions and trades: per-strategy virtual ledgers built from fills.

    Subscribes:  OrderPartiallyFilled, OrderFilled (order=-50: before the lock reads positions),
                 BarClosed (MAE/MFE of open trades)
    Publishes:   PositionOpened (flat -> open), PositionChanged, PositionClosed (-> below one lot step)
    Depends on:  InstrumentRegistry (base/quote assets, lot step), SqliteStore (positions, trades, trade_fills)

    State:
        _ledgers          Dict[(sid, sym), Position]      every ledger, INCLUDING sub-lot residuals
        _trades           Dict[(sid, sym), _OpenTrade]    the trade open on each ledger
        global_metrics    PerformanceMetrics
        strategy_metrics  Dict[strategy_id, PerformanceMetrics]
    API (read models, synchronous):
        get_position(sid, sym) -> Position|None    only when >= one lot step (used by the OMS and the lock)
        get_open_positions() / net_quantity(sym) / get_residual(sid, sym) / get_strategy_metrics(sid)
    apply_fill math: signed delta = side.sign * qty (- base-asset fee); adding -> weighted
    average entry; reducing -> realized += closed_qty * (px - avg) * direction; a fill that
    flips through zero is split into two legs (two trades); |qty| < lot step -> flat, the
    residual is valued at the fill price and carried into the next trade.
    """
    def __init__(self, bus, instruments: InstrumentRegistry, store=None):
        self.bus = bus
        self.instruments = instruments
        self.store = store
        self._ledgers: Dict[Key, Position] = {}
        self._trades: Dict[Key, _OpenTrade] = {}
        self.global_metrics = PerformanceMetrics()
        self.strategy_metrics: Dict[str, PerformanceMetrics] = {}

        # order=-50: positions update right after the lock, before the OPM/journal react.
        bus.subscribe(OrderPartiallyFilled, self.on_fill_event, order=-50)
        bus.subscribe(OrderFilled, self.on_fill_event, order=-50)
        bus.subscribe(BarClosed, self.on_bar)

    # =================================================================
    # Fill application
    # =================================================================
    async def on_fill_event(self, event: OrderPartiallyFilled | OrderFilled) -> None:
        await self.apply_fill(event.order, event.fill)

    def on_bar(self, event: BarClosed) -> None:
        """Price extremes for MAE/MFE: bars that started at or after the trade's entry."""
        bar = event.bar
        for (_, symbol), trade in self._trades.items():
            if symbol == bar.symbol and bar.start_time >= trade.entry_time:
                trade.observe(bar.high, bar.low)

    async def apply_fill(self, order: OrderSnapshot, fill: Fill) -> None:
        """Applies one fill (all mutation before any await), then persists and publishes in order."""
        effects: List[Awaitable[None]] = []
        for leg in self._split_at_flip(fill):
            effects.extend(self._apply_leg(order, leg))
        for effect in effects:
            await effect

    def _fee_effect(self, fill: Fill) -> Tuple[float, float, bool]:
        """(signed change in base held, fee valued in quote, fee could be valued) for one fill."""
        spec = self.instruments.get(fill.symbol)
        qty_delta = fill.side.sign * fill.quantity
        if not fill.fee:
            return qty_delta, 0.0, True
        if spec and fill.fee_asset == spec.base_asset:
            return qty_delta - fill.fee, fill.fee * fill.price, True    # received less base asset
        if spec and fill.fee_asset == spec.quote_asset:
            return qty_delta, fill.fee, True
        return qty_delta, 0.0, False

    def _is_dust(self, symbol: str, qty: float) -> bool:
        return self.instruments.is_qty_dust(symbol, qty)

    def _split_at_flip(self, fill: Fill) -> List[Fill]:
        """
        A fill that takes an open ledger through zero to an open ledger on the other side
        closes one trade and opens another: split it into the leg that lands exactly on
        zero and the remainder, sharing the fee pro rata.
        """
        ledger = self._ledgers.get((fill.strategy_id, fill.symbol))
        old_qty = ledger.quantity if ledger else 0.0
        if self._is_dust(fill.symbol, old_qty):
            return [fill]
        qty_delta, _, _ = self._fee_effect(fill)
        new_qty = old_qty + qty_delta
        if self._is_dust(fill.symbol, new_qty) or (new_qty > 0) == (old_qty > 0):
            return [fill]
        closing_qty = -old_qty / (qty_delta / fill.quantity)
        closing = dataclasses.replace(fill, quantity=closing_qty, fee=fill.fee * closing_qty / fill.quantity)
        opening = dataclasses.replace(fill, quantity=fill.quantity - closing_qty, fee=fill.fee - closing.fee)
        logger.warning(f"[PORTFOLIO] ({fill.strategy_id}, {fill.symbol}) FLIPPED through zero: "
                       f"{old_qty:.8g} -> {new_qty:.8g} (fill split across two trades)")
        return [closing, opening]

    def _apply_leg(self, order: OrderSnapshot, fill: Fill) -> List[Awaitable[None]]:
        """Synchronous ledger + trade mutation for one leg; returns the I/O to run afterwards, in order."""
        key = (fill.strategy_id, fill.symbol)
        qty_delta, fee_quote, valued = self._fee_effect(fill)
        if not valued:
            logger.warning(f"[PORTFOLIO] Fee {fill.fee} {fill.fee_asset} on {fill.exchange_trade_id} "
                           f"cannot be valued in the quote asset; excluded from PnL.")

        now = utc_now()
        prev = self._ledgers.get(key)
        if prev is None:
            prev = Position(strategy_id=fill.strategy_id, symbol=fill.symbol, venue=order.request.venue,
                            mode=order.mode, quantity=0.0, average_entry_price=0.0,
                            opened_at=fill.timestamp, updated_at=now)
        trade = self._trades.get(key)
        if trade is None:
            trade = self._start_trade(order, fill, prev)
            prev = dataclasses.replace(prev, realized_pnl=0.0, fees_paid=0.0, opened_at=fill.timestamp,
                                       trade_id=trade.trade_id)

        old_qty, old_avg = prev.quantity, prev.average_entry_price
        new_qty = round(old_qty + qty_delta, 12)   # strip float residue; far below any lot step
        realized = prev.realized_pnl - fee_quote
        if old_qty == 0 or (old_qty > 0) == (qty_delta > 0):
            # Opening or adding: weighted average entry (fees tracked separately).
            gross = abs(old_qty) + abs(qty_delta)
            new_avg = (abs(old_qty) * old_avg + abs(qty_delta) * fill.price) / gross if gross else 0.0
        else:
            # Reducing (through zero only into a sub-lot residual - real flips were split).
            closed = min(abs(qty_delta), abs(old_qty))
            realized += closed * (fill.price - old_avg) * (1 if old_qty > 0 else -1)
            new_avg = fill.price if abs(qty_delta) > abs(old_qty) else old_avg

        pos = dataclasses.replace(prev, quantity=new_qty, average_entry_price=new_avg if new_qty else 0.0,
                                  realized_pnl=realized, fees_paid=prev.fees_paid + fee_quote, updated_at=now)
        trade.add_leg(order, fill, new_qty)
        effects: List[Awaitable[None]] = [self._link(trade.trade_id, fill)]

        was_open = not self._is_dust(fill.symbol, old_qty)
        now_open = not self._is_dust(fill.symbol, new_qty)
        if was_open and not now_open:
            return effects + self._close(order, fill, trade, pos)

        self._ledgers[key] = pos
        record = trade.record(status="OPEN", realized=pos.realized_pnl, fees=pos.fees_paid)
        if now_open and not was_open:
            logger.info(f"[PORTFOLIO] OPEN {key} qty={new_qty:.8g} @ {new_avg:.8g}"
                        + (f" (incl. carried residual {trade.carried_in_qty:.8g})" if trade.carried_in_qty else ""))
            effects += [self._persist(pos, "OPEN"), self._save_trade(record),
                        self.bus.publish(PositionOpened(position=pos))]
        elif now_open:
            effects += [self._persist(pos, "OPEN"), self._save_trade(record),
                        self.bus.publish(PositionChanged(position=pos, delta_quantity=new_qty - old_qty))]
        else:
            # Sub-lot fill on a flat ledger: below one lot step, nothing to trade or announce yet.
            logger.info(f"[PORTFOLIO] {key} holds a sub-lot quantity {new_qty:.8g}; no position opened.")
            effects.append(self._save_trade(record))
        return effects

    def _start_trade(self, order: OrderSnapshot, fill: Fill, ledger: Position) -> _OpenTrade:
        """Opens the journal record; a sub-lot residual on the ledger becomes its carried-in quantity."""
        trade = _OpenTrade(
            trade_id=new_id(), strategy_id=fill.strategy_id, symbol=fill.symbol, venue=ledger.venue,
            mode=ledger.mode, direction=fill.side.sign, entry_time=fill.timestamp,
            entry_intent_id=fill.intent_id, entry_ref_price=order.intent.reference_price,
            carried_in_qty=ledger.quantity,
            carried_in_price=ledger.average_entry_price if ledger.quantity else 0.0,
        )
        self._trades[(fill.strategy_id, fill.symbol)] = trade
        return trade

    def _close(self, order: OrderSnapshot, fill: Fill, trade: _OpenTrade, pos: Position) -> List[Awaitable[None]]:
        """Ledger fell below one lot step: value and carry the residual, finalize the trade."""
        key = (pos.strategy_id, pos.symbol)
        residual = pos.quantity
        realized = pos.realized_pnl
        if residual:
            # Value the leftover at the closing price: the trade's PnL is complete and the next
            # trade inherits the residual at that price.
            realized += residual * (fill.price - pos.average_entry_price)
            self._ledgers[key] = dataclasses.replace(pos, average_entry_price=fill.price, realized_pnl=0.0,
                                                     fees_paid=0.0, trade_id=None)
        else:
            self._ledgers.pop(key, None)
        self._trades.pop(key, None)

        closed = dataclasses.replace(pos, quantity=0.0, average_entry_price=0.0, realized_pnl=realized,
                                     residual_quantity=residual)
        record = trade.record(status="CLOSED", realized=realized, fees=pos.fees_paid, residual_qty=residual,
                              residual_price=fill.price, exit_time=fill.timestamp)
        logger.info(f"[PORTFOLIO] CLOSED {key} realized={realized:.4f} (net of {pos.fees_paid:.4f} fees)"
                    + (f" | residual {residual:.8g} carried @ {fill.price:.8g}" if residual else ""))
        self._record_round_trip(closed)
        return [self._persist(closed, "CLOSED"), self._save_trade(record),
                self.bus.publish(PositionClosed(position=closed, realized_pnl=realized,
                                                exit_reason=order.intent.reason or order.intent.kind.name))]

    async def _persist(self, pos: Position, status: str) -> None:
        if self.store is not None:
            await self.store.save_position(pos, status)

    async def _save_trade(self, record: TradeRecord) -> None:
        if self.store is not None:
            await self.store.save_trade(record)

    async def _link(self, trade_id: str, fill: Fill) -> None:
        if self.store is not None:
            await self.store.link_trade_fill(trade_id, fill)

    def _record_round_trip(self, pos: Position) -> None:
        self.global_metrics.update(pos.realized_pnl)
        self.strategy_metrics.setdefault(pos.strategy_id, PerformanceMetrics()).update(pos.realized_pnl)
        strat = self.strategy_metrics[pos.strategy_id]
        glob = self.global_metrics
        logger.info(f"{'=' * 50}")
        logger.info(f"TRADE CLOSED | STRATEGY: {pos.strategy_id} | PnL: {pos.realized_pnl:+.4f}")
        logger.info(f"Strategy Stats -> Win Rate: {strat.win_rate:.1f}% | "
                    f"Avg Win: {strat.avg_winner:.4f} | Avg Loss: {strat.avg_loser:.4f}")
        logger.info(f"Global Stats   -> Total Trades: {glob.total_closed} | "
                    f"Win Rate: {glob.win_rate:.1f}% | Net PnL: {(glob.gross_profit - glob.gross_loss):.4f}")
        logger.info(f"{'=' * 50}")

    # =================================================================
    # Read-only APIs
    # =================================================================
    def get_position(self, strategy_id: str, symbol: str) -> Optional[Position]:
        """The open position, or None when flat (a sub-lot residual is flat for trading)."""
        pos = self._ledgers.get((strategy_id, symbol))
        if pos is None or self._is_dust(symbol, pos.quantity):
            return None
        return pos

    def get_open_positions(self) -> List[Position]:
        return [p for p in self._ledgers.values() if not self._is_dust(p.symbol, p.quantity)]

    def get_residual(self, strategy_id: str, symbol: str) -> float:
        """The sub-lot quantity held while flat (0.0 when none or while a position is open)."""
        pos = self._ledgers.get((strategy_id, symbol))
        return pos.quantity if pos is not None and self._is_dust(symbol, pos.quantity) else 0.0

    def net_quantity(self, symbol: str) -> float:
        """Everything held across strategies, residuals included: what the account should hold."""
        return sum(p.quantity for p in self._ledgers.values() if p.symbol == symbol)

    def get_strategy_metrics(self, strategy_id: str) -> PerformanceMetrics:
        return self.strategy_metrics.get(strategy_id, PerformanceMetrics())
