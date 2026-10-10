"""
state/portfolio.py

The PortfolioManager: the SINGLE WRITER of positions.

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
"""

from __future__ import annotations

import dataclasses
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from messages.events import OrderFilled, OrderPartiallyFilled, PositionChanged, PositionClosed, PositionOpened
from messages.instruments import InstrumentRegistry
from messages.model import Fill, OrderSnapshot, Position, utc_now

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


class PortfolioManager:
    """
    The single writer of positions: per-strategy virtual ledgers built from fills.

    Subscribes:  OrderPartiallyFilled, OrderFilled (order=-50: before the lock reads positions)
    Publishes:   PositionOpened (0 -> non-zero), PositionChanged, PositionClosed (-> flat)
    Depends on:  InstrumentRegistry (base/quote assets, dust threshold), SqliteStore (positions table)

    State:
        _positions        Dict[(strategy_id, symbol), Position]   open ledgers only
        global_metrics    PerformanceMetrics
        strategy_metrics  Dict[strategy_id, PerformanceMetrics]
    API (read models, synchronous):
        get_position(sid, sym) -> Position|None    used by the OMS (reduce-only) and the lock
        get_open_positions() / net_quantity(sym) / get_strategy_metrics(sid)
    apply_fill math: signed delta = side.sign * qty (- base-asset fee); adding -> weighted
    average entry; reducing -> realized += closed_qty * (px - avg) * direction; flipping
    through zero -> remainder opens at the fill price; |qty| < lot step -> flat (dust).
    """
    def __init__(self, bus, instruments: InstrumentRegistry, store=None):
        self.bus = bus
        self.instruments = instruments
        self.store = store
        self._positions: Dict[Key, Position] = {}
        self.global_metrics = PerformanceMetrics()
        self.strategy_metrics: Dict[str, PerformanceMetrics] = {}

        # order=-50: positions update right after the lock, before the OPM/journal react.
        bus.subscribe(OrderPartiallyFilled, self.on_fill_event, order=-50)
        bus.subscribe(OrderFilled, self.on_fill_event, order=-50)

    # =================================================================
    # Fill application
    # =================================================================
    async def on_fill_event(self, event: OrderPartiallyFilled | OrderFilled) -> None:
        await self.apply_fill(event.order, event.fill)

    async def apply_fill(self, order: OrderSnapshot, fill: Fill) -> None:
        """Applies one fill to its ledger (mutation completes before any await), persists, publishes."""
        key = (fill.strategy_id, fill.symbol)
        spec = self.instruments.get(fill.symbol)
        base = spec.base_asset if spec else None
        quote = spec.quote_asset if spec else None

        qty_delta = fill.side.sign * fill.quantity
        fee_quote = 0.0
        if fill.fee:
            if base and fill.fee_asset == base:
                qty_delta -= fill.fee            # received less base asset
                fee_quote = fill.fee * fill.price
            elif quote and fill.fee_asset == quote:
                fee_quote = fill.fee
            else:
                logger.warning(f"[PORTFOLIO] Fee {fill.fee} {fill.fee_asset} on {fill.exchange_trade_id} "
                               f"cannot be valued in {quote}; excluded from PnL.")

        prev = self._positions.get(key)
        now = utc_now()
        if prev is None:
            prev = Position(strategy_id=fill.strategy_id, symbol=fill.symbol, venue=order.request.venue,
                            mode=order.mode, quantity=0.0, average_entry_price=0.0,
                            opened_at=fill.timestamp, updated_at=now)

        old_qty, old_avg = prev.quantity, prev.average_entry_price
        new_qty = old_qty + qty_delta
        realized = prev.realized_pnl - fee_quote
        if old_qty == 0 or (old_qty > 0) == (qty_delta > 0):
            # Opening or adding: weighted average entry (fees tracked separately).
            gross = abs(old_qty) + abs(qty_delta)
            new_avg = (abs(old_qty) * old_avg + abs(qty_delta) * fill.price) / gross if gross else 0.0
        else:
            # Reducing (possibly through zero).
            closed = min(abs(qty_delta), abs(old_qty))
            realized += closed * (fill.price - old_avg) * (1 if old_qty > 0 else -1)
            if abs(qty_delta) > abs(old_qty):     # flipped: remainder opens at the fill price
                new_avg = fill.price
            else:
                new_avg = old_avg

        if self.instruments.is_qty_dust(fill.symbol, new_qty):
            new_qty = 0.0

        pos = dataclasses.replace(prev, quantity=new_qty, average_entry_price=new_avg if new_qty else 0.0,
                                  realized_pnl=realized, fees_paid=prev.fees_paid + fee_quote, updated_at=now)

        if old_qty == 0 and new_qty != 0:
            self._positions[key] = pos
            logger.info(f"[PORTFOLIO] OPEN {key} qty={new_qty:.8g} @ {new_avg:.8g}")
            await self._persist(pos, "OPEN")
            await self.bus.publish(PositionOpened(position=pos))
        elif new_qty == 0:
            self._positions.pop(key, None)
            logger.info(f"[PORTFOLIO] CLOSED {key} realized={realized:.4f} (net of {pos.fees_paid:.4f} fees)")
            await self._persist(pos, "CLOSED")
            self._record_round_trip(pos)
            await self.bus.publish(PositionClosed(position=pos, realized_pnl=realized,
                                                  exit_reason=order.intent.reason or order.intent.kind.name))
        else:
            self._positions[key] = pos
            if (old_qty > 0) != (new_qty > 0):
                logger.warning(f"[PORTFOLIO] {key} FLIPPED through zero: {old_qty:.8g} -> {new_qty:.8g}")
            await self._persist(pos, "OPEN")
            await self.bus.publish(PositionChanged(position=pos, delta_quantity=new_qty - old_qty))

    async def _persist(self, pos: Position, status: str) -> None:
        if self.store is not None:
            await self.store.save_position(pos, status)

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
        return self._positions.get((strategy_id, symbol))

    def get_open_positions(self) -> List[Position]:
        return list(self._positions.values())

    def net_quantity(self, symbol: str) -> float:
        return sum(p.quantity for p in self._positions.values() if p.symbol == symbol)

    def get_strategy_metrics(self, strategy_id: str) -> PerformanceMetrics:
        return self.strategy_metrics.get(strategy_id, PerformanceMetrics())
