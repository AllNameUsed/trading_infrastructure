"""
messages/model.py

The pure data structures (nouns) of the trading system - the "arrows" of the
architecture diagram. Every package may import this module; it imports nothing
from the project. If a type is used by two boxes, it lives here.

Layout (in flow order):
  1. Enums           ExecutionMode, OrderSide, OrderType, TimeInForce, OrderStatus,
                     IntentKind, ManagementAction, RejectStage, HaltLevel
  2. Market data     Bar -> MarketParams (-> Signal, planned allocator input)
  3. Commands        Intent (open/close/scale), CancelIntent, ManagementDecision (planned)
  4. Execution       OrderRequest (exchange-shaped), Fill (fact), OrderSnapshot (frozen view),
                     VenueOrderUpdate (what a venue port returns)
  5. Accounting      Position (frozen ledger snapshot), TradeRecord, PositionView (strategy-facing slice)
  6. Risk records    HaltRecord (kill-switch episode), StrategyPnL, EquitySnapshot

Units: quantities are in BASE asset units (BTC for BTCUSDT); prices and money
are in QUOTE asset units (USDT). Floats in the domain; exact Decimal rounding
happens at the exchange boundary (messages/instruments.py).

Rules:
1. Pure Data: No business logic, network requests, or complex calculations.
2. Immutable at the boundary: everything that crosses the Event Bus is a
   frozen dataclass. The only mutable aggregates (`Order` inside the OMS,
   ledgers inside the PortfolioManager) never leave their owner — they publish
   frozen snapshots (`OrderSnapshot`, `Position`) instead. This is what stops a
   subscriber from observing *future* state through a shared reference.
3. Universal Dialect: Exchange-specific terms (e.g., Binance's "origQty")
   are strictly forbidden. Use canonical financial terms.
4. Time: every timestamp is timezone-aware UTC.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, IntEnum, auto
from types import MappingProxyType
from typing import Mapping, Optional, Tuple


def utc_now() -> datetime:
    """The one clock of the system: timezone-aware UTC (naive datetimes are rejected by the store)."""
    return datetime.now(timezone.utc)


def new_id() -> str:
    """32-char hex UUID4 used for intent_id / signal_id / event_id."""
    return uuid.uuid4().hex


# =====================================================================
# 1. Primitives & Enums
# =====================================================================

class ExecutionMode(Enum):
    """
    Where a strategy's orders are routed. Market data ALWAYS comes from mainnet.
      SIM     -> connectors/sim: internal matching engine driven by live mainnet quotes
      TESTNET -> Binance Spot Testnet (API integration testing, NOT strategy evaluation)
      LIVE    -> Binance mainnet, real money
    """
    SIM = "SIM"
    TESTNET = "TESTNET"
    LIVE = "LIVE"


class OrderSide(Enum):
    """Direction of an order or fill. `sign` (+1/-1) turns quantities into signed ledger deltas."""
    BUY = auto()
    SELL = auto()

    @property
    def opposite(self) -> "OrderSide":
        return OrderSide.SELL if self is OrderSide.BUY else OrderSide.BUY

    @property
    def sign(self) -> int:
        return 1 if self is OrderSide.BUY else -1


class OrderType(Enum):
    """
    Binance-Spot-compatible order types.
      MARKET                    take liquidity now, no price
      LIMIT                     rest at `price` (GTC) or take-what-you-can (IOC/FOK)
      STOP_LOSS / TAKE_PROFIT   dormant until the last trade crosses `stop_price`, then MARKET
      *_LIMIT variants          same trigger, then a LIMIT at `price`
    LIMIT_PRICED_TYPES need `price`; STOP_TYPES need `stop_price`.
    """
    MARKET = auto()
    LIMIT = auto()
    STOP_LOSS = auto()
    STOP_LOSS_LIMIT = auto()
    TAKE_PROFIT = auto()
    TAKE_PROFIT_LIMIT = auto()


LIMIT_PRICED_TYPES = frozenset({OrderType.LIMIT, OrderType.STOP_LOSS_LIMIT, OrderType.TAKE_PROFIT_LIMIT})
STOP_TYPES = frozenset({OrderType.STOP_LOSS, OrderType.STOP_LOSS_LIMIT,
                        OrderType.TAKE_PROFIT, OrderType.TAKE_PROFIT_LIMIT})


class TimeInForce(Enum):
    """Lifetime of a LIMIT order's unfilled remainder (ignored for MARKET)."""
    GTC = "GTC"   # Good-til-canceled
    IOC = "IOC"   # Immediate-or-cancel: fill what you can, expire the rest
    FOK = "FOK"   # Fill-or-kill: all or nothing


class OrderStatus(Enum):
    """
    The universal lifecycle states of an order (state machine owned by the OMS):

      PENDING_SUBMIT -> NEW -> PARTIALLY_FILLED -> FILLED
      PENDING_SUBMIT -> FILLED                      (marketable order filled on arrival)
      NEW / PARTIALLY_FILLED -> CANCELED | EXPIRED  (client cancel, IOC/FOK remainder)
      PENDING_SUBMIT -> REJECTED                    (definite refusal)
      PENDING_SUBMIT -> UNKNOWN -> <whatever the venue reports on reconciliation>

    Terminal: FILLED, CANCELED, REJECTED, EXPIRED. UNKNOWN is deliberately NOT
    terminal: the order might exist, so locks stay engaged until it is resolved.
    """
    PENDING_SUBMIT = auto()    # OMS persisted it, sending to venue
    NEW = auto()               # Venue acknowledged, resting on book
    PARTIALLY_FILLED = auto()  # Partial execution received
    FILLED = auto()            # Completely executed
    CANCELED = auto()          # Successfully removed from order book
    REJECTED = auto()          # Venue or OMS denied the order
    EXPIRED = auto()           # IOC/FOK remainder or venue-side expiry
    UNKNOWN = auto()           # Outcome unknown (timeout) - reconciliation in progress

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset({OrderStatus.FILLED, OrderStatus.CANCELED,
                               OrderStatus.REJECTED, OrderStatus.EXPIRED})


class IntentKind(Enum):
    """
    The business meaning of an Intent. `is_entry` kinds are issued by strategies
    holding the lock in PENDING_ENTRY; `is_exit` kinds are issued by the OPM
    holding PENDING_EXIT, may only REDUCE an existing position, and bypass
    entry-only risk rules.
    """
    OPEN = auto()        # Initiating a new position
    CLOSE = auto()       # Closing an existing position
    SCALE_IN = auto()    # Adding to a winner
    SCALE_OUT = auto()   # Taking partial profits
    FLATTEN = auto()     # Emergency liquidation

    @property
    def is_entry(self) -> bool:
        return self in (IntentKind.OPEN, IntentKind.SCALE_IN)

    @property
    def is_exit(self) -> bool:
        return self in (IntentKind.CLOSE, IntentKind.SCALE_OUT, IntentKind.FLATTEN)

    @property
    def code(self) -> str:
        """The one-letter kind code embedded in client order ids (execution/order_ids.py)."""
        return _INTENT_KIND_CODES[self]

    @classmethod
    def from_code(cls, code: str) -> Optional["IntentKind"]:
        return next((k for k, c in _INTENT_KIND_CODES.items() if c == code), None)


_INTENT_KIND_CODES = {IntentKind.OPEN: "O", IntentKind.CLOSE: "C", IntentKind.SCALE_IN: "I",
                      IntentKind.SCALE_OUT: "U", IntentKind.FLATTEN: "F"}


class ManagementAction(Enum):
    """PLANNED: actions an ExitEngine / Strategy.manage() can request for an open position."""
    HOLD = auto()
    CLOSE = auto()
    MOVE_STOP = auto()
    RESIZE = auto()


class RejectStage(Enum):
    """Where in the pipeline an Intent died. Every Intent ends in exactly one terminal event."""
    REGISTRY = auto()    # Strategy not registered
    ROUTING = auto()     # No venue for the strategy's execution mode
    VALIDATION = auto()  # Malformed / rounds to zero / nothing to close
    RISK = auto()        # Pre-trade risk rule fired
    HALTED = auto()      # Kill switch engaged for the strategy's execution mode: no entries
    EXCEPTION = auto()   # Bug while processing - failed closed


class HaltLevel(IntEnum):
    """
    Kill-switch level of one execution mode (risk/kill_switch.py). Ordered: a halt only
    ever ESCALATES automatically; going back to NONE takes an operator reset.
      NONE           trading normally
      HALT_ENTRIES   no new entries; working orders run their course; exits allowed
      HALT_COMPLETE  cancel every working order, flatten every system-attributed position,
                     no entries; exits (incl. the flatten) allowed
    """
    NONE = 0
    HALT_ENTRIES = 1
    HALT_COMPLETE = 2


# =====================================================================
# 2. Market Data & Intelligence (The Ingestion Layer)
# =====================================================================

@dataclass(frozen=True, slots=True, kw_only=True)
class Bar:
    """
    Canonical OHLCV candle. Identity = (symbol, interval, start_time).
    start_time is the inclusive open; close_time = start_time + interval - 1ms
    (Binance convention). Only bars with is_closed=True are ever published.
    """
    symbol: str
    venue: str
    interval: str              # e.g. "1m" - part of the bar's identity
    start_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class MarketParams:
    """
    The enriched data payload delivered to strategies and the OPM (inside ParamsReady).
    Combines the latest closed bar with features computed ONCE per bar by the
    DataProcessor, however many strategies consume them.
      features              {"momentum_10": ..., "sma_50": ...} - the names are the contract
      volatility_estimate   placeholder volatility measure (quote units) for sizing/risk
    `features` is exposed as a read-only mapping: one strategy cannot corrupt
    the features every other consumer sees.
    """
    symbol: str
    latest_bar: Bar
    features: Mapping[str, float]
    volatility_estimate: float

    def __post_init__(self) -> None:
        if not isinstance(self.features, MappingProxyType):
            object.__setattr__(self, "features", MappingProxyType(dict(self.features)))


@dataclass(frozen=True, slots=True, kw_only=True)
class Signal:
    """
    PLANNED (Allocator input): a strategy's opinion without a size.
    Declares an opportunity but carries no execution authority; the future
    risk/allocator.py turns it into a sized OPEN Intent. Today strategies emit
    Intents directly with a fixed quantity.
    """
    strategy_id: str
    symbol: str
    side: OrderSide
    probability: float
    features_snapshot: Mapping[str, float]
    model_version: str
    signal_id: str = field(default_factory=new_id)


# =====================================================================
# 3. Strategy & OPM Commands (The Command Layer)
# =====================================================================

@dataclass(frozen=True, slots=True)
class ManagementDecision:
    """PLANNED: the output of an ExitEngine / Strategy.manage() for an open trade."""
    action: ManagementAction
    target_price: Optional[float] = None
    target_quantity: Optional[float] = None
    reason: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class Intent:
    """
    The immutable blueprint of a business decision (a Command), published in IntentIssued.

    Who creates it: strategies (OPEN, after lock.try_begin_entry) and the OPM
    (CLOSE, after lock.try_begin_exit).
    What happens to it: the OMS validates, rounds, risk-checks and either turns
    it into exactly one order or publishes IntentRejected. Never both, never neither.

      quantity      base units, unrounded (the OMS rounds DOWN to the lot step;
                    exits are clamped to the open position size)
      price         required for LIMIT-priced types; ignored for MARKET
      stop_price    required for STOP types
      intent_id     correlation id linking Intent -> Order -> Fill -> Position rows
      signal_id     link back to the Signal that motivated it (planned)
      reference_price  the price the decision was made on (the bar close the strategy/OPM saw);
                    stamped by BaseStrategy / the OPM, journaled on the trade for slippage analysis
    """
    strategy_id: str
    symbol: str
    venue: str                 # e.g., "BINANCE"
    kind: IntentKind
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    reason: str = ""
    reference_price: Optional[float] = None
    intent_id: str = field(default_factory=new_id)
    signal_id: Optional[str] = None
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True, kw_only=True)
class CancelIntent:
    """
    A request to cancel one working order (published in CancelIssued).
    The OMS refuses it unless `strategy_id` owns `client_order_id`.
    """
    strategy_id: str
    symbol: str
    client_order_id: str
    reason: str = ""


# =====================================================================
# 4. Execution State (The OMS Layer)
# =====================================================================

@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRequest:
    """
    The exchange-shaped, venue-agnostic order - the ONLY thing a venue port receives.
    Built by the OMS from an Intent: quantity/price/stop are already rounded to
    the instrument's lot/tick filters, and `client_order_id` is the idempotency
    key the venue stores (see execution/order_ids.py).
    """
    client_order_id: str
    symbol: str
    venue: str
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.GTC


@dataclass(frozen=True, slots=True, kw_only=True)
class Fill:
    """
    An immutable execution fact. Self-describing: the PortfolioManager can apply
    it to a ledger without looking anything up.

    Venues create fills with blank strategy_id/intent_id; the OMS stamps the
    attribution (and the authoritative side/symbol) before publishing.
      fee / fee_asset     as charged by the venue. On Binance Spot without BNB the
                          fee is taken from the RECEIVED asset (base on buys,
                          quote on sells) - the portfolio models both cases
      exchange_trade_id   the de-duplication key (unique within an order)
    """
    client_order_id: str
    strategy_id: str
    intent_id: str
    symbol: str
    side: OrderSide
    quantity: float       # The discrete delta filled in this execution (always > 0)
    price: float
    fee: float
    fee_asset: str
    is_maker: bool
    exchange_trade_id: str
    timestamp: datetime   # Exchange execution time (UTC)


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderSnapshot:
    """
    Frozen view of an order at one point in its life. This - never the live
    `Order` aggregate (execution/oms.py) - is what order lifecycle events carry,
    so a subscriber can never observe state from the future.
      version           bumped on every OMS transition; the store ignores writes
                        with an older version (order rows only move forward)
      filled_quantity / average_price / cumulative_fee   as of THIS snapshot
    """
    client_order_id: str
    strategy_id: str
    mode: ExecutionMode
    intent: Intent
    request: OrderRequest
    status: OrderStatus
    filled_quantity: float
    average_price: float
    cumulative_fee: float
    venue_order_id: Optional[str]
    version: int
    created_at: datetime
    updated_at: datetime

    @property
    def remaining_quantity(self) -> float:
        return max(self.request.quantity - self.filled_quantity, 0.0)


@dataclass(frozen=True, slots=True, kw_only=True)
class VenueOrderUpdate:
    """
    What an ExecutionVenue reports back about one order (the port's return type).
    `fills` may repeat fills already reported - the OMS de-duplicates by trade id.
    `cumulative_filled_quantity` is the venue's own total; when it exceeds the
    sum of known fills the OMS keeps the order working until the fills arrive.
    """
    client_order_id: str
    status: OrderStatus
    venue_order_id: Optional[str] = None
    fills: Tuple[Fill, ...] = ()
    cumulative_filled_quantity: Optional[float] = None
    reason: str = ""
    timestamp: datetime = field(default_factory=utc_now)


# =====================================================================
# 5. Accounting (The Portfolio Layer)
# =====================================================================

@dataclass(frozen=True, slots=True, kw_only=True)
class Position:
    """
    Frozen snapshot of one virtual ledger (strategy_id, symbol).
    Only the PortfolioManager creates these (published in PositionOpened /
    PositionChanged / PositionClosed). Several strategies can hold independent
    ledgers on the same symbol; the account's net is their sum.
      quantity              SIGNED base units (+ long, - short): the single source of direction
      average_entry_price   quote per base unit, fees excluded
      realized_pnl          quote units, NET of fees, accumulated since opened_at
      fees_paid             quote units (base-asset fees valued at the fill price)
      protective_order_id   PLANNED: the resting stop guarding this position
      trade_id              the `trades` row this position lifetime belongs to
      residual_quantity     PositionClosed only: the sub-lot leftover (< one lot step, "dust")
                            still held on this ledger; it is carried into the next trade
    Includes any carried-in residual: `quantity` is what the account really holds.
    """
    strategy_id: str
    symbol: str
    venue: str
    mode: ExecutionMode
    quantity: float
    average_entry_price: float
    realized_pnl: float = 0.0       # Net of fees, quote currency, since this position opened
    fees_paid: float = 0.0          # Quote currency
    protective_order_id: Optional[str] = None
    opened_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    trade_id: Optional[str] = None
    residual_quantity: float = 0.0

    @property
    def side(self) -> Optional[OrderSide]:
        if self.quantity > 0:
            return OrderSide.BUY
        if self.quantity < 0:
            return OrderSide.SELL
        return None

    @property
    def is_flat(self) -> bool:
        return self.quantity == 0


@dataclass(frozen=True, slots=True, kw_only=True)
class TradeRecord:
    """
    One round trip on one ledger (strategy_id, symbol): the historical journal row
    (`trades` table). Built and written only by the PortfolioManager.

    A trade starts with the first fill applied while no trade is open and ends when
    the ledger drops below one lot step. Quantities are base units, prices and
    money are quote units.
      direction                   "LONG" / "SHORT"
      entry_qty / exit_qty        gross filled quantity on the entry / exit side
      avg_entry_price / _exit_    VWAP of those fills (None until there is an exit fill)
      max_quantity                largest |holding| during the trade
      carried_in_qty / _price     sub-lot residual inherited from the previous trade, and
                                  the price it was carried at (that trade's last exit price)
      residual_qty / _price       sub-lot residual left at close, valued at the closing price
      gross_pnl / fees_paid / realized_pnl   realized = gross - fees; includes the residual
                                  valued at residual_price
      entry/exit_ref_price        decision-time price of the opening / closing intent
      mae / mfe                   max adverse / favourable excursion per unit vs avg_entry_price
                                  (bar resolution: fills + bars that started after entry);
                                  mae <= 0 <= mfe
    """
    trade_id: str
    strategy_id: str
    symbol: str
    venue: str
    mode: ExecutionMode
    direction: str
    status: str                       # OPEN / CLOSED
    entry_qty: float
    exit_qty: float
    max_quantity: float
    avg_entry_price: float
    avg_exit_price: Optional[float]
    carried_in_qty: float
    carried_in_price: float
    residual_qty: float
    residual_price: Optional[float]
    gross_pnl: float
    fees_paid: float
    realized_pnl: float
    entry_time: datetime
    exit_time: Optional[datetime]
    num_fills: int
    entry_intent_id: str
    exit_intent_id: Optional[str]
    exit_reason: Optional[str]
    entry_ref_price: Optional[float]
    exit_ref_price: Optional[float]
    mae: float
    mfe: float

    @property
    def holding_ms(self) -> Optional[int]:
        if self.exit_time is None:
            return None
        return int((self.exit_time - self.entry_time).total_seconds() * 1000)


@dataclass(frozen=True, slots=True, kw_only=True)
class HaltRecord:
    """
    One kill-switch episode of one execution mode (`halts` table; written only by the
    KillSwitchController). Opened by the first trigger, escalated in place, closed by the
    operator quoting `halt_id` in config/kill_switch.ini.
      level         the highest level reached in this episode
      source        manual / drawdown_window / drawdown_hwm / rehydration / breach:<rule>
      flattened_at  HALT_COMPLETE: when no system-attributed exposure or working order remained
    """
    halt_id: str
    mode: ExecutionMode
    level: HaltLevel
    source: str
    reason: str
    triggered_at: datetime
    escalated_at: Optional[datetime] = None
    flattened_at: Optional[datetime] = None
    cleared_at: Optional[datetime] = None
    clear_detail: Optional[str] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class StrategyPnL:
    """
    PnL of one (mode, strategy_id, symbol) pair at one instant (`strategy_pnl` table; written
    only by the RiskSupervisor). Money in quote units; cumulative since the first trade.
      position_qty / avg_entry_price   the ledger, sub-lot residual included
      realized_pnl     closed trades + the open trade's realized part, net of fees
      unrealized_pnl   position_qty * (mark_price - avg_entry_price)
      total_pnl        realized + unrealized: this pair's contribution to its book's equity
    """
    timestamp: datetime
    mode: ExecutionMode
    strategy_id: str
    symbol: str
    position_qty: float
    avg_entry_price: float
    mark_price: Optional[float]
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    fees_paid: float
    closed_trades: int
    winning_trades: int


@dataclass(frozen=True, slots=True, kw_only=True)
class EquitySnapshot:
    """
    One execution mode's book at one instant (`portfolio_equity` table; RiskSupervisor only).
      equity           capital + sum of its pairs' total_pnl
      peak_window      highest equity in the trailing drawdown window (since the last reset)
      high_watermark   highest equity since inception (or the last reset)
      drawdown_*       1 - equity / peak, as fractions (0.05 = 5 %)
    """
    timestamp: datetime
    mode: ExecutionMode
    capital: float
    total_pnl: float
    equity: float
    peak_window: float
    high_watermark: float
    drawdown_window: float
    drawdown_hwm: float
    halt_level: HaltLevel


@dataclass(frozen=True, slots=True)
class PositionView:
    """
    PLANNED: a read-only, sanitized slice of a Position for Strategy.manage(),
    so strategy-directed exits never see routing ids or PnL internals.
    """
    quantity: float
    average_entry_price: float
    bars_held: int
