"""
connectors/binance/codec.py

All Binance JSON dialect in ONE file: klines, exchangeInfo filters, order
statuses, execution reports. Nothing Binance-flavoured escapes this folder.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from messages.instruments import InstrumentSpec
from messages.model import Bar, Fill, OrderSide, OrderStatus, VenueOrderUpdate

VENUE_ID = "BINANCE"

_INTERVAL_UNITS_MS = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}


def interval_ms(interval: str) -> int:
    """'1m' -> 60000, '4h' -> 14400000. (Monthly '1M' is not supported here.)"""
    return int(interval[:-1]) * _INTERVAL_UNITS_MS[interval[-1]]


def _utc(ms: int | float) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


# =====================================================================
# Market data
# =====================================================================

def bar_from_rest_kline(symbol: str, interval: str, k: List[Any]) -> Bar:
    return Bar(
        symbol=symbol.upper(), venue=VENUE_ID, interval=interval,
        start_time=_utc(k[0]), close_time=_utc(k[6]),
        open=float(k[1]), high=float(k[2]), low=float(k[3]), close=float(k[4]), volume=float(k[5]),
        is_closed=True,
    )


def bar_from_ws_kline(k: Dict[str, Any]) -> Bar:
    return Bar(
        symbol=k["s"], venue=VENUE_ID, interval=k["i"],
        start_time=_utc(k["t"]), close_time=_utc(k["T"]),
        open=float(k["o"]), high=float(k["h"]), low=float(k["l"]), close=float(k["c"]), volume=float(k["v"]),
        is_closed=bool(k["x"]),
    )


def spec_from_symbol_info(info: Dict[str, Any]) -> InstrumentSpec:
    filters = {f["filterType"]: f for f in info.get("filters", [])}
    price_f = filters.get("PRICE_FILTER", {})
    lot_f = filters.get("LOT_SIZE", {})
    mlot_f = filters.get("MARKET_LOT_SIZE", {})
    notional_f = filters.get("NOTIONAL") or filters.get("MIN_NOTIONAL") or {}
    market_step = Decimal(mlot_f["stepSize"]) if mlot_f.get("stepSize") and Decimal(mlot_f["stepSize"]) > 0 else None
    return InstrumentSpec(
        symbol=info["symbol"], venue=VENUE_ID,
        base_asset=info["baseAsset"], quote_asset=info["quoteAsset"],
        tick_size=Decimal(price_f.get("tickSize", "0.01")).normalize(),
        step_size=Decimal(lot_f.get("stepSize", "0.00001")).normalize(),
        min_qty=Decimal(lot_f.get("minQty", "0")).normalize(),
        max_qty=Decimal(lot_f.get("maxQty", "0")).normalize(),
        min_notional=Decimal(notional_f.get("minNotional", "0")).normalize(),
        market_step_size=market_step,
    )


# =====================================================================
# Orders
# =====================================================================

STATUS_MAP = {
    "NEW": OrderStatus.NEW,
    "PENDING_NEW": OrderStatus.NEW,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "PENDING_CANCEL": OrderStatus.PARTIALLY_FILLED,   # still working; resolved on next poll
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.EXPIRED,
    "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,           # self-trade prevention
}


def _blank_fill(client_order_id: str, symbol: str, side: OrderSide, trade_id: str, price: float, qty: float,
                fee: float, fee_asset: str, is_maker: bool, ts: datetime) -> Fill:
    # strategy_id / intent_id are unknown to the adapter; the OMS stamps them.
    return Fill(client_order_id=client_order_id, strategy_id="", intent_id="", symbol=symbol, side=side,
                quantity=qty, price=price, fee=fee, fee_asset=fee_asset, is_maker=is_maker,
                exchange_trade_id=trade_id, timestamp=ts)


def update_from_order_response(data: Dict[str, Any], side: OrderSide) -> VenueOrderUpdate:
    """Parses POST /api/v3/order (FULL) and GET /api/v3/order responses."""
    coid = data.get("clientOrderId") or data.get("origClientOrderId") or ""
    symbol = data.get("symbol", "")
    ts_ms = data.get("transactTime") or data.get("updateTime") or data.get("time")
    ts = _utc(ts_ms) if ts_ms else datetime.now(timezone.utc)
    fills = tuple(
        _blank_fill(coid, symbol, side, str(f["tradeId"]), float(f["price"]), float(f["qty"]),
                    float(f["commission"]), f["commissionAsset"], False, ts)
        for f in data.get("fills", []) if "tradeId" in f
    )
    return VenueOrderUpdate(
        client_order_id=coid,
        status=STATUS_MAP.get(data.get("status", ""), OrderStatus.UNKNOWN),
        venue_order_id=str(data["orderId"]) if "orderId" in data else None,
        fills=fills,
        cumulative_filled_quantity=float(data["executedQty"]) if "executedQty" in data else None,
    )


def fills_from_my_trades(trades: List[Dict[str, Any]], client_order_id: str) -> tuple[Fill, ...]:
    """Parses GET /api/v3/myTrades rows for one order."""
    return tuple(
        _blank_fill(client_order_id, t["symbol"], OrderSide.BUY if t.get("isBuyer") else OrderSide.SELL,
                    str(t["id"]), float(t["price"]), float(t["qty"]), float(t["commission"]),
                    t["commissionAsset"], bool(t.get("isMaker", False)), _utc(t["time"]))
        for t in trades
    )


def error_message(data: Any) -> Optional[str]:
    if isinstance(data, dict) and "msg" in data:
        return f"[{data.get('code')}] {data['msg']}"
    return None
