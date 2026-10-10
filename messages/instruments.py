"""
messages/instruments.py

Instrument metadata and the exchange filters every order must satisfy.

The OMS rounds every Intent through this registry before building an
OrderRequest, venues format prices/quantities with it, and the simulated venue
enforces the same filters the real exchange would. Rounding uses Decimal so
that 0.1 + 0.2 style float artefacts can never produce an off-tick price.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Dict, Iterable, Optional

logger = logging.getLogger("Instruments")


@dataclass(frozen=True, slots=True, kw_only=True)
class InstrumentSpec:
    """
    One tradable instrument and its exchange filters (Binance exchangeInfo).
      base_asset / quote_asset   BTC / USDT for BTCUSDT - drive balance and fee accounting
      tick_size                  PRICE_FILTER: every price is a multiple of this
      step_size                  LOT_SIZE: every quantity is a multiple of this
      min_qty / max_qty          LOT_SIZE bounds
      min_notional               NOTIONAL: qty * price must reach this (quote units)
      market_step_size           MARKET_LOT_SIZE override for market orders, if any
    """
    symbol: str
    venue: str
    base_asset: str
    quote_asset: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    market_step_size: Optional[Decimal] = None   # MARKET_LOT_SIZE, when the venue defines one


def _dec(x: float | str | Decimal) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def _quantize(value: float, increment: Decimal, rounding: str) -> Decimal:
    if increment <= 0:
        return _dec(value)
    units = (_dec(value) / increment).to_integral_value(rounding=rounding)
    return (units * increment).normalize()


def _fmt(d: Decimal) -> str:
    # Plain (non-scientific) string with no trailing zeros: Binance rejects "1E-5".
    s = format(d.normalize(), "f")
    return s if s else "0"


class InstrumentRegistry:
    """
    Symbol -> InstrumentSpec lookup plus every rounding/formatting/validation rule.
    Built once in main.py from exchangeInfo (default_spec() fallback) and shared,
    read-only, by the OMS, both venues and the PortfolioManager.

    State:
        _specs: Dict[str, InstrumentSpec]       keyed by upper-case symbol
    API:
        add(spec) / get(sym) -> spec|None / require(sym) -> spec (KeyError) / symbols()
        round_qty(sym, qty, market=False)       DOWN to the lot step (never oversize)
        snap_qty(sym, qty)                      nearest step (clean float drift)
        round_price(sym, px, side_sign)         BUY down / SELL up / 0 nearest (never more aggressive)
        is_qty_dust(sym, qty)                   |qty| < one step -> economically flat
        format_qty / format_price(sym, x)       plain decimal strings for the wire
        validate(sym, qty, px, market) -> str|None   Binance-style filter failure reason
    """

    def __init__(self, specs: Iterable[InstrumentSpec] = ()):
        self._specs: Dict[str, InstrumentSpec] = {}
        for s in specs:
            self.add(s)

    def add(self, spec: InstrumentSpec) -> None:
        self._specs[spec.symbol.upper()] = spec

    def get(self, symbol: str) -> Optional[InstrumentSpec]:
        return self._specs.get(symbol.upper())

    def require(self, symbol: str) -> InstrumentSpec:
        spec = self.get(symbol)
        if spec is None:
            raise KeyError(f"Instrument {symbol} is not registered")
        return spec

    def __contains__(self, symbol: str) -> bool:
        return symbol.upper() in self._specs

    def symbols(self) -> list[str]:
        return list(self._specs)

    # -----------------------------------------------------------------
    # Rounding (float in, float out - the domain stays float-based)
    # -----------------------------------------------------------------
    def round_qty(self, symbol: str, qty: float, *, market: bool = False) -> float:
        """Always rounds DOWN: never send more than was sized."""
        spec = self.require(symbol)
        step = spec.market_step_size if (market and spec.market_step_size) else spec.step_size
        return float(_quantize(abs(qty), step, ROUND_DOWN))

    def snap_qty(self, symbol: str, qty: float) -> float:
        """Nearest lot step - removes float drift from arithmetic on already-aligned quantities."""
        spec = self.require(symbol)
        return float(_quantize(qty, spec.step_size, ROUND_HALF_UP))

    def round_price(self, symbol: str, price: float, *, side_sign: int = 0) -> float:
        """
        side_sign = +1 (BUY) rounds down, -1 (SELL) rounds up: the rounded
        limit is never more aggressive than requested. 0 rounds to nearest.
        """
        spec = self.require(symbol)
        rounding = ROUND_DOWN if side_sign > 0 else ROUND_UP if side_sign < 0 else ROUND_HALF_UP
        return float(_quantize(price, spec.tick_size, rounding))

    def is_qty_dust(self, symbol: str, qty: float) -> bool:
        """True if |qty| is below one lot step - i.e. economically flat."""
        spec = self.get(symbol)
        if spec is None:
            return abs(qty) < 1e-12
        return _dec(abs(qty)) < spec.step_size

    # -----------------------------------------------------------------
    # Wire formatting
    # -----------------------------------------------------------------
    def format_qty(self, symbol: str, qty: float) -> str:
        spec = self.require(symbol)
        return _fmt(_quantize(qty, spec.step_size, ROUND_DOWN))

    def format_price(self, symbol: str, price: float) -> str:
        spec = self.require(symbol)
        return _fmt(_quantize(price, spec.tick_size, ROUND_HALF_UP))

    # -----------------------------------------------------------------
    # Filter validation (shared by the simulator and pre-trade checks)
    # -----------------------------------------------------------------
    def validate(self, symbol: str, qty: float, price: Optional[float], *, market: bool = False) -> Optional[str]:
        """Returns None if the order passes the exchange filters, else a Binance-style reason."""
        spec = self.get(symbol)
        if spec is None:
            return f"Invalid symbol {symbol}."
        q = _dec(qty)
        step = spec.market_step_size if (market and spec.market_step_size) else spec.step_size
        if q < spec.min_qty or (spec.max_qty > 0 and q > spec.max_qty):
            return "Filter failure: LOT_SIZE"
        if step > 0 and (q / step) != (q / step).to_integral_value():
            return "Filter failure: LOT_SIZE"
        if price is not None:
            p = _dec(price)
            if p <= 0:
                return "Filter failure: PRICE_FILTER"
            if spec.tick_size > 0 and (p / spec.tick_size) != (p / spec.tick_size).to_integral_value():
                return "Filter failure: PRICE_FILTER"
            if q * p < spec.min_notional:
                return "Filter failure: NOTIONAL"
        return None


def default_spec(symbol: str, venue: str = "BINANCE", quote_asset: str = "USDT") -> InstrumentSpec:
    """Conservative fallback used only when exchangeInfo cannot be fetched."""
    sym = symbol.upper()
    base = sym[: -len(quote_asset)] if sym.endswith(quote_asset) else sym
    return InstrumentSpec(
        symbol=sym, venue=venue, base_asset=base, quote_asset=quote_asset,
        tick_size=Decimal("0.01"), step_size=Decimal("0.00001"),
        min_qty=Decimal("0.00001"), max_qty=Decimal("9000"),
        min_notional=Decimal("5"),
    )
