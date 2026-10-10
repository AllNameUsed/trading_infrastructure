"""
data/processor.py

The Data Processor Facade.
Subscribes to BarClosed, maintains the system's rolling market state (ring
buffers), computes features, and publishes ParamsReady.

Rules:
1. Edge Filtering: drops any symbols not in the active configuration.
2. Single Source of Truth: strategies MUST NOT maintain their own price arrays.
3. Monotonic windows: a bar whose start time is not strictly newer than the
   window's last bar is discarded (backfill/live overlap, duplicate deliveries).
4. Replay propagation: ParamsReady inherits BarClosed.replay so downstream
   decision makers can refuse to trade on history.
"""

import asyncio
import logging
from collections import deque
from typing import Dict, Optional, Set

from messages.events import BarClosed, ParamsReady
from messages.model import Bar, MarketParams

logger = logging.getLogger("DataProcessor")


class RollingWindow:
    """
    Per-symbol ring buffer of recent bars (O(1) append, fixed memory = maxlen).
      bars / closes / volumes   parallel deques (closes/volumes avoid attribute lookups in features)
      append(bar) -> bool       False if not strictly newer than the last bar (dedupe/out-of-order)
      is_warmed_up              len == maxlen; no ParamsReady is published before this
    """

    def __init__(self, maxlen: int):
        self.bars: deque[Bar] = deque(maxlen=maxlen)
        self.closes: deque[float] = deque(maxlen=maxlen)
        self.volumes: deque[float] = deque(maxlen=maxlen)

    def append(self, bar: Bar) -> bool:
        """Returns False (and ignores the bar) unless it is strictly newer than the last one."""
        if self.bars and bar.start_time <= self.bars[-1].start_time:
            return False
        self.bars.append(bar)
        self.closes.append(bar.close)
        self.volumes.append(bar.volume)
        return True

    @property
    def is_warmed_up(self) -> bool:
        return len(self.bars) == self.bars.maxlen


class DataProcessor:
    """
    Data Processing facade: owns ALL rolling market state, computes features once per bar.

    Subscribes:  BarClosed
    Publishes:   ParamsReady(params, replay inherited from the bar)
    State:
        _windows        Dict[symbol, RollingWindow]   only for active_assets (edge filter)
        _latest_params  Dict[symbol, MarketParams]    latest computed features (read model)
    API:
        warm_up(sym, bars)             startup rehydration (called by WarmupLoader.warm)
        window_status(sym) -> (n, cap)
        get_latest_market_state(sym) -> {"close", "volume", "bar_close_time",
                                          "volatility_estimate", <features>...}   OMS/risk read model
    Strategies MUST NOT keep their own price arrays - this is what lets the lock
    mute a strategy without it missing any market history.
    """
    def __init__(self, bus, active_assets: Set[str], max_lookback: int = 1000):
        self.bus = bus
        self.active_assets = {s.upper() for s in active_assets}
        self.max_lookback = max_lookback
        self._windows: Dict[str, RollingWindow] = {s: RollingWindow(maxlen=max_lookback) for s in self.active_assets}
        self._latest_params: Dict[str, MarketParams] = {}
        bus.subscribe(BarClosed, self.on_bar)
        logger.info(f"DataProcessor initialized for {len(self.active_assets)} assets.")

    async def on_bar(self, event: BarClosed) -> None:
        """Append (dedupe) -> warm-up guard -> compute features -> cache -> publish ParamsReady."""
        bar = event.bar
        window = self._windows.get(bar.symbol)
        if window is None:
            return
        if not window.append(bar):
            logger.debug(f"Duplicate/out-of-order bar for {bar.symbol} at {bar.start_time} discarded.")
            return
        if not window.is_warmed_up:
            return

        # Keeps the loop responsive (ping frames) once features become NumPy-heavy.
        try:
            features, vol_estimate = await asyncio.to_thread(self._compute_features, window)
        except Exception as e:
            logger.error(f"Feature computation failed for {bar.symbol}: {e}", exc_info=True)
            return

        params = MarketParams(symbol=bar.symbol, latest_bar=bar, features=features, volatility_estimate=vol_estimate)
        self._latest_params[bar.symbol] = params
        await self.bus.publish(ParamsReady(params=params, replay=event.replay))

    @staticmethod
    def _compute_features(window: RollingWindow) -> tuple[Dict[str, float], float]:
        """Placeholder features. In the full system these map to pure functions in data/features.py."""
        closes = window.closes
        n = len(closes)
        latest_close = closes[-1]
        last_50 = list(closes)[-50:]
        features = {
            "momentum_10": latest_close - closes[-min(10, n)],
            "sma_50": sum(last_50) / len(last_50),
        }
        vol_estimate = (abs(closes[-1] - closes[-2]) if n > 1 else 0.0) + 1.0
        return features, vol_estimate

    def warm_up(self, symbol: str, historical_bars: list[Bar]) -> None:
        window = self._windows.get(symbol.upper())
        if window is None:
            return
        accepted = sum(window.append(b) for b in historical_bars)
        logger.info(f"Warmed up {symbol} with {accepted} bars ({len(window.bars)}/{window.bars.maxlen}).")

    def window_status(self, symbol: str) -> tuple[int, int]:
        window = self._windows.get(symbol.upper())
        return (len(window.bars), window.bars.maxlen or 0) if window else (0, 0)

    def get_latest_market_state(self, symbol: str) -> dict:
        """Read model for the OMS / risk: latest close, volume, volatility and features."""
        window = self._windows.get(symbol.upper())
        if window is None or not window.bars:
            return {}
        latest_bar = window.bars[-1]
        state = {"close": latest_bar.close, "volume": latest_bar.volume, "bar_close_time": latest_bar.close_time}
        params: Optional[MarketParams] = self._latest_params.get(symbol.upper())
        if params is not None:
            state["volatility_estimate"] = params.volatility_estimate
            state.update(params.features)
        return state
