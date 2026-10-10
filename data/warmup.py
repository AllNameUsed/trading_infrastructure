"""
data/warmup.py

Startup rehydration - the ONE implementation of gap healing (previously
duplicated between main.py and the feed).

For each instrument:
  1. Heal internal gaps in the recent recovery horizon and the trailing gap up
     to "now" via the HistoricalBarSource (closed bars only), persisting them.
  2. Flush the store so the feed's later "latest stored bar" lookup is exact.
  3. Load the last `max_lookback` bars from the store into the DataProcessor.
"""

from __future__ import annotations

import logging
import time
from typing import Iterable

from connectors.base import HistoricalBarSource
from data.processor import DataProcessor

logger = logging.getLogger("WarmupLoader")


def _interval_ms(interval: str) -> int:
    units = {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    return int(interval[:-1]) * units[interval[-1]]


class WarmupLoader:
    """
    Startup rehydration, run by main.py before the feed starts.
      heal_gaps(symbols)        fill internal gaps + the trailing gap within recovery_hours
                                from the HistoricalBarSource into the store, then flush()
      warm(processor, symbols)  load the newest max_lookback bars into the DataProcessor
    """
    def __init__(self, store, source: HistoricalBarSource, interval: str, recovery_hours: float):
        self.store = store
        self.source = source
        self.interval = interval
        self.recovery_hours = recovery_hours

    async def heal_gaps(self, symbols: Iterable[str]) -> None:
        step = _interval_ms(self.interval)
        now_ms = int(time.time() * 1000)
        horizon_ms = now_ms - int(self.recovery_hours * 3_600_000)
        for symbol in symbols:
            starts = await self.store.get_bar_start_times(symbol, self.interval, horizon_ms)
            ranges: list[tuple[int, int]] = []
            # Internal gaps
            for prev, curr in zip(starts, starts[1:]):
                if curr - prev > step:
                    ranges.append((prev + step, curr - step))
            # Trailing gap (or the whole horizon if we hold nothing recent)
            tail_start = (starts[-1] + step) if starts else horizon_ms
            ranges.append((tail_start, now_ms))

            healed = 0
            for start_ms, end_ms in ranges:
                if end_ms < start_ms:
                    continue
                try:
                    bars = await self.source.fetch_bars(symbol, self.interval, start_ms, end_ms)
                except Exception as e:
                    logger.error(f"Failed to heal {symbol} [{start_ms}..{end_ms}]: {e}")
                    continue
                await self.store.save_bars(bars)
                healed += len(bars)
            if healed:
                logger.info(f"Healed {healed} bar(s) for {symbol} within the last {self.recovery_hours}h.")
        await self.store.flush()

    async def warm(self, processor: DataProcessor, symbols: Iterable[str]) -> None:
        for symbol in symbols:
            bars = await self.store.get_recent_bars(symbol, self.interval, limit=processor.max_lookback)
            if bars:
                processor.warm_up(symbol, bars)
            else:
                logger.warning(f"No local history for {symbol}. Starting cold.")
