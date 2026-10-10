"""
main.py - The Composition Root.

The only file that knows the concrete world (which venues, which strategies,
which keys). It constructs every component, wires them, runs the startup
sequence, and guarantees an orderly shutdown. It contains no business logic.

Startup:  store -> instruments (exchangeInfo) -> gap healing -> warm-up ->
          venues -> OMS -> bus -> quote stream -> feed
Shutdown: feeds off -> in-flight venue calls drained -> bus drained ->
          venues closed -> store flushed (ALWAYS, including Ctrl+C on Windows)
"""

import asyncio
import logging
import os
import signal
import sys

from bus.event_bus import EventBus
from config import settings
from connectors.binance.feed import BinanceFeed
from connectors.binance.quotes import BinanceQuoteStream
from connectors.binance.rest import BinanceMarketDataREST
from connectors.binance.venue import BinanceVenue
from connectors.sim.venue import SimConfig, SimulatedVenue
from data.processor import DataProcessor
from data.warmup import WarmupLoader
from execution.oms import OrderManagementSystem
from execution.router import VenueRouter
from messages.events import BarClosed, ParamsReady
from messages.instruments import InstrumentRegistry, default_spec
from messages.model import ExecutionMode
from ops.journal import DatabaseJournal
from opm.lock import LockManager
from opm.manager import OpenPositionManager
from risk.pretrade import (
    DuplicateIntentRule,
    FatFingerRule,
    MinNotionalRule,
    PreTradeRiskManager,
    RandomMockRejectionRule,
)
from state.portfolio import PortfolioManager
from state.stores import SqliteStore
from strategies.mock_strat import MockStrat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(settings.LOG_PATH, mode="a"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("CompositionRoot")


async def load_instruments(rest: BinanceMarketDataREST, symbols) -> InstrumentRegistry:
    """exchangeInfo -> InstrumentRegistry; conservative default_spec() for anything missing."""
    registry = InstrumentRegistry()
    try:
        for spec in await rest.fetch_instrument_specs(symbols):
            registry.add(spec)
            logger.info(f"Instrument {spec.symbol}: tick={spec.tick_size} step={spec.step_size} "
                        f"minNotional={spec.min_notional}")
    except Exception as e:
        logger.error(f"exchangeInfo unavailable ({e}); using conservative default filters.")
    for sym in symbols:
        if sym not in registry:
            registry.add(default_spec(sym))
    return registry


async def main() -> None:
    """Build -> pre-flight -> run until a stop signal or a core task dies -> shutdown()."""
    settings.validate()
    symbols = sorted(settings.ACTIVE_ASSETS)
    logger.info(f"Starting trading infrastructure for {symbols} ({settings.BAR_INTERVAL} bars).")

    # ----------------------------------------------------------------
    # 1. Infrastructure
    # ----------------------------------------------------------------
    bus = EventBus()
    store = SqliteStore(db_path=settings.DB_PATH)
    rest = BinanceMarketDataREST()
    await store.start()
    instruments = await load_instruments(rest, symbols)

    # ----------------------------------------------------------------
    # 2. Venues (ports) and routing
    # ----------------------------------------------------------------
    sim_venue = SimulatedVenue(instruments, SimConfig(
        initial_balances=dict(settings.SIM_INITIAL_BALANCES),
        taker_fee_rate=settings.SIM_TAKER_FEE, maker_fee_rate=settings.SIM_MAKER_FEE,
        latency_median_ms=settings.SIM_LATENCY_MEDIAN_MS,
    ))
    router = VenueRouter([sim_venue])
    modes = set(settings.STRATEGY_REGISTRY.values())
    if ExecutionMode.TESTNET in modes:
        router.register(BinanceVenue(settings.BINANCE_TESTNET_API_KEY, settings.BINANCE_TESTNET_SECRET,
                                     ExecutionMode.TESTNET, instruments))
    if ExecutionMode.LIVE in modes:
        router.register(BinanceVenue(settings.BINANCE_LIVE_API_KEY, settings.BINANCE_LIVE_SECRET,
                                     ExecutionMode.LIVE, instruments))

    # ----------------------------------------------------------------
    # 3. Core components (handler order is declared via `order=`, not by these lines)
    # ----------------------------------------------------------------
    DatabaseJournal(bus, store)
    portfolio = PortfolioManager(bus, instruments, store)
    lock_manager = LockManager(bus, position_view=portfolio.get_position)
    processor = DataProcessor(bus, active_assets=set(symbols), max_lookback=settings.MAX_LOOKBACK)
    OpenPositionManager(bus, lock_manager)
    risk_manager = PreTradeRiskManager(rules=[
        FatFingerRule(max_notional_value=settings.RISK_MAX_ORDER_NOTIONAL),
        MinNotionalRule(min_notional=5.0),
        DuplicateIntentRule(window_seconds=2.0),
        RandomMockRejectionRule(probability=settings.RISK_MOCK_REJECTION_PROBABILITY),
    ])
    oms = OrderManagementSystem(
        bus=bus, store=store, router=router, strategy_registry=settings.STRATEGY_REGISTRY,
        risk_manager=risk_manager, instruments=instruments,
        market_view=processor.get_latest_market_state, position_view=portfolio.get_position,
    )
    strategies = [
        MockStrat(bus=bus, lock_manager=lock_manager, strategy_id=settings.MOCK_STRAT_ID, symbols=["ETHUSDT"]),
    ]
    unregistered = [s.strategy_id for s in strategies if s.strategy_id not in settings.STRATEGY_REGISTRY]
    if unregistered:
        raise ValueError(f"Strategies {unregistered} are not in STRATEGY_REGISTRY - "
                         f"every intent they issue would be rejected.")

    # Market data plumbing
    bus.subscribe(BarClosed, lambda e: store.save_bar(e.bar), order=-20)
    bus.subscribe(BarClosed, lambda e: sim_venue.on_bar(e.bar), order=-15)   # degraded-mode fallback only

    def print_telemetry(event: ParamsReady) -> None:
        p = event.params
        n, cap = processor.window_status(p.symbol)
        logger.info(f"[FEATURES{' REPLAY' if event.replay else ''}] {p.symbol} | Close: {p.latest_bar.close:.2f} | "
                    f"Vol: {p.volatility_estimate:.2f} | Mom_10: {p.features.get('momentum_10', 0.0):.2f} | "
                    f"SMA_50: {p.features.get('sma_50', 0.0):.2f} | Buffer {n}/{cap}")
    bus.subscribe(ParamsReady, print_telemetry, order=100)

    # ----------------------------------------------------------------
    # 4. Pre-flight: heal gaps, warm the windows
    # ----------------------------------------------------------------
    warmup = WarmupLoader(store, rest, settings.BAR_INTERVAL, settings.MAX_BACKFILL_HOURS)
    await warmup.heal_gaps(symbols)
    await warmup.warm(processor, symbols)

    # ----------------------------------------------------------------
    # 5. Run
    # ----------------------------------------------------------------
    for venue in router.all():
        await venue.start()
    await oms.start()

    feed = BinanceFeed(bus, symbols, timeframe=settings.BAR_INTERVAL, rest=rest,
                       get_latest_time_cb=store.get_latest_bar_time)
    quotes = (BinanceQuoteStream(symbols, sim_venue)
              if settings.ENABLE_SIM_QUOTE_STREAM and ExecutionMode.SIM in modes else None)

    tasks = [asyncio.create_task(bus.run(), name="bus"), asyncio.create_task(feed.start(), name="feed")]
    if quotes is not None:
        tasks.append(asyncio.create_task(quotes.start(), name="quotes"))

    stop_event = asyncio.Event()
    if os.name != "nt":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

    logger.info("System running. Waiting for candle closes...")
    try:
        # Returns on a stop signal, or as soon as any core task dies unexpectedly.
        stopper = asyncio.create_task(stop_event.wait(), name="stop-signal")
        done, _ = await asyncio.wait([stopper, *tasks], return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            if t is not stopper and not t.cancelled() and t.exception():
                logger.critical(f"Core task {t.get_name()} died: {t.exception()!r}")
    except asyncio.CancelledError:
        # Windows Ctrl+C: asyncio.run cancels this task - fall through to cleanup.
        logger.warning("Interrupt received. Shutting down gracefully...")
    finally:
        await shutdown(feed, quotes, oms, bus, router, rest, store, tasks)


async def shutdown(feed, quotes, oms, bus, router, rest, store, tasks) -> None:
    """Orderly stop. Resting venue orders are deliberately left alone; the store is ALWAYS flushed."""
    logger.warning("Shutting down: feeds off, resting orders left untouched at the venue, state flushed.")
    feed.stop()
    if quotes is not None:
        quotes.stop()
    try:
        await oms.stop(timeout=10.0)
        try:
            await asyncio.wait_for(bus.drain(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.error("Bus did not drain within 10s.")
        bus.stop()
        await asyncio.wait([tasks[0]], timeout=5.0)   # the bus exits via its stop sentinel
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for venue in router.all():
            await venue.stop()
        await rest.stop()
    finally:
        logger.info("Flushing database queue to disk...")
        await store.stop()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
