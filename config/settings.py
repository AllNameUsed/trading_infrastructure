"""
config/settings.py

Master configuration. Everything tunable lives here; secrets come from the
project-root .env file (never committed).

Execution modes (see messages.model.ExecutionMode):
  SIM     -> internal matching engine on live mainnet quotes  (strategy evaluation)
  TESTNET -> Binance Spot testnet                            (API integration checks)
  LIVE    -> Binance mainnet, real money
"""

import os
from pathlib import Path

from dotenv import load_dotenv

from messages.model import ExecutionMode

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env FIRST so every os.getenv below sees it.
load_dotenv(PROJECT_ROOT / ".env")

# =====================================================================
# Storage & logging
# =====================================================================
DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "system.db"))
LOG_PATH = os.getenv("LOG_PATH", str(PROJECT_ROOT / "sys.log"))

# =====================================================================
# Market data
# =====================================================================
ACTIVE_ASSETS = {"BTCUSDT", "ETHUSDT"}
BAR_INTERVAL = "1m"
MAX_LOOKBACK = 100              # bars kept per symbol in the DataProcessor
MAX_BACKFILL_HOURS = 4          # gap-healing horizon at startup
ENABLE_SIM_QUOTE_STREAM = True  # bookTicker + aggTrade for the simulator (else bar-only mode)

# =====================================================================
# API credentials (names of env vars, values only in .env)
# =====================================================================
BINANCE_TESTNET_API_KEY = os.getenv("BINANCE_TESTNET_API_KEY", "")
BINANCE_TESTNET_SECRET = os.getenv("BINANCE_TESTNET_SECRET", "")
BINANCE_LIVE_API_KEY = os.getenv("BINANCE_LIVE_API_KEY", "")
BINANCE_LIVE_SECRET = os.getenv("BINANCE_LIVE_SECRET", "")

# =====================================================================
# Strategies: the ONE place a strategy id is defined
# =====================================================================
MOCK_STRAT_ID = "MOCK_STRAT_01"

# Execution routing. If a strategy is not listed here, the OMS rejects its intents
# with a terminal IntentRejected(stage=REGISTRY).
STRATEGY_REGISTRY = {
    MOCK_STRAT_ID: ExecutionMode.SIM,
    # "MEAN_REVERSION_ETH": ExecutionMode.LIVE,  <-- change with caution
}

# =====================================================================
# Simulated venue
# =====================================================================
SIM_INITIAL_BALANCES = {"USDT": 10_000.0}
SIM_TAKER_FEE = 0.001
SIM_MAKER_FEE = 0.001
SIM_LATENCY_MEDIAN_MS = 40.0

# =====================================================================
# Risk
# =====================================================================
RISK_MAX_ORDER_NOTIONAL = 5_000.0
RISK_MOCK_REJECTION_PROBABILITY = 0.20


def validate() -> None:
    """Fail loudly at startup instead of silently at 2pm."""
    modes = set(STRATEGY_REGISTRY.values())
    if ExecutionMode.LIVE in modes and not (BINANCE_LIVE_API_KEY and BINANCE_LIVE_SECRET):
        raise ValueError("Strategies are routed LIVE but BINANCE_LIVE_API_KEY / BINANCE_LIVE_SECRET are missing in .env")
    if ExecutionMode.TESTNET in modes and not (BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_SECRET):
        raise ValueError("Strategies are routed TESTNET but BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_SECRET are missing in .env")
