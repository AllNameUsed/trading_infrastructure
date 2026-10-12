-- state/schema.sql
-- All timestamps are INTEGER epoch milliseconds (UTC). One format, sortable,
-- unambiguous, no dependency on sqlite3's deprecated datetime adapters.

-- 1. Market Data (Rolling Window for Data Processor / Warmups)
CREATE TABLE IF NOT EXISTS klines (
    symbol      TEXT    NOT NULL,
    interval    TEXT    NOT NULL,
    start_time  INTEGER NOT NULL,
    close_time  INTEGER NOT NULL,
    open        REAL    NOT NULL,
    high        REAL    NOT NULL,
    low         REAL    NOT NULL,
    close       REAL    NOT NULL,
    volume      REAL    NOT NULL,
    PRIMARY KEY (symbol, interval, start_time)
) WITHOUT ROWID;

-- 2. Intents (every business decision, accepted or not)
CREATE TABLE IF NOT EXISTS intents (
    intent_id    TEXT PRIMARY KEY,
    created_at   INTEGER NOT NULL,
    strategy_id  TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    venue        TEXT NOT NULL,
    kind         TEXT NOT NULL,
    side         TEXT NOT NULL,
    order_type   TEXT NOT NULL,
    quantity     REAL NOT NULL,
    price        REAL,
    stop_price   REAL,
    reason       TEXT,
    outcome      TEXT,              -- NULL until resolved: ORDERED / REJECTED:<stage>
    outcome_detail TEXT,
    reference_price REAL            -- decision price (v4): lets rehydration rebuild the Intent
);

-- 3. OMS Order Tracking (single writer: the OMS)
CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY,
    intent_id       TEXT NOT NULL,
    strategy_id     TEXT NOT NULL,
    mode            TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    venue           TEXT NOT NULL,
    venue_order_id  TEXT,
    side            TEXT NOT NULL,
    order_type      TEXT NOT NULL,
    time_in_force   TEXT NOT NULL,
    status          TEXT NOT NULL,
    quantity        REAL NOT NULL,
    price           REAL,
    stop_price      REAL,
    filled_quantity REAL NOT NULL DEFAULT 0.0,
    average_price   REAL NOT NULL DEFAULT 0.0,
    cumulative_fee  REAL NOT NULL DEFAULT 0.0,
    version         INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- 4. Fills (immutable execution facts)
CREATE TABLE IF NOT EXISTS fills (
    exchange_trade_id TEXT NOT NULL,
    client_order_id   TEXT NOT NULL,
    intent_id         TEXT NOT NULL,
    strategy_id       TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    side              TEXT NOT NULL,
    price             REAL NOT NULL,
    quantity          REAL NOT NULL,
    fee               REAL NOT NULL,
    fee_asset         TEXT NOT NULL,
    is_maker          INTEGER NOT NULL,
    timestamp         INTEGER NOT NULL,
    PRIMARY KEY (client_order_id, exchange_trade_id)
);

-- 5. Positions (single writer: the PortfolioManager)
CREATE TABLE IF NOT EXISTS positions (
    strategy_id         TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    mode                TEXT NOT NULL,
    status              TEXT NOT NULL, -- 'OPEN' or 'CLOSED'
    quantity            REAL NOT NULL,
    average_entry_price REAL NOT NULL,
    realized_pnl        REAL NOT NULL DEFAULT 0.0,
    fees_paid           REAL NOT NULL DEFAULT 0.0,
    protective_order_id TEXT,
    opened_at           INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    trade_id            TEXT,          -- -> trades.trade_id
    PRIMARY KEY (strategy_id, symbol, opened_at)
);

-- 6. Risk Ledger (pre-trade rejections and genuine breaches)
CREATE TABLE IF NOT EXISTS risk_ledger (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp    INTEGER NOT NULL,
    rule_name    TEXT NOT NULL,
    symbol       TEXT,
    action_taken TEXT NOT NULL,
    detail       TEXT NOT NULL
);

-- 7. Trades: the historical journal, one row per round trip (single writer: the PortfolioManager)
--    A trade ends when its ledger drops below one lot step. Any sub-lot leftover ("dust") is
--    valued at the closing price (residual_*) and carried into the next trade (carried_in_*).
--    realized_pnl = gross_pnl - fees_paid, quote currency.
CREATE TABLE IF NOT EXISTS trades (
    trade_id         TEXT PRIMARY KEY,
    strategy_id      TEXT    NOT NULL,
    symbol           TEXT    NOT NULL,
    venue            TEXT    NOT NULL,
    mode             TEXT    NOT NULL,
    direction        TEXT    NOT NULL,   -- LONG / SHORT
    status           TEXT    NOT NULL,   -- OPEN / CLOSED
    entry_qty        REAL    NOT NULL,   -- gross filled quantity, entry side
    exit_qty         REAL    NOT NULL,   -- gross filled quantity, exit side
    max_quantity     REAL    NOT NULL,   -- largest |holding| during the trade
    avg_entry_price  REAL    NOT NULL,   -- VWAP of entry fills
    avg_exit_price   REAL,               -- VWAP of exit fills
    carried_in_qty   REAL    NOT NULL DEFAULT 0,
    carried_in_price REAL    NOT NULL DEFAULT 0,
    residual_qty     REAL    NOT NULL DEFAULT 0,
    residual_price   REAL,
    gross_pnl        REAL    NOT NULL,
    fees_paid        REAL    NOT NULL,
    realized_pnl     REAL    NOT NULL,
    entry_time       INTEGER NOT NULL,
    exit_time        INTEGER,
    holding_ms       INTEGER,
    num_fills        INTEGER NOT NULL,
    entry_intent_id  TEXT    NOT NULL,
    exit_intent_id   TEXT,
    exit_reason      TEXT,
    entry_ref_price  REAL,               -- decision-time price -> entry slippage
    exit_ref_price   REAL,
    mae              REAL    NOT NULL DEFAULT 0,   -- per unit vs avg_entry_price, <= 0
    mfe              REAL    NOT NULL DEFAULT 0    -- per unit vs avg_entry_price, >= 0
);
CREATE INDEX IF NOT EXISTS idx_trades_strategy_time ON trades(strategy_id, entry_time);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades(symbol, entry_time);

-- 8. Trade <-> fill links (single writer: the PortfolioManager)
--    Every fill belongs to exactly one trade, except a fill that flips a position through
--    zero: it is split into two legs, one per trade. quantity/fee are this leg's share.
CREATE TABLE IF NOT EXISTS trade_fills (
    trade_id          TEXT    NOT NULL,
    client_order_id   TEXT    NOT NULL,
    exchange_trade_id TEXT    NOT NULL,
    quantity          REAL    NOT NULL,
    fee               REAL    NOT NULL,
    PRIMARY KEY (trade_id, client_order_id, exchange_trade_id)
);
CREATE INDEX IF NOT EXISTS idx_trade_fills_fill ON trade_fills(client_order_id, exchange_trade_id);

-- 9. Kill-switch episodes (single writer: the KillSwitchController, risk/kill_switch.py)
--    One row per episode and execution mode; escalated in place; cleared_at stays NULL
--    until the operator quotes halt_id in config/kill_switch.ini.
CREATE TABLE IF NOT EXISTS halts (
    halt_id      TEXT PRIMARY KEY,
    mode         TEXT    NOT NULL,
    level        TEXT    NOT NULL,   -- HALT_ENTRIES / HALT_COMPLETE (highest reached)
    source       TEXT    NOT NULL,   -- manual / drawdown_window / drawdown_hwm / rehydration / breach:<rule>
    reason       TEXT    NOT NULL,
    triggered_at INTEGER NOT NULL,
    escalated_at INTEGER,
    flattened_at INTEGER,            -- HALT_COMPLETE: no system exposure or working order left
    cleared_at   INTEGER,            -- NULL while active
    clear_detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_halts_mode ON halts(mode, cleared_at);

-- 10. PnL per (mode, strategy, symbol), sampled on every live bar close of the symbol
--     (single writer: the RiskSupervisor, risk/supervisor.py). Cumulative since inception.
CREATE TABLE IF NOT EXISTS strategy_pnl (
    timestamp       INTEGER NOT NULL,
    mode            TEXT    NOT NULL,
    strategy_id     TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    position_qty    REAL    NOT NULL,   -- ledger, sub-lot residual included
    avg_entry_price REAL    NOT NULL,
    mark_price      REAL,
    realized_pnl    REAL    NOT NULL,   -- net of fees
    unrealized_pnl  REAL    NOT NULL,
    total_pnl       REAL    NOT NULL,
    fees_paid       REAL    NOT NULL,
    closed_trades   INTEGER NOT NULL,
    winning_trades  INTEGER NOT NULL,
    PRIMARY KEY (mode, strategy_id, symbol, timestamp)
) WITHOUT ROWID;

-- 11. Book equity per execution mode, sampled on every live bar close (single writer: the
--     RiskSupervisor). Drawdowns are fractions: 0.05 = 5 %.
CREATE TABLE IF NOT EXISTS portfolio_equity (
    timestamp       INTEGER NOT NULL,
    mode            TEXT    NOT NULL,
    capital         REAL    NOT NULL,
    total_pnl       REAL    NOT NULL,
    equity          REAL    NOT NULL,
    peak_window     REAL    NOT NULL,
    high_watermark  REAL    NOT NULL,
    drawdown_window REAL    NOT NULL,
    drawdown_hwm    REAL    NOT NULL,
    halt_level      TEXT    NOT NULL,
    PRIMARY KEY (mode, timestamp)
) WITHOUT ROWID;
