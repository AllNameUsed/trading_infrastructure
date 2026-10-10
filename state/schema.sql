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
    outcome_detail TEXT
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
