"""
state/stores.py

The SQLite persistence engine (Repository pattern).

Concurrency model:
  * ONE writer connection, owned by ONE background task, per database file.
    Nothing else in the process writes to the file, so SQLite's single WAL
    write-lock is never contended and no bus handler ever waits on
    "database is locked".
  * Writes are enqueued (fire-and-forget) and committed in batches: the writer
    drains everything currently queued into a single transaction.
  * Writes that must be durable BEFORE the caller proceeds (the OMS
    write-ahead before sending an order to a venue) use `durable=True`: the
    caller awaits a future resolved after that batch commits.
  * Writes are applied strictly in enqueue order, so a later order-state
    upsert can never be overtaken by an earlier one.
  * Reads (startup/rehydration only) use short-lived separate connections;
    WAL allows them to run concurrently with the writer.
  * Schema upgrades are explicit, additive migrations (MIGRATIONS), applied in
    place at start(). A file from a version with no migration path aborts startup.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiosqlite

from messages.model import (
    Bar,
    EquitySnapshot,
    ExecutionMode,
    Fill,
    HaltLevel,
    HaltRecord,
    Intent,
    IntentKind,
    OrderRequest,
    OrderSide,
    OrderSnapshot,
    OrderStatus,
    OrderType,
    Position,
    StrategyPnL,
    TimeInForce,
    TradeRecord,
)

logger = logging.getLogger("SqliteStore")

SCHEMA_VERSION = 4
# from_version -> statements run BEFORE schema.sql (which then creates any new tables).
# Additive only, so history is kept. v3 -> v4: kill switch, per-strategy PnL, intent reference price.
MIGRATIONS: Dict[int, List[str]] = {
    3: ["ALTER TABLE intents ADD COLUMN reference_price REAL"],
}
_TERMINAL = ("FILLED", "CANCELED", "REJECTED", "EXPIRED")
_DEFAULT_SCHEMA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

_WriteItem = Tuple[str, tuple, Optional[asyncio.Future]]


def to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError(f"Naive datetime {dt!r} reached the store - all timestamps must be UTC-aware")
    return int(dt.timestamp() * 1000)


def from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _opt_ms(dt: Optional[datetime]) -> Optional[int]:
    return to_ms(dt) if dt is not None else None


class SchemaMismatchError(RuntimeError):
    """The database file was created by an older schema version; archive it and restart."""


class SqliteStore:
    """
    Repository over one SQLite file (schema: state/schema.sql, version SCHEMA_VERSION).

    State:
        _write_queue   asyncio.Queue[(sql, params, Future|None) | None]   None = stop sentinel
        _writer_task   the ONLY task holding a write connection
    Lifecycle:
        start()        create/verify schema (WAL), launch writer
        stop()         flush everything queued, then stop (always called on shutdown)
        flush()        await until everything enqueued so far is committed
    Writes (enqueue; durable=True awaits the commit):
        save_bar(s)                       klines upsert by (symbol, interval, start_time)
        upsert_order(snapshot, durable)   OMS only; version-guarded (never moves backwards)
        record_fill(fill)                 INSERT OR IGNORE by (client_order_id, trade id)
        save_position(pos, status)        PortfolioManager only; keyed by (sid, sym, opened_at)
        save_trade(record)                PortfolioManager only; trades upsert by trade_id
        link_trade_fill(...)              PortfolioManager only; trade_fills (one row per fill leg)
        log_intent / resolve_intent       Journal inserts, OMS resolves ORDERED / REJECTED:<stage>
        log_risk_event                    risk_ledger rows (rejections, breaches)
        save_halt(record)                 KillSwitchController only; halts upsert by halt_id (durable)
        save_strategy_pnl / save_equity   RiskSupervisor only; time series (insert or replace)
    Reads (separate short-lived connections; startup only):
        get_recent_bars(sym, interval, limit) / get_bar_start_times(...) / get_latest_bar_time(...)
        rehydration: get_latest_trades / get_trade_legs / get_open_position_rows /
                     get_closed_trade_stats / get_closed_trades / get_working_orders (domain objects) /
                     get_fills_for_mode / get_halts / get_equity_history / get_max_total_pnl
    """
    def __init__(self, db_path: str = "system.db", schema_path: str = _DEFAULT_SCHEMA, max_batch: int = 500):
        self.db_path = db_path
        self.schema_path = schema_path
        self.max_batch = max_batch
        self._write_queue: asyncio.Queue[Optional[_WriteItem]] = asyncio.Queue()
        self._writer_task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    # =================================================================
    # Lifecycle
    # =================================================================
    async def start(self) -> None:
        await self._init_db()
        self._writer_task = asyncio.create_task(self._writer_loop(), name="sqlite-writer")
        logger.info(f"SqliteStore started on {self.db_path}")

    async def stop(self) -> None:
        """Flushes everything queued, then stops the writer."""
        if self._writer_task is None:
            return
        self._write_queue.put_nowait(None)   # sentinel: processed after all prior writes
        try:
            await asyncio.wait_for(self._writer_task, timeout=30)
        except asyncio.TimeoutError:
            logger.error("SqliteStore writer did not flush within 30s; cancelling.")
            self._writer_task.cancel()
        self._writer_task = None
        logger.info("SqliteStore stopped (queue flushed).")

    async def _init_db(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode = WAL;")
            await db.execute("PRAGMA synchronous = NORMAL;")
            async with db.execute("PRAGMA user_version;") as cur:
                (version,) = await cur.fetchone()  # type: ignore[misc]
            async with db.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='klines';") as cur:
                (has_tables,) = await cur.fetchone()  # type: ignore[misc]
            if has_tables and version != SCHEMA_VERSION:
                if version not in MIGRATIONS:
                    raise SchemaMismatchError(
                        f"{self.db_path} was created by schema v{version}, this build needs v{SCHEMA_VERSION} "
                        f"and has no migration from it. Archive the file (it holds your history) and restart."
                    )
                while version != SCHEMA_VERSION:
                    for statement in MIGRATIONS[version]:
                        await db.execute(statement)
                    logger.warning(f"Migrated {self.db_path}: schema v{version} -> v{version + 1} (history kept).")
                    version += 1
            with open(self.schema_path, "r", encoding="utf-8") as f:
                await db.executescript(f.read())
            await db.execute(f"PRAGMA user_version = {SCHEMA_VERSION};")
            await db.commit()

    async def _writer_loop(self) -> None:
        """Drains the queue in batches (one transaction per batch) until the sentinel arrives."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA synchronous = NORMAL;")
            await db.execute("PRAGMA busy_timeout = 5000;")
            stop = False
            while not stop:
                first = await self._write_queue.get()
                batch: List[_WriteItem] = []
                if first is None:
                    stop = True
                else:
                    batch.append(first)
                # Drain whatever else is already queued into the same transaction.
                while not stop and len(batch) < self.max_batch:
                    try:
                        item = self._write_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if item is None:
                        stop = True
                    else:
                        batch.append(item)
                if batch:
                    await self._commit_batch(db, batch)
                # task_done for every item pulled (including a sentinel)
                for _ in range(len(batch) + (1 if stop else 0)):
                    self._write_queue.task_done()

    async def _commit_batch(self, db: aiosqlite.Connection, batch: List[_WriteItem]) -> None:
        """Commits a batch; on failure retries row-by-row so one bad row cannot lose its neighbours."""
        try:
            for query, params, _ in batch:
                await db.execute(query, params)
            await db.commit()
            for _, _, fut in batch:
                if fut is not None and not fut.done():
                    fut.set_result(None)
        except Exception as batch_error:
            # Roll back and retry one-by-one so one bad row cannot drop its neighbours.
            await db.rollback()
            logger.error(f"Batch write failed ({batch_error}); retrying {len(batch)} statements individually.")
            for query, params, fut in batch:
                try:
                    await db.execute(query, params)
                    await db.commit()
                    if fut is not None and not fut.done():
                        fut.set_result(None)
                except Exception as e:
                    await db.rollback()
                    logger.error(f"Database write failed: {e} | {query.split()[0:4]}", exc_info=True)
                    if fut is not None and not fut.done():
                        fut.set_exception(e)

    async def _write(self, query: str, params: tuple, durable: bool = False) -> None:
        """Enqueue a statement; with durable=True, return only after its batch has committed."""
        if not durable:
            self._write_queue.put_nowait((query, params, None))
            return
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._write_queue.put_nowait((query, params, fut))
        await fut

    async def flush(self) -> None:
        """Returns once every write enqueued before this call is committed."""
        await self._write("SELECT 1", (), durable=True)

    # =================================================================
    # Writes
    # =================================================================
    async def save_bar(self, bar: Bar) -> None:
        await self._write(
            """INSERT INTO klines (symbol, interval, start_time, close_time, open, high, low, close, volume)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol, interval, start_time) DO UPDATE SET
                 close_time=excluded.close_time, open=excluded.open, high=excluded.high,
                 low=excluded.low, close=excluded.close, volume=excluded.volume""",
            (bar.symbol, bar.interval, to_ms(bar.start_time), to_ms(bar.close_time),
             bar.open, bar.high, bar.low, bar.close, bar.volume),
        )

    async def save_bars(self, bars: Iterable[Bar]) -> None:
        for bar in bars:
            await self.save_bar(bar)

    async def upsert_order(self, o: OrderSnapshot, durable: bool = False) -> None:
        r = o.request
        await self._write(
            """INSERT INTO orders (client_order_id, intent_id, strategy_id, mode, symbol, venue, venue_order_id,
                   side, order_type, time_in_force, status, quantity, price, stop_price,
                   filled_quantity, average_price, cumulative_fee, version, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(client_order_id) DO UPDATE SET
                 venue_order_id=excluded.venue_order_id, status=excluded.status,
                 filled_quantity=excluded.filled_quantity, average_price=excluded.average_price,
                 cumulative_fee=excluded.cumulative_fee, version=excluded.version,
                 updated_at=excluded.updated_at
               WHERE excluded.version >= orders.version""",
            (o.client_order_id, o.intent.intent_id, o.strategy_id, o.mode.value, r.symbol, r.venue,
             o.venue_order_id, r.side.name, r.order_type.name, r.time_in_force.value, o.status.name,
             r.quantity, r.price, r.stop_price, o.filled_quantity, o.average_price, o.cumulative_fee,
             o.version, to_ms(o.created_at), to_ms(o.updated_at)),
            durable=durable,
        )

    async def record_fill(self, f: Fill) -> None:
        await self._write(
            """INSERT OR IGNORE INTO fills (exchange_trade_id, client_order_id, intent_id, strategy_id, symbol,
                   side, price, quantity, fee, fee_asset, is_maker, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (f.exchange_trade_id, f.client_order_id, f.intent_id, f.strategy_id, f.symbol, f.side.name,
             f.price, f.quantity, f.fee, f.fee_asset, int(f.is_maker), to_ms(f.timestamp)),
        )

    async def save_position(self, pos: Position, status: str) -> None:
        await self._write(
            """INSERT INTO positions (strategy_id, symbol, mode, status, quantity, average_entry_price,
                   realized_pnl, fees_paid, protective_order_id, opened_at, updated_at, trade_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(strategy_id, symbol, opened_at) DO UPDATE SET
                 status=excluded.status, quantity=excluded.quantity,
                 average_entry_price=excluded.average_entry_price, realized_pnl=excluded.realized_pnl,
                 fees_paid=excluded.fees_paid, protective_order_id=excluded.protective_order_id,
                 updated_at=excluded.updated_at, trade_id=excluded.trade_id""",
            (pos.strategy_id, pos.symbol, pos.mode.value, status, pos.quantity, pos.average_entry_price,
             pos.realized_pnl, pos.fees_paid, pos.protective_order_id, to_ms(pos.opened_at), to_ms(pos.updated_at),
             pos.trade_id),
        )

    async def save_trade(self, t: TradeRecord) -> None:
        await self._write(
            """INSERT INTO trades (trade_id, strategy_id, symbol, venue, mode, direction, status,
                   entry_qty, exit_qty, max_quantity, avg_entry_price, avg_exit_price,
                   carried_in_qty, carried_in_price, residual_qty, residual_price,
                   gross_pnl, fees_paid, realized_pnl, entry_time, exit_time, holding_ms, num_fills,
                   entry_intent_id, exit_intent_id, exit_reason, entry_ref_price, exit_ref_price, mae, mfe)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(trade_id) DO UPDATE SET
                 direction=excluded.direction, status=excluded.status,
                 entry_qty=excluded.entry_qty, exit_qty=excluded.exit_qty, max_quantity=excluded.max_quantity,
                 avg_entry_price=excluded.avg_entry_price, avg_exit_price=excluded.avg_exit_price,
                 residual_qty=excluded.residual_qty, residual_price=excluded.residual_price,
                 gross_pnl=excluded.gross_pnl, fees_paid=excluded.fees_paid, realized_pnl=excluded.realized_pnl,
                 exit_time=excluded.exit_time, holding_ms=excluded.holding_ms, num_fills=excluded.num_fills,
                 exit_intent_id=excluded.exit_intent_id, exit_reason=excluded.exit_reason,
                 entry_ref_price=excluded.entry_ref_price, exit_ref_price=excluded.exit_ref_price,
                 mae=excluded.mae, mfe=excluded.mfe""",
            (t.trade_id, t.strategy_id, t.symbol, t.venue, t.mode.value, t.direction, t.status,
             t.entry_qty, t.exit_qty, t.max_quantity, t.avg_entry_price, t.avg_exit_price,
             t.carried_in_qty, t.carried_in_price, t.residual_qty, t.residual_price,
             t.gross_pnl, t.fees_paid, t.realized_pnl, to_ms(t.entry_time),
             to_ms(t.exit_time) if t.exit_time else None, t.holding_ms, t.num_fills,
             t.entry_intent_id, t.exit_intent_id, t.exit_reason, t.entry_ref_price, t.exit_ref_price,
             t.mae, t.mfe),
        )

    async def link_trade_fill(self, trade_id: str, fill: Fill) -> None:
        """`fill` is the leg attributed to this trade (a flip-through-zero fill arrives as two legs)."""
        await self._write(
            """INSERT OR IGNORE INTO trade_fills (trade_id, client_order_id, exchange_trade_id, quantity, fee)
               VALUES (?, ?, ?, ?, ?)""",
            (trade_id, fill.client_order_id, fill.exchange_trade_id, fill.quantity, fill.fee),
        )

    async def log_intent(self, intent: Intent) -> None:
        await self._write(
            """INSERT OR IGNORE INTO intents (intent_id, created_at, strategy_id, symbol, venue, kind, side,
                   order_type, quantity, price, stop_price, reason, reference_price)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (intent.intent_id, to_ms(intent.created_at), intent.strategy_id, intent.symbol, intent.venue,
             intent.kind.name, intent.side.name, intent.order_type.name, intent.quantity,
             intent.price, intent.stop_price, intent.reason, intent.reference_price),
        )

    async def resolve_intent(self, intent_id: str, outcome: str, detail: str = "") -> None:
        await self._write(
            "UPDATE intents SET outcome = ?, outcome_detail = ? WHERE intent_id = ? AND outcome IS NULL",
            (outcome, detail, intent_id),
        )

    async def log_risk_event(self, rule_name: str, symbol: Optional[str], action: str,
                             detail: str, timestamp: datetime) -> None:
        await self._write(
            "INSERT INTO risk_ledger (timestamp, rule_name, symbol, action_taken, detail) VALUES (?, ?, ?, ?, ?)",
            (to_ms(timestamp), rule_name, symbol, action, detail),
        )

    async def save_halt(self, h: HaltRecord) -> None:
        await self._write(
            """INSERT INTO halts (halt_id, mode, level, source, reason, triggered_at, escalated_at,
                   flattened_at, cleared_at, clear_detail)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(halt_id) DO UPDATE SET
                 level=excluded.level, escalated_at=excluded.escalated_at, flattened_at=excluded.flattened_at,
                 cleared_at=excluded.cleared_at, clear_detail=excluded.clear_detail""",
            (h.halt_id, h.mode.value, h.level.name, h.source, h.reason, to_ms(h.triggered_at),
             _opt_ms(h.escalated_at), _opt_ms(h.flattened_at), _opt_ms(h.cleared_at), h.clear_detail),
            durable=True,   # a halt must be on disk before anything relies on it
        )

    async def save_strategy_pnl(self, r: StrategyPnL) -> None:
        await self._write(
            """INSERT OR REPLACE INTO strategy_pnl (timestamp, mode, strategy_id, symbol, position_qty,
                   avg_entry_price, mark_price, realized_pnl, unrealized_pnl, total_pnl, fees_paid,
                   closed_trades, winning_trades)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (to_ms(r.timestamp), r.mode.value, r.strategy_id, r.symbol, r.position_qty, r.avg_entry_price,
             r.mark_price, r.realized_pnl, r.unrealized_pnl, r.total_pnl, r.fees_paid,
             r.closed_trades, r.winning_trades),
        )

    async def save_equity(self, e: EquitySnapshot) -> None:
        await self._write(
            """INSERT OR REPLACE INTO portfolio_equity (timestamp, mode, capital, total_pnl, equity,
                   peak_window, high_watermark, drawdown_window, drawdown_hwm, halt_level)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (to_ms(e.timestamp), e.mode.value, e.capital, e.total_pnl, e.equity, e.peak_window,
             e.high_watermark, e.drawdown_window, e.drawdown_hwm, e.halt_level.name),
        )

    # =================================================================
    # Reads (startup / rehydration only)
    # =================================================================
    async def _fetch(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                return [dict(r) for r in await cursor.fetchall()]

    async def get_latest_trades(self) -> List[Dict[str, Any]]:
        """The newest trade row of every ledger (strategy_id, symbol): the open one, or the last closed."""
        return await self._fetch(
            """SELECT * FROM (SELECT t.*, ROW_NUMBER() OVER (PARTITION BY strategy_id, symbol
                                ORDER BY entry_time DESC, rowid DESC) AS rn FROM trades t)
               WHERE rn = 1""")

    async def get_trade_legs(self, trade_id: str) -> List[Dict[str, Any]]:
        """A trade's fill legs in the order they were applied (leg quantity/fee + the fill's columns)."""
        return await self._fetch(
            """SELECT l.quantity AS leg_quantity, l.fee AS leg_fee, f.*
               FROM trade_fills l JOIN fills f USING (client_order_id, exchange_trade_id)
               WHERE l.trade_id = ? ORDER BY l.rowid""", (trade_id,))

    async def get_open_position_rows(self) -> List[Dict[str, Any]]:
        return await self._fetch("SELECT * FROM positions WHERE status = 'OPEN'")

    async def get_closed_trade_stats(self) -> List[Dict[str, Any]]:
        """Per (mode, strategy, symbol): closed-trade count, winners, realized PnL and fees."""
        return await self._fetch(
            """SELECT mode, strategy_id, symbol, COUNT(*) AS closed_trades,
                      SUM(realized_pnl > 0) AS winning_trades,
                      SUM(realized_pnl) AS realized_pnl, SUM(fees_paid) AS fees_paid
               FROM trades WHERE status = 'CLOSED' GROUP BY mode, strategy_id, symbol""")

    async def get_closed_trades(self) -> List[Dict[str, Any]]:
        return await self._fetch(
            "SELECT strategy_id, realized_pnl FROM trades WHERE status = 'CLOSED' ORDER BY exit_time, rowid")

    async def get_working_orders(self) -> List[Tuple[OrderSnapshot, List[Fill], Optional[str]]]:
        """
        Every non-terminal order as its last persisted snapshot (Intent rebuilt) plus its fills so far,
        and a problem string when its intent row is missing. Such an order is still returned - rebuilt
        from the order row, kind from its client order id - so it can never silently drop out of
        rehydration; the problem halts entries instead.
        """
        rows = await self._fetch(
            f"""SELECT o.*, i.created_at AS intent_created_at, i.kind, i.quantity AS intent_quantity,
                       i.price AS intent_price, i.stop_price AS intent_stop_price, i.reason,
                       i.reference_price, i.venue AS intent_venue
                FROM orders o LEFT JOIN intents i USING (intent_id)
                WHERE o.status NOT IN {_TERMINAL} ORDER BY o.created_at""")
        out: List[Tuple[OrderSnapshot, List[Fill], Optional[str]]] = []
        for r in rows:
            side, order_type = OrderSide[r["side"]], OrderType[r["order_type"]]
            tif = TimeInForce(r["time_in_force"])
            problem = None
            if r["intent_created_at"] is not None:
                kind = IntentKind[r["kind"]]
                intent_fields = dict(venue=r["intent_venue"], quantity=r["intent_quantity"], price=r["intent_price"],
                                     stop_price=r["intent_stop_price"], reason=r["reason"] or "",
                                     reference_price=r["reference_price"], created_at=from_ms(r["intent_created_at"]))
            else:
                parts = r["client_order_id"].split(".")
                kind = IntentKind.from_code(parts[1]) if len(parts) == 3 else None
                problem = (f"order {r['client_order_id']}: intent {r['intent_id']} is missing from the journal; "
                           f"rebuilt from the order row as {kind.name if kind else 'OPEN (kind unknown)'}")
                kind = kind or IntentKind.OPEN
                intent_fields = dict(venue=r["venue"], quantity=r["quantity"], price=r["price"],
                                     stop_price=r["stop_price"], reason="rehydrated without an intent row",
                                     reference_price=None, created_at=from_ms(r["created_at"]))
            intent = Intent(strategy_id=r["strategy_id"], symbol=r["symbol"], kind=kind, side=side,
                            order_type=order_type, time_in_force=tif, intent_id=r["intent_id"], **intent_fields)
            request = OrderRequest(
                client_order_id=r["client_order_id"], symbol=r["symbol"], venue=r["venue"], side=side,
                order_type=order_type, quantity=r["quantity"], price=r["price"], stop_price=r["stop_price"],
                time_in_force=tif)
            snap = OrderSnapshot(
                client_order_id=r["client_order_id"], strategy_id=r["strategy_id"], mode=ExecutionMode(r["mode"]),
                intent=intent, request=request, status=OrderStatus[r["status"]],
                filled_quantity=r["filled_quantity"], average_price=r["average_price"],
                cumulative_fee=r["cumulative_fee"], venue_order_id=r["venue_order_id"], version=r["version"],
                created_at=from_ms(r["created_at"]), updated_at=from_ms(r["updated_at"]))
            fills = [self._row_to_fill(f) for f in await self._fetch(
                "SELECT * FROM fills WHERE client_order_id = ? ORDER BY rowid", (r["client_order_id"],))]
            out.append((snap, fills, problem))
        return out

    async def get_fills_for_mode(self, mode: ExecutionMode) -> List[Fill]:
        """Every recorded fill of orders routed to `mode`, in recording order."""
        return [self._row_to_fill(r) for r in await self._fetch(
            """SELECT f.* FROM fills f JOIN orders o USING (client_order_id)
               WHERE o.mode = ? ORDER BY f.rowid""", (mode.value,))]

    async def get_halts(self) -> List[HaltRecord]:
        """Every kill-switch episode, oldest first (cleared_at None = still active)."""
        opt = lambda ms: from_ms(ms) if ms is not None else None   # noqa: E731
        return [HaltRecord(halt_id=r["halt_id"], mode=ExecutionMode(r["mode"]), level=HaltLevel[r["level"]],
                           source=r["source"], reason=r["reason"], triggered_at=from_ms(r["triggered_at"]),
                           escalated_at=opt(r["escalated_at"]), flattened_at=opt(r["flattened_at"]),
                           cleared_at=opt(r["cleared_at"]), clear_detail=r["clear_detail"])
                for r in await self._fetch("SELECT * FROM halts ORDER BY triggered_at, rowid")]

    async def get_max_total_pnl(self, mode: ExecutionMode, since_ms: int) -> Optional[float]:
        """Highest book PnL recorded since `since_ms` (the high-water mark's seed), or None."""
        rows = await self._fetch("SELECT MAX(total_pnl) AS m FROM portfolio_equity WHERE mode = ? AND timestamp >= ?",
                                 (mode.value, since_ms))
        return rows[0]["m"] if rows and rows[0]["m"] is not None else None

    async def get_equity_history(self, mode: ExecutionMode, since_ms: int) -> List[Dict[str, Any]]:
        return await self._fetch(
            "SELECT timestamp, total_pnl FROM portfolio_equity WHERE mode = ? AND timestamp >= ? "
            "ORDER BY timestamp", (mode.value, since_ms))
    async def get_recent_bars(self, symbol: str, interval: str, limit: int) -> List[Bar]:
        query = """SELECT symbol, interval, start_time, close_time, open, high, low, close, volume
                   FROM klines WHERE symbol = ? AND interval = ?
                   ORDER BY start_time DESC LIMIT ?"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query, (symbol, interval, limit)) as cursor:
                rows = list(await cursor.fetchall())
        return [self._row_to_bar(r) for r in reversed(rows)]

    async def get_bar_start_times(self, symbol: str, interval: str, since_ms: int) -> List[int]:
        query = """SELECT start_time FROM klines WHERE symbol = ? AND interval = ? AND start_time >= ?
                   ORDER BY start_time ASC"""
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query, (symbol, interval, since_ms)) as cursor:
                return [int(r[0]) for r in await cursor.fetchall()]

    async def get_latest_bar_time(self, symbol: str, interval: str = "1m") -> Optional[int]:
        """Epoch-ms start time of the most recent stored bar, or None."""
        query = "SELECT MAX(start_time) FROM klines WHERE symbol = ? AND interval = ?"
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(query, (symbol, interval)) as cursor:
                row = await cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    @staticmethod
    def _row_to_fill(r: Dict[str, Any]) -> Fill:
        return Fill(client_order_id=r["client_order_id"], strategy_id=r["strategy_id"], intent_id=r["intent_id"],
                    symbol=r["symbol"], side=OrderSide[r["side"]], quantity=r["quantity"], price=r["price"],
                    fee=r["fee"], fee_asset=r["fee_asset"], is_maker=bool(r["is_maker"]),
                    exchange_trade_id=r["exchange_trade_id"], timestamp=from_ms(r["timestamp"]))

    @staticmethod
    def _row_to_bar(r) -> Bar:
        return Bar(
            symbol=r[0], venue="BINANCE", interval=r[1],
            start_time=from_ms(r[2]), close_time=from_ms(r[3]),
            open=r[4], high=r[5], low=r[6], close=r[7], volume=r[8], is_closed=True,
        )
