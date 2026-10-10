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
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

import aiosqlite

from messages.model import Bar, Fill, Intent, OrderSnapshot, Position

logger = logging.getLogger("SqliteStore")

SCHEMA_VERSION = 2
_DEFAULT_SCHEMA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

_WriteItem = Tuple[str, tuple, Optional[asyncio.Future]]


def to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError(f"Naive datetime {dt!r} reached the store - all timestamps must be UTC-aware")
    return int(dt.timestamp() * 1000)


def from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


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
        log_intent / resolve_intent       Journal inserts, OMS resolves ORDERED / REJECTED:<stage>
        log_risk_event                    risk_ledger rows (rejections, breaches)
    Reads (separate short-lived connections; startup only):
        get_recent_bars(sym, interval, limit) / get_bar_start_times(...) / get_latest_bar_time(...)
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
                raise SchemaMismatchError(
                    f"{self.db_path} was created by an older schema (user_version={version}). "
                    f"Rename/archive it and restart; schema v{SCHEMA_VERSION} uses epoch-ms timestamps."
                )
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
                   realized_pnl, fees_paid, protective_order_id, opened_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(strategy_id, symbol, opened_at) DO UPDATE SET
                 status=excluded.status, quantity=excluded.quantity,
                 average_entry_price=excluded.average_entry_price, realized_pnl=excluded.realized_pnl,
                 fees_paid=excluded.fees_paid, protective_order_id=excluded.protective_order_id,
                 updated_at=excluded.updated_at""",
            (pos.strategy_id, pos.symbol, pos.mode.value, status, pos.quantity, pos.average_entry_price,
             pos.realized_pnl, pos.fees_paid, pos.protective_order_id, to_ms(pos.opened_at), to_ms(pos.updated_at)),
        )

    async def log_intent(self, intent: Intent) -> None:
        await self._write(
            """INSERT OR IGNORE INTO intents (intent_id, created_at, strategy_id, symbol, venue, kind, side,
                   order_type, quantity, price, stop_price, reason)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (intent.intent_id, to_ms(intent.created_at), intent.strategy_id, intent.symbol, intent.venue,
             intent.kind.name, intent.side.name, intent.order_type.name, intent.quantity,
             intent.price, intent.stop_price, intent.reason),
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

    # =================================================================
    # Reads (startup / rehydration only)
    # =================================================================
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
    def _row_to_bar(r) -> Bar:
        return Bar(
            symbol=r[0], venue="BINANCE", interval=r[1],
            start_time=from_ms(r[2]), close_time=from_ms(r[3]),
            open=r[4], high=r[5], low=r[6], close=r[7], volume=r[8], is_closed=True,
        )
