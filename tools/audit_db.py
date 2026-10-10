"""
tools/audit_db.py  (formerly test.py)

Inspects table row counts and audits kline continuity to detect dropped
candles or ingestion gaps within the last few hours.

Usage:  python -m tools.audit_db [hours]
"""

import sqlite3
import sys
from datetime import datetime, timezone

from config.settings import BAR_INTERVAL, DB_PATH

INTERVAL_MS = int(BAR_INTERVAL[:-1]) * {"s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}[BAR_INTERVAL[-1]]


def _fmt(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def audit_database(hours: float = 4.0) -> None:
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    cur = conn.cursor()

    print("=" * 60)
    print(f" ROW COUNTS ({DB_PATH})")
    print("=" * 60)
    for table in ("klines", "intents", "orders", "fills", "positions", "risk_ledger"):
        (n,) = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        print(f"  {table:<12} {n}")

    counts = cur.execute("SELECT symbol, COUNT(*) FROM klines WHERE interval = ? GROUP BY symbol",
                         (BAR_INTERVAL,)).fetchall()
    if not counts:
        print("\nTable 'klines' is empty.")
        conn.close()
        return

    cutoff_ms = int(datetime.now(timezone.utc).timestamp() * 1000 - hours * 3_600_000)
    print("\n" + "=" * 60)
    print(f" CONTINUITY AUDIT ({BAR_INTERVAL}, last {hours}h)")
    print("=" * 60)
    for symbol, _ in counts:
        starts = [r[0] for r in cur.execute(
            "SELECT start_time FROM klines WHERE symbol = ? AND interval = ? AND start_time >= ? ORDER BY start_time",
            (symbol, BAR_INTERVAL, cutoff_ms))]
        gaps = [(a, b) for a, b in zip(starts, starts[1:]) if b - a != INTERVAL_MS]
        if not starts:
            print(f"[{symbol}] No data in window.")
        elif not gaps:
            print(f"[{symbol}] PASSED: {len(starts)} contiguous bars.")
        else:
            print(f"[{symbol}] FAILED: {len(gaps)} gap(s)")
            for a, b in gaps:
                print(f"   {_fmt(a)} -> {_fmt(b)}  (missing {(b - a) // INTERVAL_MS - 1} bar(s))")
    conn.close()


if __name__ == "__main__":
    audit_database(float(sys.argv[1]) if len(sys.argv) > 1 else 4.0)
