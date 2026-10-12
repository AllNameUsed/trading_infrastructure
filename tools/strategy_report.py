"""
tools/strategy_report.py

Read-only scorecard per (mode, strategy, symbol): which strategy is under-performing on
which asset. Closed-trade statistics come from `trades`; the latest marked PnL (realized
+ unrealized, residual included) from `strategy_pnl`; each book's equity and drawdowns
from `portfolio_equity`; active halts from `halts`.

Usage:  python -m tools.strategy_report [db_path]
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from config.settings import DB_PATH


def _ts(ms) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if ms else "-"


def report(db_path: str) -> None:
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    print("=" * 108)
    print(f" STRATEGY SCORECARD ({db_path})")
    print("=" * 108)
    print(f"{'mode':<8}{'strategy':<18}{'symbol':<10}{'trades':>7}{'win%':>7}{'realized':>12}{'fees':>10}"
          f"{'avg/trade':>11}{'marked PnL':>12}{'  as of'}")
    marks = {(r["mode"], r["strategy_id"], r["symbol"]): r for r in conn.execute(
        """SELECT * FROM strategy_pnl p WHERE timestamp = (SELECT MAX(timestamp) FROM strategy_pnl q
           WHERE q.mode = p.mode AND q.strategy_id = p.strategy_id AND q.symbol = p.symbol)""")}
    stats = {(r["mode"], r["strategy_id"], r["symbol"]): r for r in conn.execute(
        """SELECT mode, strategy_id, symbol, COUNT(*) n, AVG(realized_pnl > 0) * 100 win,
                  SUM(realized_pnl) realized, SUM(fees_paid) fees, AVG(realized_pnl) avg
           FROM trades WHERE status = 'CLOSED' GROUP BY 1, 2, 3""")}
    for key in sorted(stats.keys() | marks.keys()):
        s, m = stats.get(key), marks.get(key)
        print(f"{key[0]:<8}{key[1]:<18}{key[2]:<10}{(s['n'] if s else 0):>7}{(s['win'] if s else 0):>6.1f}%"
              f"{(s['realized'] if s else 0):>+12.4f}{(s['fees'] if s else 0):>10.4f}{(s['avg'] if s else 0):>+11.4f}"
              f"{(m['total_pnl'] if m else float('nan')):>+12.4f}  {_ts(m['timestamp']) if m else '-'}")

    print("\n BOOKS (latest portfolio_equity row per mode)")
    for r in conn.execute("""SELECT * FROM portfolio_equity e WHERE timestamp =
                             (SELECT MAX(timestamp) FROM portfolio_equity f WHERE f.mode = e.mode)"""):
        print(f"  {r['mode']:<8} equity {r['equity']:,.4f} (capital {r['capital']:,.2f}, PnL {r['total_pnl']:+.4f}) | "
              f"drawdown {r['drawdown_window'] * 100:.3f}% vs window peak, {r['drawdown_hwm'] * 100:.3f}% vs HWM | "
              f"{r['halt_level']} | {_ts(r['timestamp'])}")

    active = conn.execute("SELECT * FROM halts WHERE cleared_at IS NULL").fetchall()
    print("\n ACTIVE HALTS" + ("" if active else ": none"))
    for h in active:
        print(f"  {h['mode']:<8} {h['level']:<14} {h['halt_id']}  since {_ts(h['triggered_at'])}  "
              f"({h['source']}: {h['reason']})")
    conn.close()


if __name__ == "__main__":
    report(sys.argv[1] if len(sys.argv) > 1 else DB_PATH)
