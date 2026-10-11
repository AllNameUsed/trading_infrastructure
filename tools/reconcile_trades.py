"""
tools/reconcile_trades.py

Independent check of the trade journal (`trades`, `trade_fills`) against the
immutable fills ledger. Nothing here reuses the PortfolioManager's code: every
trade is rebuilt from its fills with plain cash-flow arithmetic.

Checks
  1. Attribution: every fill is linked, and its legs add up to the fill's quantity and fee.
  2. Per trade: entry/exit quantity and VWAP, number of fill legs.
  3. Per trade: realized_pnl == gross_pnl - fees_paid, and (closed trades)
        realized = net cash flow - quote-asset fees
                   + residual_qty * residual_price - carried_in_qty * carried_in_price
  4. Per trade: inventory - carried_in + sum of base-asset deltas (net of base-asset fees)
                equals residual_qty (closed) or the open position's quantity (open).
  5. Per ledger (strategy, symbol): each trade's carried_in equals the previous trade's
     residual. A break to zero after a residual means the process restarted: residuals
     are not rehydrated yet, so this is reported as a warning, not a failure.
  6. CLOSED positions rows agree with their trade's realized PnL.
Fees in a third asset (e.g. BNB) cannot be valued in quote and are reported as warnings.

Usage:  python -m tools.reconcile_trades [db_path]      (exit code 1 on any failure)
"""

from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

from config.settings import DB_PATH


def _close(a: float, b: float, tol: float = 1e-8) -> bool:
    return abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def _fee_kind(symbol: str, fee_asset: str) -> str:
    """'base' / 'quote' / 'other' - inferred from the symbol (e.g. ETHUSDT: ETH base, USDT quote)."""
    if symbol.startswith(fee_asset):
        return "base"
    if symbol.endswith(fee_asset):
        return "quote"
    return "other"


def reconcile(db_path: str) -> Tuple[List[str], List[str], dict]:
    """Returns (failures, warnings, summary); summary["residuals"] maps 'strategy/symbol' -> quantity held."""
    conn = sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True)   # read-only
    conn.row_factory = sqlite3.Row
    failures: List[str] = []
    warnings: List[str] = []

    # 1. Attribution ------------------------------------------------------------------
    for r in conn.execute(
        """SELECT f.client_order_id, f.exchange_trade_id, f.quantity, f.fee,
                  COUNT(l.trade_id) AS n, IFNULL(SUM(l.quantity), 0) AS lq, IFNULL(SUM(l.fee), 0) AS lf
           FROM fills f LEFT JOIN trade_fills l USING (client_order_id, exchange_trade_id)
           GROUP BY f.client_order_id, f.exchange_trade_id"""):
        fid = f"{r['client_order_id']}/{r['exchange_trade_id']}"
        if r["n"] == 0:
            failures.append(f"fill {fid} is not linked to any trade")
        elif not _close(r["quantity"], r["lq"]) or not _close(r["fee"], r["lf"]):
            failures.append(f"fill {fid}: legs sum to qty {r['lq']} fee {r['lf']}, "
                            f"fill has qty {r['quantity']} fee {r['fee']}")

    legs: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    for r in conn.execute(
        """SELECT l.trade_id, l.quantity, l.fee, f.side, f.price, f.fee_asset, f.symbol
           FROM trade_fills l JOIN fills f USING (client_order_id, exchange_trade_id)
           ORDER BY f.timestamp"""):
        legs[r["trade_id"]].append(r)
    open_qty = {r["trade_id"]: r["quantity"] for r in conn.execute(
        "SELECT trade_id, quantity FROM positions WHERE status = 'OPEN' AND trade_id IS NOT NULL")}
    closed_pos = {r["trade_id"]: r["realized_pnl"] for r in conn.execute(
        "SELECT trade_id, realized_pnl FROM positions WHERE status = 'CLOSED' AND trade_id IS NOT NULL")}

    trades = conn.execute("SELECT * FROM trades ORDER BY strategy_id, symbol, entry_time").fetchall()
    prev_by_ledger: Dict[Tuple[str, str], sqlite3.Row] = {}
    summary: dict = {"trades": len(trades), "closed": 0, "realized_pnl": 0.0, "fees_paid": 0.0, "residuals": {}}

    for t in trades:
        tid, sym = t["trade_id"], t["symbol"]
        direction = 1 if t["direction"] == "LONG" else -1
        tl = legs.get(tid, [])

        # 2. Quantities and prices --------------------------------------------------------
        sign = lambda leg: 1 if leg["side"] == "BUY" else -1   # noqa: E731
        entry = [x for x in tl if sign(x) == direction]
        exits = [x for x in tl if sign(x) != direction]
        e_qty, x_qty = sum(x["quantity"] for x in entry), sum(x["quantity"] for x in exits)
        if not _close(e_qty, t["entry_qty"]) or not _close(x_qty, t["exit_qty"]):
            failures.append(f"trade {tid}: entry/exit qty {t['entry_qty']}/{t['exit_qty']}, fills say {e_qty}/{x_qty}")
        if e_qty and not _close(sum(x["quantity"] * x["price"] for x in entry) / e_qty, t["avg_entry_price"]):
            failures.append(f"trade {tid}: avg_entry_price {t['avg_entry_price']} does not match its fills")
        if x_qty and not _close(sum(x["quantity"] * x["price"] for x in exits) / x_qty, t["avg_exit_price"]):
            failures.append(f"trade {tid}: avg_exit_price {t['avg_exit_price']} does not match its fills")
        if len(tl) != t["num_fills"]:
            failures.append(f"trade {tid}: num_fills {t['num_fills']}, linked legs {len(tl)}")

        # 3./4. Cash and inventory ------------------------------------------------------
        cash = quote_fees = inventory = 0.0
        for x in tl:
            cash -= sign(x) * x["quantity"] * x["price"]
            inventory += sign(x) * x["quantity"]
            kind = _fee_kind(sym, x["fee_asset"])
            if kind == "base":
                inventory -= x["fee"]
            elif kind == "quote":
                quote_fees += x["fee"]
            elif x["fee"]:
                warnings.append(f"trade {tid}: fee {x['fee']} {x['fee_asset']} cannot be valued; not reconciled")
        held = t["carried_in_qty"] + inventory

        if not _close(t["realized_pnl"], t["gross_pnl"] - t["fees_paid"]):
            failures.append(f"trade {tid}: realized {t['realized_pnl']} != gross {t['gross_pnl']} - fees {t['fees_paid']}")
        if t["status"] == "CLOSED":
            summary["closed"] += 1
            summary["realized_pnl"] += t["realized_pnl"]
            summary["fees_paid"] += t["fees_paid"]
            expected = (cash - quote_fees + t["residual_qty"] * (t["residual_price"] or 0.0)
                        - t["carried_in_qty"] * t["carried_in_price"])
            if not _close(expected, t["realized_pnl"]):
                failures.append(f"trade {tid}: realized {t['realized_pnl']:.10f}, fills give {expected:.10f}")
            if not _close(held, t["residual_qty"], tol=1e-10):
                failures.append(f"trade {tid}: fills leave {held:.12g} held, residual_qty is {t['residual_qty']:.12g}")
            if tid in closed_pos and not _close(closed_pos[tid], t["realized_pnl"]):
                failures.append(f"trade {tid}: positions row realized {closed_pos[tid]} != trade {t['realized_pnl']}")
        elif tid in open_qty and not _close(held, open_qty[tid], tol=1e-10):
            failures.append(f"trade {tid} (OPEN): fills leave {held:.12g} held, position says {open_qty[tid]:.12g}")

        # 5. Residual chain ---------------------------------------------------------------
        key = (t["strategy_id"], sym)
        prev = prev_by_ledger.get(key)
        expected_in = prev["residual_qty"] if prev is not None else 0.0
        if not _close(t["carried_in_qty"], expected_in, tol=1e-10):
            if t["carried_in_qty"] == 0 and prev is not None:
                warnings.append(f"{key}: residual {expected_in:.12g} of trade {prev['trade_id']} was not carried "
                                f"into {tid} (process restarted; residuals are not rehydrated yet)")
            else:
                failures.append(f"trade {tid}: carried_in {t['carried_in_qty']:.12g}, previous residual {expected_in:.12g}")
        elif prev is not None and expected_in and not _close(t["carried_in_price"], prev["residual_price"]):
            failures.append(f"trade {tid}: carried_in_price {t['carried_in_price']} != previous residual_price "
                            f"{prev['residual_price']}")
        prev_by_ledger[key] = t

    summary["residuals"] = {f"{sid}/{sym}": t["residual_qty"] for (sid, sym), t in prev_by_ledger.items()
                            if t["status"] == "CLOSED" and t["residual_qty"]}
    conn.close()
    return failures, warnings, summary


def main(db_path: str) -> int:
    failures, warnings, s = reconcile(db_path)
    print("=" * 60)
    print(f" TRADE JOURNAL RECONCILIATION ({db_path})")
    print("=" * 60)
    print(f"  trades {s['trades']} (closed {s['closed']}) | realized {s['realized_pnl']:+.6f} "
          f"| fees {s['fees_paid']:.6f}")
    for ledger, qty in s["residuals"].items():
        print(f"  residual carried on {ledger}: {qty:.12g}")
    for w in warnings:
        print(f"  WARN  {w}")
    for f in failures:
        print(f"  FAIL  {f}")
    print("  PASSED" if not failures else f"  FAILED: {len(failures)} problem(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else DB_PATH))
