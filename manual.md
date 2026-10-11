# Operator's Manual — Change Playbooks & Checklists

`readme.md` explains **how the system is built**. This manual explains **how to change it without breaking it**.
Every playbook is a checklist: work top to bottom and tick each box. If a step doesn't apply, write down why
instead of skipping it silently.

> **Code is referenced by name, never by line number.** For example, "the `strategies = [...]` list in
> `main.py`, section 3", not "main.py line 125". Line numbers go stale with the first edit; names don't.

---

## 0. Before any change: the invariants you must not break

These are the system's load-bearing walls (full text: `readme.md` §2). Every playbook below protects one or more of them.

| # | Invariant | Typical way to break it |
|---|---|---|
| I1 | **Single writer per state.** The OMS owns orders, the PortfolioManager owns positions, the DataProcessor owns market state and the LockManager owns locks. | A strategy that keeps its own price array, or a connector that publishes on the bus |
| I2 | **Every intent ends in exactly one terminal outcome:** an order or an `IntentRejected`. | An early `return` in an OMS path that publishes nothing, which leaves the lock stuck in PENDING forever |
| I3 | **No network I/O on the bus loop.** | `await session.get(...)` inside a bus handler, a strategy or a risk rule |
| I4 | **Unknown is not rejected.** A timed-out order stays UNKNOWN until reconciled. | A venue adapter that returns REJECTED on a timeout. That is how double entries happen |
| I5 | **Never trade on history.** `replay=True` bars update state but never trigger trades. | A new decision maker that forgets the `event.replay` check |
| I6 | **Locks are acquired synchronously at the moment of decision** (`try_begin_entry` / `try_begin_exit`, with no `await` between the check and the publish). | Publishing `IntentIssued` without acquiring the lock. The lock logs an ERROR audit line |
| I7 | **Imports point inward.** Only `main.py` constructs concrete adapters. The word "Binance" never appears in `strategies/`, `opm/`, `risk/` or `execution/`. | `from connectors.binance... import` inside core code |
| I8 | **Everything on the bus is frozen.** | Putting a mutable object (a list, a live `Order`) into an event |

---

## 1. The universal change checklist (applies to EVERY change)

**Before**
- [ ] Working tree is clean (`git status`), and you are on a branch, not `master`.
- [ ] You know which invariants (§0) the change touches.
- [ ] If the change touches the DB schema, read **Playbook I** first.

**During**
- [ ] Construction happens only in `main.py` (or `tests/helpers.py`). No component builds its own dependencies.
- [ ] New config values go in `config/settings.py`. Secrets go in `.env`, never in code.
- [ ] New bus subscribers have their `order=` chosen deliberately (see the table in `readme.md` §4).

**After: the definition of done**
- [ ] Unit tests pass: `python -m unittest discover -s tests -t .` (no network, about 2 s).
- [ ] A new test covers the change (the `System` fixture in `tests/helpers.py` wires the whole core on the simulator).
- [ ] **SIM soak run** of at least a few hours, then the post-run audit (§3) is clean.
- [ ] `readme.md` and this manual are updated if a structure, event, table or setting changed.
- [ ] Commit.

---

## 2. Playbooks

### A. Add a new strategy

1. **Implement it.**
   - [ ] Create `strategies/<name>.py` with a class that subclasses `BaseStrategy` and implements `async analyze_opportunity(params) -> Intent | None`.
   - [ ] Read market data **only** from `params.features` / `params.latest_bar` (I1). If you need a new feature, do **Playbook C** first.
   - [ ] Return entries only: `kind=IntentKind.OPEN`. Exits belong to the OPM (**Playbook E**).
   - [ ] Don't touch the lock or publish anything yourself. `BaseStrategy.on_params_ready` handles the replay check (I5), the lock (I6) and publishing.
   - [ ] No blocking or network calls inside `analyze_opportunity` (I3). Heavy maths goes through `asyncio.to_thread`.
   - [ ] Size the quantity above the instrument's `min_notional` and at least one `step_size`. Otherwise the OMS rejects it at `LotSize` or `MinNotionalRule`.
   - [ ] Spot venue: SELL entries need the base asset already in the account (see `MockStrat`'s note). Long-only unless the venue supports shorting.
   - [ ] `BaseStrategy` stamps `reference_price` (the bar close you decided on) for slippage analysis in `trades`. Set it yourself only if your decision price is something else, such as a limit price.
2. **Register an ID.**
   - [ ] Add `MY_STRAT_ID = "..."` in `config/settings.py` next to `MOCK_STRAT_ID`.
   - [ ] Add it to `STRATEGY_REGISTRY` with `ExecutionMode.SIM`. **Always start in SIM.**
   - [ ] **Never reuse or rename an ID that has history.** It is the key in `positions`, `orders` and `fills`, so a rename splits the history and a reuse merges two strategies' records. Changing a strategy's logic materially should mean a new ID (e.g. `_v2`).
   - [ ] Keep the first 10 characters of the ID unique among strategies. They become the human-readable prefix of every client order ID (`execution/order_ids.py`).
3. **Wire it.**
   - [ ] In `main.py`, section 3, add the instance to the `strategies = [...]` list with `strategy_id=settings.MY_STRAT_ID`.
   - [ ] Every symbol the strategy trades must be in `settings.ACTIVE_ASSETS`. **Nothing checks this today.** A missing symbol means the strategy silently never trades.
   - [ ] `main.py` already refuses to start if the ID isn't in `STRATEGY_REGISTRY`.
4. **Persist the code.**
   - [ ] `.gitignore` excludes `strategies/*` except `base.py`, `mock_strat.py` and `__init__.py`. **Your strategy file is NOT committed.** Back it up deliberately (a private repo or encrypted backup), or whitelist it in `.gitignore`.
5. **Verify.**
   - [ ] Run the universal checklist (§1). In the soak-run log, look for `Strategy [<ID>] initialized and listening for {...}` with the right symbols.

### B. Modify an existing strategy

- [ ] Is it a material logic change? Then make it a **new strategy ID** (Playbook A) so the trade history of the old logic stays clean. Parameter tweaks may keep the ID; note the change date in the commit message.
- [ ] Never edit a strategy while it holds a position (see **Playbook L, restart**).
- [ ] If it is routed TESTNET or LIVE, re-run it in SIM first (temporarily point `STRATEGY_REGISTRY` at `SIM`).

### C. Add or change a feature, the lookback, or the bar interval

**Feature**
- [ ] Compute it in `DataProcessor._compute_features` (later: a pure function in `data/features.py`). It runs in a worker thread, so keep it pure: windows in, numbers out.
- [ ] Check the window: `MAX_LOOKBACK` (in bars) ≥ the longest window any feature needs.

**Lookback / backfill (an easy one to get wrong)**
- [ ] Keep **`MAX_BACKFILL_HOURS × bars-per-hour ≥ MAX_LOOKBACK`**. Today that's 4 h × 60 = 240 ≥ 100 ✓.
  At startup the warm-up loads the most recent `MAX_LOOKBACK` bars from the DB, but gap healing only repairs the last `MAX_BACKFILL_HOURS`. If the lookback is longer than the healed horizon and the system was down, the window will contain older bars from before the outage, with a **silent hole in the middle**. The DataProcessor only rejects bars that are not newer; it doesn't detect holes.
- [ ] No `ParamsReady` is published until a window is full, so a larger lookback delays the first signal after startup.

**Bar interval (`BAR_INTERVAL`)**
- [ ] `klines` is keyed by interval, so old data is kept and doesn't collide.
- [ ] Every "N bars" quantity changes meaning: features, `MAX_LOOKBACK`, OPM bars-held logic, and per-bar probabilities like `MockStrat`'s 5 %.
- [ ] Re-check the backfill inequality above.

### D. Add or remove a traded symbol

- [ ] Add it to `ACTIVE_ASSETS`. This drives the windows, the bar feed, the quote stream and exchangeInfo.
- [ ] At startup, check the log for `Instrument <SYM>: tick=... step=... minNotional=...`. If you see `exchangeInfo unavailable ... using conservative default filters` instead, **don't trade it live**: the default spec is a guess.
- [ ] The first run only has `MAX_BACKFILL_HOURS` of history, and that must cover `MAX_LOOKBACK` (Playbook C).
- [ ] Removing a symbol: make sure **no strategy holds a position in it** first, or the OPM will never see bars for that position again and can't exit.

### E. Change how the OPM manages open positions (exits)

The OPM (`opm/manager.py`) is the **only** component that exits positions. There are two routes:

- **Route A, strategy-directed:** a callable `(Position, MarketParams) -> bool` per strategy ID, passed as `strategy_evaluators={...}` to `OpenPositionManager(...)` in `main.py`.
  ⚠️ Today Route A is a placeholder. The evaluator call is commented out and replaced by RNG (`if random.random() > 0.05`), and `main.py` passes no evaluators. Replace the placeholder line with the evaluator call **before** using Route A.
- **Route B, independent:** `_independent_risk_check(pos, params)`. This is where trailing stop, take-profit and time-stop logic goes (planned: `opm/engines.py`).

Checklist
- [ ] Exit logic is pure and synchronous given `(pos, params)`. No network, no awaits (I3).
- [ ] Keep the existing gates in `on_params_ready`: skip on `event.replay` (I5) and on `not lock.can_opm_manage(...)`.
- [ ] Exits always go through `_trigger_exit`. It builds a `CLOSE` intent for `|quantity|` (which includes any carried sub-lot residual, so the exit sells everything sellable), stamps `reference_price`, acquires `try_begin_exit` (I6) and publishes at `EventPriority.HIGH`.
- [ ] The OMS enforces reduce-only: an exit can never flip or open a position. Don't work around this.
- [ ] Exits skip risk rules unless `applies_to_exits = True` (Playbook F). That's intentional: never block a stop-out.
- [ ] Exit timing: the OPM runs **before** strategies on each bar (`order=-10` vs `0`). Keep it that way so exits outrank entries.
- [ ] Note: `_bars_held` also counts replay bars. Time-based exits measure bars since open, including any backfilled during a reconnect.
- [ ] Test with the `System` fixture: open, then exit on the condition, then make sure there's no exit when the condition is false and no exit on replay.

### F. Add or change a pre-trade risk rule

- [ ] Subclass `RiskRule` in `risk/pretrade.py` and implement `evaluate(intent, request, market_state)`. Raise `RiskRejection(self.name, detail)` to block.
- [ ] **Pure and synchronous.** No I/O, no awaits. `request` is already rounded to exchange filters.
- [ ] **Fail closed:** if you need a reference price and `reference_price(...)` returns `None`, reject.
- [ ] Set `applies_to_exits` deliberately. Default `False` means the rule is skipped for exits. Only hard sanity rules (like `FatFingerRule`) should block exits.
- [ ] Order matters: rules run in list order and the first rejection wins. Stateful rules (like `DuplicateIntentRule`) record state even when a **later** rule rejects.
- [ ] Add it to the `rules=[...]` list of `PreTradeRiskManager` in `main.py`. The class name becomes `rule_name` in `risk_ledger`.
- [ ] Before any LIVE run: **remove `RandomMockRejectionRule`** (or set `RISK_MOCK_REJECTION_PROBABILITY = 0`).

### G. Promote a strategy: SIM → TESTNET → LIVE

- [ ] **SIM:** enough round trips that the result means something, a clean audit (§3), and you've reviewed the trade-level results.
- [ ] **TESTNET** (`ExecutionMode.TESTNET`): this only checks API integration (signing, filters, order types). Testnet prices and PnL are meaningless. Needs `BINANCE_TESTNET_API_KEY/SECRET` in `.env`; `settings.validate()` refuses to start without them.
- [ ] **LIVE**, only after **all** of the following:
  - [ ] Restart rehydration exists (readme §14). Until then, **never restart with an open live position** (Playbook L).
  - [ ] A kill switch exists, or at minimum you have a manual flatten procedure you've rehearsed.
  - [ ] `RandomMockRejectionRule` is removed. `RISK_MAX_ORDER_NOTIONAL` is sized for the account.
  - [ ] `python -m tools.reconcile_trades` passes on the SIM and TESTNET runs.
  - [ ] You start with minimum size.
- [ ] The change itself is a one-line edit of `STRATEGY_REGISTRY`. Nothing else should need to change; if it does, stop and find out why.

### H. Connect a new broker / venue

> ⚠️ **This is NOT a "zero core changes" job yet** (despite `readme.md` §13). A second *simulated or testnet* venue
> is mostly plumbing, but a second **live** broker hits real structural limits. Read the whole list before starting.

**Structural limits you'll hit (decide on each before writing code)**
- [ ] **One venue per execution mode.** `VenueRouter` maps `ExecutionMode → venue` and refuses a second venue for the same mode, and `STRATEGY_REGISTRY` maps strategy → mode only. Binance LIVE and IBKR LIVE side by side means changing the routing key to `(venue_id, mode)` in the router, the registry and the OMS.
- [ ] **Instruments are keyed by symbol only.** The same symbol on two venues collides in `InstrumentRegistry`, and in `klines` and the DataProcessor windows.
- [ ] **The market-data path is Binance-only in `main.py`** (`BinanceFeed`, `BinanceMarketDataREST`, `BinanceQuoteStream` → simulator).
- [ ] **Fee valuation** in the PortfolioManager understands only the base or quote asset. A fee in any third asset (BNB, or a USD commission on a non-USD instrument) is **excluded from PnL** with a warning.
- [ ] **One quote currency is assumed.** PnL in USDT and USD (or EUR) is never converted or aggregated.
- [ ] **Spot semantics:** no shorting or margin. Futures or margin need their own venue model and position semantics.
- [ ] **Trading hours:** crypto runs 24/7. For equities, gap healing and feature windows will span closed sessions, so decide how features treat overnight gaps.
- [ ] **Client order IDs** use the Binance format (≤36 chars, `[A-Za-z0-9._-]`). Check the new venue's rules.

**Implementing the execution adapter** (`connectors/<broker>/venue.py`, following `connectors/binance/venue.py`)
- [ ] Implement the `ExecutionVenue` protocol (`connectors/base.py`): `venue_id`, `mode`, `needs_polling`, `start`/`stop`, `set_update_handler`, `submit`, `cancel`, `query`.
- [ ] **Never publish on the bus and never mutate OMS state** (I1). Return `VenueOrderUpdate`s, or push them through the registered update handler.
- [ ] `submit()`: a **definite** refusal (4xx, validation, throttled client-side, connection refused before sending) returns a `REJECTED` update. An **ambiguous** outcome (timeout, disconnect mid-request, 5xx) **raises `VenueUnknownOutcome`** (I4). This is the single most important line in the adapter.
- [ ] `query()` returns the full current state **including all fills so far**, each with a **stable, unique exchange trade ID**. The OMS de-duplicates fills on it, so an unstable ID means double-counted fills. Return `None` only when the venue positively says the order doesn't exist.
- [ ] Fills report the actual `fee` and `fee_asset` from the venue. **Never compute fees from a configured rate** in a real adapter. The ledger, the dust carry (readme §8.1) and the trade journal all depend on fees being facts.
- [ ] No sleeping inside calls. Rate limits are a client-side guard that refuses immediately.
- [ ] Format prices and quantities with `InstrumentRegistry` (`format_qty`/`format_price`). Load real filters from the venue at startup.
- [ ] Put pure JSON ↔ model translation in a `codec.py` so it can be unit-tested without a network.

**Market data (if the broker is also a data source)**
- [ ] A feed implementing `MarketDataFeed`, publishing `BarClosed` with `replay=True` for backfilled bars and only **closed** bars.
- [ ] A `HistoricalBarSource` for gap healing (`fetch_bars(symbol, interval, start_ms, end_ms)`, closed bars only, oldest first).

**Wiring and verification**
- [ ] Construct and register it in `main.py`, section 2, with credentials from `.env` checked in `settings.validate()`.
- [ ] Unit tests for the codec plus a TESTNET or paper-account run of every order type you'll use.
- [ ] Test the unknown path deliberately: kill the network mid-submit and confirm UNKNOWN, then reconciliation, then the correct final state.

### I. Change the database schema

- [ ] Edit `state/schema.sql`. Timestamps are **INTEGER epoch ms UTC**, always.
- [ ] Bump `SCHEMA_VERSION` in `state/stores.py`. An existing DB with the old version **aborts startup** (`SchemaMismatchError`) by design.
- [ ] Before deploying, **archive the old DB** (e.g. `system.v2.YYYYMMDD.db`). Don't delete it: it is your trade history. If the history must carry over, write a one-off migration script and verify the row counts.
- [ ] Each new table has **exactly one writer**, and it writes through `SqliteStore` (one write connection per DB file). Add the write method to `SqliteStore`; never open a second connection.
- [ ] Update `readme.md` §8 (the table/key/writer list) and `tools/audit_db.py` if the table should be audited.
- [ ] Touching `fills`, `trades` or `trade_fills`? Update `tools/reconcile_trades.py` and keep `tests/test_trades.py` green. The reconciler is the independent check on the journal.

### J. Add a new event type or a new bus subscriber

- [ ] The event class goes in `messages/events.py` and is frozen. Its payload is frozen snapshots only (I8).
- [ ] Choose the priority: anything about a live order or position is `HIGH` (causality: a fill must never overtake its accept). Bars and features are `NORMAL`.
- [ ] Choose the handler's `order=` against the table in `readme.md` §4, and add your row to that table.
- [ ] A handler does in-memory work plus at most a local SQLite write (I3). Network work is spawned as a background task.
- [ ] One handler raising is isolated by the bus, but it is still a bug. Test it.

### K. Change simulator settings (fees, balances, latency)

- [ ] `SIM_*` in `config/settings.py`. These affect **only** the simulator's realism.
- [ ] Keep `SIM_TAKER_FEE`/`SIM_MAKER_FEE` at or above your **real** expected rates. Optimistic sim fees inflate paper PnL.
- [ ] The simulator is in-memory: balances reset to `SIM_INITIAL_BALANCES` on every restart.

### L. Operations: start, stop, restart, move data

**Start:** `python main.py`. Check the startup log for: instrument filters loaded (not defaults), `Healed N bar(s)`, `Warmed up ... (100/100)` for every symbol, `Binance WebSocket connected`, and `Quote stream connected`.

**Stop:** Ctrl+C or SIGTERM. A clean stop ends with `SqliteStore stopped (queue flushed)` and `Shutdown complete.` If those lines are missing, the last writes may be lost; audit before trusting the DB.

**Restart:** ⚠️ **Until restart rehydration exists, stop only when flat.** On startup the PortfolioManager, LockManager and OMS begin empty:
- SIM: an open position is forgotten. Its `positions` and `trades` rows stay `OPEN` forever, and the simulator's balances reset. Sub-lot residuals are forgotten too (harmless in SIM, because the simulator's balances reset with them).
- LIVE: the venue still holds the asset or the resting order, and the system doesn't know. That is unmanaged exposure.
- [ ] Check you're flat before stopping: `SELECT * FROM positions WHERE status='OPEN';` returns nothing, and there are no `HOLDING`/`PENDING_*` lock lines without a later `-> IDLE` in the log.

**Moving the DB or log between machines:** copy while the process is **stopped**. The DB runs in WAL mode, so copy `system.db` together with any `system.db-wal` / `system.db-shm` beside it, or let a clean shutdown checkpoint first.

---

## 3. Post-run audit (run after every soak run and every change)

Run `python -m tools.audit_db 24` for row counts and kline continuity, and
`python -m tools.reconcile_trades` for the trade journal. It must print `PASSED`. Warnings about a residual
not carried into the next trade are expected once per restart. Then run these queries
(`sqlite3 "file:system.db?mode=ro"`):

```sql
-- 1. Kline continuity: must return no rows
SELECT symbol, start_time, prev, (start_time - prev)/60000 - 1 AS missing_bars
FROM (SELECT symbol, start_time, LAG(start_time) OVER (PARTITION BY symbol, interval ORDER BY start_time) prev
      FROM klines WHERE interval = '1m')
WHERE start_time - prev <> 60000;

-- 2. Bar sanity: must be 0
SELECT COUNT(*) FROM klines
WHERE high < MAX(open, close) OR low > MIN(open, close) OR volume < 0 OR close_time <> start_time + 59999;

-- 3. Intent outcomes (I2): no NULL outcome may remain after a clean stop
SELECT kind, side, outcome, COUNT(*) FROM intents GROUP BY 1, 2, 3;
SELECT COUNT(*) FROM intents WHERE outcome IS NULL;                         -- must be 0

-- 4. Funnel: ORDERED intents = orders, and no order left non-terminal after a clean stop
SELECT (SELECT COUNT(*) FROM intents WHERE outcome = 'ORDERED') AS ordered,
       (SELECT COUNT(*) FROM orders) AS orders;
SELECT status, COUNT(*) FROM orders GROUP BY 1;   -- only FILLED / REJECTED / CANCELED / EXPIRED

-- 5. Orders agree with their fills
SELECT o.client_order_id, o.filled_quantity, SUM(f.quantity) AS fill_qty
FROM orders o LEFT JOIN fills f USING (client_order_id)
GROUP BY o.client_order_id HAVING ABS(o.filled_quantity - IFNULL(SUM(f.quantity), 0)) > 1e-12;  -- no rows

-- 6. Every position and trade closed at stop (see Playbook L)
SELECT * FROM positions WHERE status = 'OPEN';                               -- no rows
SELECT * FROM trades WHERE status = 'OPEN';                                  -- no rows

-- 7. Every position lifetime has its journal row
SELECT COUNT(*) FROM positions p LEFT JOIN trades t USING (trade_id) WHERE t.trade_id IS NULL;   -- 0
```

**Log checks**
- `grep -E "ERROR|CRITICAL" sys.log` should return nothing. Any `[LOCK] ... issued without acquiring the lock` or `opened ... while IDLE` line is an invariant violation (I6).
- Lock balance: each `PENDING_ENTRY` must end in `HOLDING` or `IDLE`, and each `PENDING_EXIT` in `IDLE` or `HOLDING`.
- Feed: count `[FEATURES]` lines per symbol. It should equal the minutes of runtime, and any `[BACKFILL] Recovered N` line should cover the disconnected window exactly.

**Cash reconciliation (SIM).** The final balances in the `SimulatedVenue stopped. Final balances:` line must equal the initial balance plus the fills:
`USDT_end = USDT_start − Σ buy price×qty + Σ sell price×qty − Σ USDT fees`, and
`BASE_end = Σ buy qty − Σ base-asset fees − Σ sell qty`.
After all positions are closed, `BASE_end` is the sub-lot residual (dust). It must equal the residual that
`reconcile_trades` reports as carried on that ledger, and it must be below one lot step (readme §8.1).

**Market-data truth check (optional, strongest).** Download the same window from Binance's public klines endpoint and compare OHLCV bar by bar. It should match exactly. This was done on the 2026-10-10 soak: 837/837 bars matched for both symbols.

---

## 4. Known limitations (read before relying on the system)

| Limitation | Consequence | Status |
|---|---|---|
| No restart rehydration | Restarting with an open position orphans it (Playbook L) | Planned, readme §14. **Blocks LIVE** |
| No kill switch or supervisor | No automated halt or flatten | Planned, readme §14. **Blocks LIVE** |
| Sub-lot residuals ("dust") live in memory | They are carried correctly while running (readme §8.1), but a restart forgets them: up to one lot step per ledger goes untracked on LIVE | Part of restart rehydration |
| MAE/MFE are bar-resolution | Price moves inside the entry bar after the fill aren't seen (only the fill price is), because that bar also contains pre-entry prices | Acceptable for 1m bars; tick data would be needed for more |
| One venue per mode; symbol-keyed instruments | Second live broker needs core changes | See Playbook H |
| Strategy symbols aren't checked against `ACTIVE_ASSETS` | A strategy can be silently inert | Small validation worth adding in `main.py` |
| Floats in ledgers | Tiny drift against the venue over many trades | Decimal ledgers planned, readme §14 |
| `strategies/*` is gitignored | Strategy code isn't versioned by this repo | Intentional; back it up separately |
