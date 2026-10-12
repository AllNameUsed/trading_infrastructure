"""
risk/kill_switch.py

The kill switch: the SINGLE WRITER of the halt state of every execution mode.

Levels (messages.model.HaltLevel), per ExecutionMode, latching:
  HALT_ENTRIES   the OMS rejects every new entry (IntentRejected stage HALTED; the
                 strategies keep running and their signals are journaled). Working
                 orders run their course - a resting entry can still fill - and the
                 OPM keeps managing exits.
  HALT_COMPLETE  as above, plus the flatten loop: cancel every working order of the
                 mode, then sell (FLATTEN, MARKET) every SYSTEM-ATTRIBUTED position, i.e.
                 what the strategy ledgers hold - never the rest of the account. Repeats
                 until nothing is working and nothing is held above one lot step, then
                 records `flattened_at` and checks the venue's balances.

Why the policy is here and not in each venue:
  A venue that "sold everything" by itself would bypass the OMS: its fills would never
  reach the PortfolioManager and the ledgers would silently diverge from the account.
  The flatten therefore goes through the normal pipeline (OMS -> venue -> fills ->
  portfolio -> lock), as FLATTEN intents that no risk rule may block. Venue specifics
  live behind the ExecutionVenue port (cancel, query, fetch_balances).

Triggers (escalate only; nothing here ever lowers a level):
  * RiskSupervisor drawdown rules            -> HALT_COMPLETE
  * RiskBreach (e.g. an order vanished)      -> HALT_ENTRIES (its mode, or every mode)
  * rehydration integrity problems           -> HALT_ENTRIES
  * the operator, config/kill_switch.ini     -> `halt = HALT_ENTRIES | HALT_COMPLETE`

Operator control (two files, one writer each):
  config/kill_switch.ini         written by YOU; polled every `poll_seconds`.
      halt  = NONE | HALT_ENTRIES | HALT_COMPLETE   level-triggered manual halt, all modes:
                                                    while it says HALT_*, the modes stay
                                                    halted at least at that level
      reset = <halt id>                             clears exactly that episode. A stale id
                                                    does nothing, so an old value can never
                                                    release a new halt. Refused while the
                                                    file still requests a manual halt.
  config/kill_switch_status.txt  written by the SYSTEM on every change: level, halt id,
                                 cause and flatten progress per mode, and how to resume.
  The episode is persisted (`halts` table, durable) BEFORE it is announced, and reloaded at
  startup: a halt survives restarts until the operator resets it.
"""

from __future__ import annotations

import asyncio
import configparser
import dataclasses
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from messages.events import EventPriority, IntentIssued, KillSwitch, RiskBreach
from messages.model import (
    ExecutionMode,
    HaltLevel,
    HaltRecord,
    Intent,
    IntentKind,
    OrderType,
    new_id,
    utc_now,
)

logger = logging.getLogger("KillSwitch")

CONTROL_TEMPLATE = """\
; =====================================================================
; Kill switch control. YOU edit this file; the trading system reads it every
; few seconds and writes its answer to kill_switch_status.txt next to it.
; =====================================================================
[kill_switch]
; Manual halt, applied to EVERY execution mode:
;   NONE           no manual halt requested
;   HALT_ENTRIES   no new entries; working orders and open positions run their course
;   HALT_COMPLETE  cancel all working orders, sell every system-attributed position, no entries
; While this says HALT_*, the system stays halted at least at that level.
halt = NONE

; Resume trading: paste the halt id shown in kill_switch_status.txt (and in the log), e.g.
;   reset = SIM-20261012-081500-a1b2
; A manual halt can only be reset once `halt` above is back to NONE. An old id does nothing.
reset =
"""


class KillSwitchController:
    """
    Halt state per execution mode + the flatten procedure + the operator control files.

    Subscribes:  RiskBreach -> HALT_ENTRIES
    Publishes:   KillSwitch (every level change), IntentIssued(FLATTEN, CRITICAL)
    Depends on:  store (halts), oms (cancel_all_working, working_orders), portfolio
                 (get_open_positions, ledgers), lock (can_opm_manage, try_begin_exit), router
                 (venue_for -> fetch_balances, survives_restart), instruments (base assets),
                 market_view(symbol) -> {"close"} (the flatten's reference price)
    State:
        _active      Dict[mode, HaltRecord]   the open episode per mode (absent = NONE)
        _last_clear  Dict[mode, datetime]     when the mode was last reset (drawdown baseline)
    API:
        level(mode) -> HaltLevel                     synchronous: the OMS entry gate
        trigger(mode, level, source, reason) -> bool escalate (or open) the mode's episode
        reset(halt_id, detail) -> bool               operator release; reset listeners run synchronously
                                                     (before any await) so the release is atomic
        add_reset_listener(cb(mode))                 the RiskSupervisor re-baselines its peaks
        last_reset(mode) -> datetime | None
        load() / start() / stop()                    reload episodes / control poller + flatten loops
        poll_control_file()                          one read of config/kill_switch.ini
    """

    def __init__(self, bus, store, *, oms, portfolio, lock, router, instruments,
                 market_view: Callable[[str], dict], modes: Iterable[ExecutionMode],
                 control_path: Path, status_path: Path,
                 poll_seconds: float = 2.0, flatten_retry_seconds: float = 3.0):
        self.bus = bus
        self.store = store
        self.oms = oms
        self.portfolio = portfolio
        self.lock = lock
        self.router = router
        self.instruments = instruments
        self.market_view = market_view
        self.modes = sorted(set(modes), key=lambda m: m.value)
        self.control_path = Path(control_path)
        self.status_path = Path(status_path)
        self.poll_seconds = poll_seconds
        self.flatten_retry_seconds = flatten_retry_seconds

        self._active: Dict[ExecutionMode, HaltRecord] = {}
        self._last_clear: Dict[ExecutionMode, datetime] = {}
        self._reset_listeners: List[Callable[[ExecutionMode], None]] = []
        self._flatteners: Dict[ExecutionMode, asyncio.Task] = {}
        self._poller: Optional[asyncio.Task] = None
        self._last_control: Optional[Tuple[str, str]] = None
        self._control_mtime: Optional[float] = None

        bus.subscribe(RiskBreach, self._on_breach)

    # =================================================================
    # Read model (synchronous)
    # =================================================================
    def level(self, mode: ExecutionMode) -> HaltLevel:
        rec = self._active.get(mode)
        return rec.level if rec else HaltLevel.NONE

    def active(self, mode: ExecutionMode) -> Optional[HaltRecord]:
        return self._active.get(mode)

    def last_reset(self, mode: ExecutionMode) -> Optional[datetime]:
        return self._last_clear.get(mode)

    def add_reset_listener(self, callback: Callable[[ExecutionMode], None]) -> None:
        self._reset_listeners.append(callback)

    # =================================================================
    # Lifecycle
    # =================================================================
    async def load(self) -> None:
        """Startup: reload open episodes (a halt survives restarts) and the last reset per mode."""
        for rec in await self.store.get_halts():
            if rec.cleared_at is not None:
                prev = self._last_clear.get(rec.mode)
                self._last_clear[rec.mode] = rec.cleared_at if prev is None or rec.cleared_at > prev else prev
                continue
            self._active[rec.mode] = rec
            logger.critical(f"[KILL SWITCH] {rec.mode.name} is still {rec.level.name} from before the restart "
                            f"(halt {rec.halt_id}, {rec.source}: {rec.reason}).")

    async def start(self) -> None:
        """Reads the control file once (a manual halt applies before the first bar), then runs."""
        self._ensure_control_file()
        await self.poll_control_file()
        self._write_status()
        for mode, rec in self._active.items():
            if rec.level is HaltLevel.HALT_COMPLETE:
                self._start_flatten(mode)
        self._poller = asyncio.create_task(self._poll_loop(), name="kill-switch-control")

    async def stop(self) -> None:
        tasks = [t for t in [self._poller, *self._flatteners.values()] if t is not None]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._poller = None
        self._flatteners.clear()

    # =================================================================
    # Trigger / reset
    # =================================================================
    async def trigger(self, mode: ExecutionMode, level: HaltLevel, source: str, reason: str) -> bool:
        """Opens or escalates the mode's episode. Never lowers it. True if the level rose."""
        if level is HaltLevel.NONE:
            return False
        current = self._active.get(mode)
        if current is not None and current.level >= level:
            return False
        now = utc_now()
        if current is None:
            rec = HaltRecord(halt_id=f"{mode.value}-{now:%Y%m%d-%H%M%S}-{new_id()[:4]}", mode=mode, level=level,
                             source=source, reason=reason, triggered_at=now)
        else:
            rec = dataclasses.replace(current, level=level, escalated_at=now,
                                      reason=f"{current.reason} | escalated by {source}: {reason}")
        previous = current.level if current else HaltLevel.NONE
        self._active[mode] = rec          # the OMS gate closes NOW, before any await
        logger.critical(f"[KILL SWITCH] {mode.name}: {previous.name} -> {level.name} ({source}: {reason}). "
                        f"Halt id {rec.halt_id}.")
        await self.store.save_halt(rec)
        await self.bus.publish(KillSwitch(mode=mode, level=level, previous_level=previous, halt_id=rec.halt_id,
                                          source=source, reason=reason))
        self._write_status()
        if level is HaltLevel.HALT_COMPLETE:
            self._start_flatten(mode)
        return True

    async def reset(self, halt_id: str, detail: str) -> bool:
        """Operator release of exactly the episode `halt_id`. False if it is not active."""
        for mode, rec in list(self._active.items()):
            if rec.halt_id != halt_id:
                continue
            now = utc_now()
            cleared = dataclasses.replace(rec, cleared_at=now, clear_detail=detail)
            # Everything that defines "reset" happens before the first await: the gate opens AND the
            # drawdown peaks re-baseline atomically, so no bar evaluated in between can re-trip
            # the old drawdown against a mode that is already NONE.
            del self._active[mode]
            self._last_clear[mode] = now
            for cb in self._reset_listeners:
                try:
                    cb(mode)
                except Exception:
                    logger.exception("Reset listener failed")
            task = self._flatteners.pop(mode, None)
            if task is not None:
                task.cancel()
            logger.critical(f"[KILL SWITCH] {mode.name}: {rec.level.name} -> NONE (halt {halt_id} reset: {detail}). "
                            f"Trading resumes.")
            await self.store.save_halt(cleared)
            await self.bus.publish(KillSwitch(mode=mode, level=HaltLevel.NONE, previous_level=rec.level,
                                              halt_id=halt_id, source="operator", reason=detail))
            self._write_status()
            return True
        return False

    async def _on_breach(self, event: RiskBreach) -> None:
        for mode in ([event.mode] if event.mode is not None else self.modes):
            await self.trigger(mode, HaltLevel.HALT_ENTRIES, f"breach:{event.rule_name}",
                               f"{event.symbol or '-'}: {event.detail}")

    # =================================================================
    # Operator control file
    # =================================================================
    def _ensure_control_file(self) -> None:
        if not self.control_path.exists():
            self.control_path.parent.mkdir(parents=True, exist_ok=True)
            self.control_path.write_text(CONTROL_TEMPLATE, encoding="utf-8")
            logger.info(f"Created the kill switch control file {self.control_path}")

    def _read_control(self) -> Optional[Tuple[HaltLevel, str]]:
        """(manual level, reset id), or None if the file is missing/unreadable (nothing changes)."""
        parser = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
        try:
            with open(self.control_path, "r", encoding="utf-8") as f:
                parser.read_file(f)
            section = parser["kill_switch"]
            raw_halt = (section.get("halt", "NONE") or "NONE").strip().upper()
            reset_id = (section.get("reset", "") or "").strip()
            return HaltLevel[raw_halt], reset_id
        except FileNotFoundError:
            self._ensure_control_file()
            return None
        except Exception as e:
            mtime = self.control_path.stat().st_mtime if self.control_path.exists() else None
            if mtime != self._control_mtime:
                logger.error(f"[KILL SWITCH] Cannot parse {self.control_path} ({type(e).__name__}: {e}); "
                             f"ignoring it until it is fixed. Valid halt values: NONE, HALT_ENTRIES, HALT_COMPLETE.")
                self._control_mtime = mtime
            return None

    async def poll_control_file(self) -> None:
        control = self._read_control()
        if control is None:
            return
        manual, reset_id = control
        seen = (manual.name, reset_id)
        changed = seen != self._last_control
        self._last_control = seen
        if manual is not HaltLevel.NONE:
            for mode in self.modes:
                await self.trigger(mode, manual, "manual", f"operator: halt = {manual.name} in {self.control_path.name}")
        if reset_id:
            for mode, rec in list(self._active.items()):
                if rec.halt_id != reset_id:
                    continue
                if manual is not HaltLevel.NONE:
                    if changed:
                        logger.warning(f"[KILL SWITCH] Reset of {reset_id} refused: {self.control_path.name} still "
                                       f"requests halt = {manual.name}. Set halt = NONE to resume.")
                    continue
                await self.reset(reset_id, f"operator reset via {self.control_path.name}")

    async def _poll_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.poll_seconds)
                await self.poll_control_file()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("[KILL SWITCH] Control file poll failed")

    def _write_status(self) -> None:
        lines = ["# Written by the trading system - do not edit. Edit kill_switch.ini instead.",
                 f"updated_at = {utc_now().isoformat(timespec='seconds')}", ""]
        for mode in self.modes:
            rec = self._active.get(mode)
            lines.append(f"[{mode.name}]")
            if rec is None:
                lines += ["level = NONE (trading)", ""]
                continue
            lines += [f"level        = {rec.level.name}",
                      f"halt_id      = {rec.halt_id}",
                      f"source       = {rec.source}",
                      f"reason       = {rec.reason}",
                      f"triggered_at = {rec.triggered_at.isoformat(timespec='seconds')}"]
            if rec.level is HaltLevel.HALT_COMPLETE:
                lines.append("flattened_at = " + (rec.flattened_at.isoformat(timespec='seconds')
                                                  if rec.flattened_at else "PENDING (flatten in progress)"))
            hint = "set `halt = NONE` and " if rec.source == "manual" else ""
            lines += [f"to resume    : {hint}set `reset = {rec.halt_id}` in {self.control_path.name}", ""]
        try:
            self.status_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.status_path.with_suffix(".tmp")
            tmp.write_text("\n".join(lines), encoding="utf-8")
            os.replace(tmp, self.status_path)
        except Exception:
            logger.exception(f"[KILL SWITCH] Could not write {self.status_path}")

    # =================================================================
    # HALT_COMPLETE: cancel everything, flatten every system-attributed position
    # =================================================================
    def _start_flatten(self, mode: ExecutionMode) -> None:
        task = self._flatteners.get(mode)
        if task is None or task.done():
            self._flatteners[mode] = asyncio.create_task(self._flatten_loop(mode), name=f"flatten-{mode.value}")

    async def _flatten_loop(self, mode: ExecutionMode) -> None:
        attempts = 0
        while self.level(mode) is HaltLevel.HALT_COMPLETE:
            try:
                flat = await self._flatten_step(mode)
                attempts = 0 if flat else attempts + 1
                if attempts and attempts % 10 == 0:
                    logger.critical(f"[KILL SWITCH] {mode.name}: still not flat after {attempts} attempts "
                                    f"({self._exposure_summary(mode)}). Retrying.")
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception(f"[KILL SWITCH] {mode.name}: flatten step failed; retrying")
            try:
                await asyncio.sleep(self.flatten_retry_seconds)
            except asyncio.CancelledError:
                return

    def _exposure(self, mode: ExecutionMode):
        working = [o for o in self.oms.working_orders() if o.mode is mode]
        positions = [p for p in self.portfolio.get_open_positions() if p.mode is mode]
        return working, positions

    def _exposure_summary(self, mode: ExecutionMode) -> str:
        working, positions = self._exposure(mode)
        return (f"{len(working)} working order(s), positions: "
                + (", ".join(f"{p.strategy_id}/{p.symbol} {p.quantity:.8g}" for p in positions) or "none"))

    async def _flatten_step(self, mode: ExecutionMode) -> bool:
        """One round: cancel what works, flatten what is held. True when the mode is flat."""
        self.oms.cancel_all_working(mode)
        working, positions = self._exposure(mode)
        rec = self._active.get(mode)
        if not working and not positions:
            if rec is not None and rec.flattened_at is None:
                await self._mark_flattened(mode, rec)
            return True
        if rec is not None and rec.flattened_at is not None:
            logger.critical(f"[KILL SWITCH] {mode.name}: exposure reappeared after the flatten "
                            f"({self._exposure_summary(mode)}); flattening again.")
            rec = dataclasses.replace(rec, flattened_at=None)
            self._active[mode] = rec
            await self.store.save_halt(rec)
            self._write_status()
        for pos in positions:
            # Wait while an order of this ledger is still working (being cancelled, or an exit in flight).
            if not self.lock.can_opm_manage(pos.strategy_id, pos.symbol) or pos.side is None:
                continue
            px = (self.market_view(pos.symbol) or {}).get("close")
            intent = Intent(strategy_id=pos.strategy_id, symbol=pos.symbol, venue=pos.venue,
                            kind=IntentKind.FLATTEN, side=pos.side.opposite, order_type=OrderType.MARKET,
                            quantity=abs(pos.quantity), reason=f"Kill switch {rec.halt_id if rec else ''}".strip(),
                            reference_price=float(px) if px else None)
            if self.lock.try_begin_exit(intent):
                logger.critical(f"[KILL SWITCH] {mode.name}: flattening {pos.strategy_id}/{pos.symbol} "
                                f"{intent.side.name} {abs(pos.quantity):.8g} (MARKET).")
                await self.bus.publish(IntentIssued(intent=intent, priority=EventPriority.CRITICAL))
        return False

    async def _mark_flattened(self, mode: ExecutionMode, rec: HaltRecord) -> None:
        rec = dataclasses.replace(rec, flattened_at=utc_now())
        self._active[mode] = rec
        await self.store.save_halt(rec)
        logger.critical(f"[KILL SWITCH] {mode.name}: FLAT. No working order and no system-attributed position "
                        f"left (halt {rec.halt_id}). Entries stay blocked until reset.")
        self._write_status()
        await self._verify_venue(mode)

    async def _verify_venue(self, mode: ExecutionMode) -> None:
        """After a flatten: the venue must hold at least what the ledgers say (exactly, for the simulator)."""
        venue = self.router.venue_for(mode)
        if venue is None:
            return
        try:
            balances = await venue.fetch_balances()
        except Exception as e:
            logger.error(f"[KILL SWITCH] {mode.name}: could not fetch venue balances to verify the flatten: {e}")
            return
        expected: Dict[str, float] = {}
        for pos in self.portfolio.ledgers():
            spec = self.instruments.get(pos.symbol)
            if pos.mode is mode and spec is not None:
                expected[spec.base_asset] = expected.get(spec.base_asset, 0.0) + pos.quantity
        for asset, qty in sorted(expected.items()):
            held = balances.get(asset, 0.0)
            ok = abs(held - qty) <= 1e-9 if not venue.survives_restart else held + 1e-9 >= qty
            (logger.info if ok else logger.error)(
                f"[KILL SWITCH] {mode.name} venue check {asset}: venue holds {held:.12g}, "
                f"ledgers hold {qty:.12g} (sub-lot residuals) -> {'OK' if ok else 'MISMATCH'}")
