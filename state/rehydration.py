"""
state/rehydration.py

Restart rehydration: after a restart the process resumes exactly where the store (and,
for a real exchange, the venue) says it left off. Run once by main.py after the windows
are warm and the venues have started, BEFORE the bus, the OMS poller and the feeds run,
so nothing can act on half-restored state.

Order (each step only reads what the previous steps restored):
  1. KillSwitchController.load()   open halt episodes survive the restart (and their latch)
  2. PortfolioManager.rehydrate()  ledgers (incl. sub-lot residuals), open trades, PnL totals
  3. venues that forget on restart (the simulator): restore_from_fills() - the account is
     its initial balances plus every recorded fill, so it matches the ledgers exactly
  4. OMS.rehydrate()               working orders: reconciled by query on a real exchange,
                                   closed as CANCELED on the simulator (they died with it)
  5. LockManager / OPM             derived from the restored positions and working orders
  6. venue check                   every venue must hold at least what the ledgers attribute
                                   to the system (exactly, for the simulator)
  7. RiskSupervisor.rehydrate()    drawdown windows and high-water marks (needs step 1's resets)
Any integrity problem halts the affected mode(s) at HALT_ENTRIES: nothing new is opened on
state the system cannot vouch for, while existing positions stay managed.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from messages.model import ExecutionMode, HaltLevel

logger = logging.getLogger("Rehydration")


class Rehydrator:
    """
    Startup orchestrator (no state of its own).
    Depends on: store, portfolio, oms, lock, opm, router, instruments, kill_switch, supervisor
    API:  run() -> {mode or None: [problem]}   None = not attributable to one mode (halts every mode)
    """

    def __init__(self, store, *, portfolio, oms, lock, opm, router, instruments, kill_switch, supervisor,
                 bar_seconds: float = 60.0):
        self.store = store
        self.portfolio = portfolio
        self.oms = oms
        self.lock = lock
        self.opm = opm
        self.router = router
        self.instruments = instruments
        self.kill_switch = kill_switch
        self.supervisor = supervisor
        self.bar_seconds = bar_seconds

    async def run(self) -> Dict[Optional[ExecutionMode], List[str]]:
        problems: Dict[Optional[ExecutionMode], List[str]] = {}

        await self.kill_switch.load()
        for p in await self.portfolio.rehydrate():
            problems.setdefault(None, []).append(p)

        for venue in self.router.all():
            if not venue.survives_restart:
                venue.restore_from_fills(await self.store.get_fills_for_mode(venue.mode))

        entries = await self.store.get_working_orders()
        for p in await self.oms.rehydrate(entries):
            problems.setdefault(None, []).append(p)

        positions = self.portfolio.get_open_positions()
        self.lock.rehydrate(positions, self.oms.working_orders())
        self.opm.rehydrate(positions, bar_seconds=self.bar_seconds)

        for venue in self.router.all():
            for p in await self._check_venue(venue):
                problems.setdefault(venue.mode, []).append(p)

        await self.supervisor.rehydrate()

        for mode, found in problems.items():
            detail = "; ".join(found)
            for m in ([mode] if mode is not None else self.kill_switch.modes):
                await self.kill_switch.trigger(m, HaltLevel.HALT_ENTRIES, "rehydration",
                                               detail if len(detail) <= 900 else detail[:897] + "...")
        n_problems = sum(len(v) for v in problems.values())
        logger.info(f"Rehydration complete: {len(positions)} open position(s), {len(self.oms.working_orders())} "
                    f"working order(s), {n_problems} integrity problem(s)."
                    + ("" if not n_problems else " Affected modes start HALTED (HALT_ENTRIES)."))
        return problems

    async def _check_venue(self, venue) -> List[str]:
        """The venue must hold at least what the ledgers attribute to the system (exactly, if it forgot)."""
        expected: Dict[str, float] = {}
        for pos in self.portfolio.ledgers():
            spec = self.instruments.get(pos.symbol)
            if pos.mode is venue.mode and spec is not None:
                expected[spec.base_asset] = expected.get(spec.base_asset, 0.0) + pos.quantity
        if not expected:
            return []
        try:
            held = await venue.fetch_balances()
        except Exception as e:
            return [f"{venue.mode.name}: could not fetch balances to verify the ledgers ({e})"]
        problems = []
        for asset, qty in sorted(expected.items()):
            have = held.get(asset, 0.0)
            ok = abs(have - qty) <= 1e-9 if not venue.survives_restart else have + 1e-9 >= qty
            if ok:
                logger.info(f"[{venue.mode.name}] {asset}: venue holds {have:.12g}, ledgers attribute {qty:.12g} - OK")
            else:
                problems.append(f"{venue.mode.name} {asset}: venue holds {have:.12g} but the ledgers attribute "
                                f"{qty:.12g} to the system")
        return problems
