"""
connectors/base.py

THE PORTS. The entire surface the green core is allowed to see of a venue.
The OMS depends on `ExecutionVenue`; Binance, the Binance testnet and the
internal simulator are interchangeable adapters behind it.

Contract of an ExecutionVenue:
  * It NEVER publishes onto the Event Bus and NEVER mutates OMS state. It
    returns `VenueOrderUpdate`s (synchronously from submit/cancel/query) or
    pushes them through the update handler the OMS registers. The OMS is the
    single writer of order state and the only publisher of order events.
  * submit() raises `VenueUnknownOutcome` when the request may or may not
    have reached the matching engine (timeout, disconnect mid-flight, 5xx).
    It must never raise for a definite rejection - return a REJECTED update.
  * query() returns None only when the venue positively reports that the
    client order id does not exist.
  * fetch_balances() reports the ACCOUNT's holdings (free + locked) per asset.
    The account may hold more than the system attributes to its strategies;
    rehydration and the kill switch only ever compare and trade the
    system-attributed part.
"""

from __future__ import annotations

from typing import Awaitable, Callable, Dict, List, Optional, Protocol, runtime_checkable

from messages.model import Bar, ExecutionMode, OrderRequest, VenueOrderUpdate

UpdateHandler = Callable[[VenueOrderUpdate], Awaitable[None]]


class VenueUnknownOutcome(Exception):
    """
    Raised by submit() when the request may or may not have reached the matching
    engine (timeout, disconnect mid-flight, 5xx). The OMS marks the order UNKNOWN
    and reconciles. Definite refusals (4xx, client-side throttle, connect failure)
    are NOT exceptions - they come back as a REJECTED VenueOrderUpdate.
    """


@runtime_checkable
class ExecutionVenue(Protocol):
    """
    Port for order execution. Implementations: connectors/binance/venue.BinanceVenue
    (TESTNET, LIVE) and connectors/sim/venue.SimulatedVenue (SIM).

    Attributes:
        venue_id        e.g. "BINANCE" (stamped on OrderRequest.venue)
        mode            the ExecutionMode this instance serves (router key)
        needs_polling   True -> the OMS polls working orders via query()
        survives_restart  True -> orders and balances outlive OUR process (a real exchange):
                        rehydration reconciles working orders against it. False -> the venue
                        forgets everything on restart (the simulator): the store is the truth,
                        and rehydration restores the venue's balances from the fills ledger.
    Methods:
        start() / stop()                     open / close sessions and background tasks
        set_update_handler(cb)               OMS registers its push-report callback
        submit(req) -> VenueOrderUpdate      may raise VenueUnknownOutcome
        cancel(sym, coid) -> update | None   None = venue does not know the order
        query(sym, coid) -> update | None    full current state incl. ALL fills so far
        fetch_balances() -> {asset: total}   free + locked, whole account
    """
    venue_id: str
    mode: ExecutionMode
    # True if the venue cannot push execution reports and the OMS must poll
    # working orders with query().
    needs_polling: bool
    survives_restart: bool

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def set_update_handler(self, handler: UpdateHandler) -> None: ...

    async def submit(self, request: OrderRequest) -> VenueOrderUpdate: ...
    async def cancel(self, symbol: str, client_order_id: str) -> Optional[VenueOrderUpdate]: ...
    async def query(self, symbol: str, client_order_id: str) -> Optional[VenueOrderUpdate]: ...
    async def fetch_balances(self) -> Dict[str, float]: ...


@runtime_checkable
class MarketDataFeed(Protocol):
    """Port for live bars: start() runs until stop(); publishes BarClosed onto the bus."""
    async def start(self) -> None: ...
    def stop(self) -> None: ...


@runtime_checkable
class HistoricalBarSource(Protocol):
    """Port for historical closed bars (gap healing, reconnect backfill)."""
    async def fetch_bars(self, symbol: str, interval: str, start_ms: int,
                         end_ms: Optional[int] = None) -> List[Bar]:
        """Closed bars with start_time >= start_ms (and <= end_ms), oldest first."""
        ...
