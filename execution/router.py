"""
execution/router.py

VenueRouter: which ExecutionVenue receives a strategy's orders.
Replaces the paper/live if-else that used to live inside the OMS. A missing
route is a normal, journaled IntentRejected - never an AttributeError on None.
"""

from typing import Dict, Iterable, Optional

from connectors.base import ExecutionVenue
from messages.model import ExecutionMode


class VenueRouter:
    """
    ExecutionMode -> ExecutionVenue map (one venue per mode).
    API:
        register(venue)              raises if the mode is already taken
        venue_for(mode) -> venue|None
        all() -> [venue]             for start/stop and handler registration
    """

    def __init__(self, venues: Iterable[ExecutionVenue] = ()):
        self._venues: Dict[ExecutionMode, ExecutionVenue] = {}
        for v in venues:
            self.register(v)

    def register(self, venue: ExecutionVenue) -> None:
        if venue.mode in self._venues:
            raise ValueError(f"A venue is already registered for mode {venue.mode.name}")
        self._venues[venue.mode] = venue

    def venue_for(self, mode: ExecutionMode) -> Optional[ExecutionVenue]:
        return self._venues.get(mode)

    def all(self) -> list[ExecutionVenue]:
        return list(self._venues.values())
