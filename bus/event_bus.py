"""
bus/event_bus.py

The Central Event Bus (Mediator).
An asynchronous priority queue that routes immutable events from publishers
to registered subscribers.

Rules:
1. The bus is a dumb pipe. It contains zero business or filtering logic.
2. Ordering is (priority, sequence): the sequence number is assigned at
   publish time, so events of equal priority are strictly FIFO and a replay
   of the same publish sequence dispatches identically.
3. Handlers for one event run sequentially, sorted by their explicit `order`
   (then subscription order). "The OPM evaluates exits before strategies see
   the bar" is declared with `order=`, not by constructor ordering in main.py.
4. Handlers are dispatched for the event's class AND its base classes, so a
   journal can subscribe once to `OrderEvent` (or `Event`).
5. Handlers MUST NOT block on network I/O. The loop is the system's single
   critical path; anything slow belongs in its own task (see execution/oms.py).
6. A crashing handler is logged and does not kill the loop.
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Type

from messages.events import Event, EventPriority

logger = logging.getLogger("EventBus")

# Sentinel that sorts after every real priority; used to wake run() on stop().
_STOP_PRIORITY = max(EventPriority) + 1


@dataclass(frozen=True, slots=True)
class _Subscription:
    """One registered handler: sorted by (order, seq) at dispatch time."""
    order: int
    seq: int
    handler: Callable


class EventBus:
    """
    The Mediator: components never call each other, they publish and subscribe here.

    State:
        _subscribers  Dict[type, List[_Subscription]]   registrations per event class
        _resolved     Dict[type, Tuple[handler]]        cache: MRO-merged, order-sorted handlers
        _queue        PriorityQueue[(priority, seq, event|None)]   None = stop sentinel
        _pub_seq      itertools.count                   FIFO tiebreak, assigned at publish
    API:
        subscribe(event_type, handler, order=0)   lower order runs first; base classes match subclasses
        publish(event) / publish_nowait(event)     enqueue only - never runs handlers inline
        run()                                      the drain loop (one task for the whole process)
        drain()                                    await until queue + cascades are processed
        stop()                                     enqueue the sentinel; run() exits after it
        depth                                      current queue length
    Handler contract: sync or async callables taking the event. Exceptions are
    logged and swallowed per handler; slow I/O must be moved to a task.

    Dispatch order convention (order=):
        -200 Journal  -100 Lock (intents/positions)  -50 Portfolio (fills)  -40 Lock (orders)
         -20 store.save_bar  -15 sim.on_bar  -10 OPM  0 strategies/OMS (default)  100 telemetry
    """

    def __init__(self) -> None:
        self._subscribers: Dict[Type[Event], List[_Subscription]] = {}
        self._resolved: Dict[Type[Event], Tuple[Callable, ...]] = {}
        self._sub_seq = itertools.count()
        self._pub_seq = itertools.count()
        self._queue: asyncio.PriorityQueue[Tuple[int, int, Optional[Event]]] = asyncio.PriorityQueue()
        self._running = False

    # -----------------------------------------------------------------
    # Subscription
    # -----------------------------------------------------------------
    def subscribe(self, event_type: Type[Event], handler: Callable, *, order: int = 0) -> None:
        """
        Registers a handler. Lower `order` runs first; ties run in subscription order.
        Subscribing to a base class receives every subclass event.
        """
        subs = self._subscribers.setdefault(event_type, [])
        if any(s.handler == handler for s in subs):
            return
        subs.append(_Subscription(order, next(self._sub_seq), handler))
        self._resolved.clear()
        logger.debug(f"Subscribed {getattr(handler, '__qualname__', handler)} to {event_type.__name__} (order={order})")

    def _handlers_for(self, event_type: Type[Event]) -> Tuple[Callable, ...]:
        cached = self._resolved.get(event_type)
        if cached is not None:
            return cached
        subs: List[_Subscription] = []
        for cls in event_type.__mro__:
            subs.extend(self._subscribers.get(cls, ()))
        subs.sort(key=lambda s: (s.order, s.seq))
        resolved = tuple(s.handler for s in subs)
        self._resolved[event_type] = resolved
        return resolved

    # -----------------------------------------------------------------
    # Publishing
    # -----------------------------------------------------------------
    async def publish(self, event: Event) -> None:
        """Fire-and-forget: enqueue and return. Never awaits a handler."""
        self.publish_nowait(event)

    def publish_nowait(self, event: Event) -> None:
        self._queue.put_nowait((int(event.priority), next(self._pub_seq), event))

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    # -----------------------------------------------------------------
    # Drain loop
    # -----------------------------------------------------------------
    async def run(self) -> None:
        self._running = True
        logger.info("Event Bus started.")
        while self._running:
            try:
                _, _, event = await self._queue.get()
            except asyncio.CancelledError:
                logger.info("Event Bus loop cancelled.")
                break
            try:
                if event is None:      # stop sentinel
                    break
                await self._dispatch(event)
            except asyncio.CancelledError:
                logger.info("Event Bus loop cancelled.")
                break
            except Exception as e:
                logger.error(f"Critical error in event bus loop: {e}", exc_info=True)
            finally:
                self._queue.task_done()
        self._running = False
        logger.info("Event Bus stopped.")

    async def _dispatch(self, event: Event) -> None:
        for handler in self._handlers_for(type(event)):
            try:
                result = handler(event)
                if inspect.isawaitable(result):
                    await result
            except Exception as e:
                logger.error(
                    f"Handler {getattr(handler, '__qualname__', handler)} failed processing "
                    f"{type(event).__name__}: {e}",
                    exc_info=True,
                )

    async def drain(self) -> None:
        """Waits until every event published so far (and their cascades) is dispatched."""
        await self._queue.join()

    def stop(self) -> None:
        """
        Stops the loop after the events already queued AHEAD of the sentinel.
        Uses a sentinel because flipping a flag cannot wake a blocked get().
        """
        self._queue.put_nowait((_STOP_PRIORITY, next(self._pub_seq), None))
