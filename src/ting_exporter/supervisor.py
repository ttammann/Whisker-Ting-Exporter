"""Task supervision: a crashed task is logged, counted and restarted; nothing exits the process."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Awaitable, Callable

from .clock import Clock

log = logging.getLogger(__name__)

RESTART_MIN, RESTART_MAX = 1.0, 60.0
STABLE_AFTER = 300.0


async def supervise(
    name: str,
    factory: Callable[[], Awaitable[None]],
    stop: asyncio.Event,
    restarts: Counter[str],
    clock: Clock | None = None,
) -> None:
    """Run `factory()` until `stop` is set, restarting it with backoff if it raises or returns early."""
    clock = clock or Clock()
    delay = RESTART_MIN
    while not stop.is_set():
        started = clock.monotonic()
        try:
            await factory()
            if stop.is_set():
                return
            log.error("task %s returned unexpectedly; restarting", name)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("task %s crashed; restarting", name)
        restarts[name] += 1
        if clock.monotonic() - started > STABLE_AFTER:
            delay = RESTART_MIN
        try:
            await asyncio.wait_for(stop.wait(), clock.real(delay))
        except asyncio.TimeoutError:
            pass
        delay = min(delay * 2, RESTART_MAX)
