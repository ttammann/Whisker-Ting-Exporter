"""Time sources, injectable so tests can run minutes of protocol time in a second.

`monotonic` drives staleness, backoff and rate limits; `time` is the wall clock
(arrival stamps, the delay metric). Nothing else in the package calls
time.monotonic(), time.time() or asyncio.sleep() for policy decisions.
"""

from __future__ import annotations

import asyncio
import time


class Clock:
    def monotonic(self) -> float:
        return time.monotonic()

    def time(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))

    def real(self, seconds: float) -> float:
        """Real seconds for a policy duration (a timeout passed to asyncio)."""
        return max(0.0, seconds)


class ScaledClock(Clock):
    """Runs `scale` times faster than real time (tests). Monotonic and wall time both scale."""

    def __init__(self, scale: float, start_wall: float | None = None) -> None:
        self.scale = scale
        self._real0 = time.monotonic()
        self._wall0 = time.time() if start_wall is None else start_wall

    def monotonic(self) -> float:
        return (time.monotonic() - self._real0) * self.scale

    def time(self) -> float:
        return self._wall0 + self.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds) / self.scale)

    def real(self, seconds: float) -> float:
        return max(0.0, seconds) / self.scale
