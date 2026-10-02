"""Push the outbox to VictoriaMetrics: one pusher per target, each from its own cursor (design 7.2, 6.1).

    POST <vm>/api/v1/import/prometheus    Content-Encoding: gzip
    ting_voltage_volts{serial="TNG000001",site="a"} 124.996 1790603992097

Timestamps are device milliseconds, so a resent batch (a timeout after the
store had stored it, a cursor written late, a replayed file) is identical and
VictoriaMetrics keeps one (-dedup.minScrapeInterval=1ms).

A pusher sends up to LIVE_BATCH lines (CATCHUP_BATCH while it is more than
LIVE_BATCH behind), at most RATE requests a second, and advances its cursor
on 2xx. Connection errors, timeouts, 429 and 5xx keep the cursor and retry
after 1 s doubling to 60 s, jittered. Any other 4xx means the batch itself is
wrong: it goes to outbox/rejected/, is counted, and the cursor moves on.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import random
import time
from collections import Counter

import aiohttp

from .clock import Clock
from .outbox import Outbox
from .stats import Hist

log = logging.getLogger(__name__)

IMPORT_PATH = "/api/v1/import/prometheus"
PUSH_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
LIVE_BATCH, CATCHUP_BATCH = 2_000, 20_000
RATE = 4.0  # requests per second at most
RETRY_MIN, RETRY_MAX = 1.0, 60.0
TIMEOUT = 10.0

OK, RETRY, REJECT = "ok", "retry", "reject"


class VmClient:
    def __init__(self, session: aiohttp.ClientSession, base_url: str, timeout: float = TIMEOUT, name: str = "VictoriaMetrics") -> None:
        self.name = name
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.url = self.base_url + IMPORT_PATH
        self.timeout = timeout
        self.requests: Counter[str] = Counter()  # by HTTP status or "error"
        self.duration = Hist(PUSH_BUCKETS)

    async def push(self, lines: list[str]) -> str:
        """POST one batch; return OK, RETRY or REJECT. Never raises."""
        body = gzip.compress("".join(lines).encode(), compresslevel=5)
        started = time.monotonic()
        try:
            async with self.session.post(
                self.url, data=body, headers={"Content-Encoding": "gzip", "Content-Type": "text/plain"},
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                status, reply = resp.status, await resp.read()  # bytes: a body need not be UTF-8
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
            self.requests["error"] += 1
            log.debug("%s: push failed: %s", self.name, type(err).__name__)
            return RETRY
        except asyncio.CancelledError:
            raise
        except Exception as err:  # anything else is a failed push too, never a lost batch
            self.requests["error"] += 1
            log.warning("%s: push failed unexpectedly: %s (%s)", self.name, err, type(err).__name__)
            return RETRY
        finally:
            self.duration.observe(time.monotonic() - started)
        self.requests[str(status)] += 1
        text = reply[:300].decode("utf-8", errors="replace").strip()
        if 200 <= status < 300:
            return OK
        if status == 429 or status >= 500:
            log.debug("%s: push got HTTP %d: %s", self.name, status, text)
            return RETRY
        log.error("%s refused a batch of %d samples: HTTP %d %s", self.name, len(lines), status, text)
        return REJECT


class Pusher:
    def __init__(self, name: str, client: VmClient, outbox: Outbox, *, interval: float = 5.0, clock: Clock | None = None,
                 retry_min: float = RETRY_MIN, retry_max: float = RETRY_MAX) -> None:
        self.name = name
        self.client = client
        self.outbox = outbox
        self.interval = interval
        self.clock = clock or Clock()
        self.retry_min, self.retry_max = retry_min, retry_max
        self.results: Counter[str] = Counter()  # samples by ok | rejected (dropped is the outbox's)
        self.failing_since: float | None = None  # monotonic
        self.last_run = self.clock.monotonic()
        self._retry = 0.0

    @property
    def up(self) -> bool:
        return self.failing_since is None

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.last_run = self.clock.monotonic()
            behind, _ = self.outbox.lag(self.name)
            await self.outbox.prefetch(self.name)
            lines, pos = self.outbox.read(self.name, CATCHUP_BATCH if behind > LIVE_BATCH else LIVE_BATCH)
            if not lines:
                await self._wait(stop, self.outbox.wake, self.interval)
                continue
            started = self.clock.monotonic()
            outcome = await self.client.push(lines)
            if outcome == RETRY:
                if self.failing_since is None:
                    self.failing_since = self.clock.monotonic()
                    log.warning("push to %s failing; keeping its samples in the outbox and retrying", self.name)
                self._retry = min(self.retry_max, max(self.retry_min, self._retry * 2))
                await self._wait(stop, None, self._retry * random.uniform(0.8, 1.2))
                continue
            if outcome == REJECT:
                self.outbox.reject(self.name, lines)
                self.results["rejected"] += len(lines)
            else:
                self.results["ok"] += len(lines)
                if self.failing_since is not None:
                    log.info("push to %s works again after %.0f s", self.name, self.clock.monotonic() - self.failing_since)
                self.failing_since, self._retry = None, 0.0
            self.outbox.commit(self.name, pos)
            if (rest := 1.0 / RATE - (self.clock.monotonic() - started)) > 0:
                await self._wait(stop, None, rest)

    async def _wait(self, stop: asyncio.Event, wake: asyncio.Event | None, seconds: float) -> None:
        waits = [asyncio.ensure_future(stop.wait()), asyncio.ensure_future(self.clock.sleep(seconds))]
        if wake is not None:
            waits.append(asyncio.ensure_future(wake.wait()))
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waits:
                w.cancel()
