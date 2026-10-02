"""Rollup repair: fill the 1-minute rollups vmalert missed (design 6.8).

vmalert never evaluates a minute twice. A minute's rollups are missing when
VictoriaMetrics was down at evaluation time, and when its raw data arrived
late, e.g. a peer's backlog drained after the inter-site link was down (the
hole is then on the receiving store).

Every REPAIR_INTERVAL, for each push target (store):
  1. skip it while this exporter still has an outbox backlog for it;
  2. find the minutes T in the last LOOKBACK (and older than SETTLE) whose raw
     voltage count is > 0 but which have no ting:voltage_volts:count_1m point;
  3. evaluate every rollup rule of the registry (rules.expression) over those
     minutes, narrowed to the series that misses them, and import the values
     stamped T: exactly what vmalert would have written.

Fill only, never overwrite (VictoriaMetrics keeps the larger value on equal
timestamps, which would be wrong for min). A minute whose rollup was computed
from partial data (its count_1m is lower than the raw count) cannot be fixed
in place: it is counted in `stale`. Both exporters may repair the same minute;
they compute the same values from the same data.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone

import aiohttp

from . import rules, signals
from .clock import Clock
from .pipeline.model import label_text
from .vmquery import VmError, VmQuery

log = logging.getLogger(__name__)

MINUTE = 60
LOOKBACK = 48 * 3600
SETTLE = 600  # vmalert writes a minute about 82 s after it ends; give it 10 minutes
BACKLOG_SECONDS = 30.0  # an outbox lag above this: raw data for that store is still on its way
COUNT_RULE = signals.PRIMARY.record_name(next(r for r in signals.PRIMARY.rollups if r.agg == "count"))
MAX_MINUTES_PER_RUN = 6 * 60  # per target and series; the rest waits for the next run


def runs(minutes: list[int]) -> list[tuple[int, int]]:
    """Contiguous runs of minute timestamps (seconds), as (first, last)."""
    out: list[tuple[int, int]] = []
    for t in sorted(minutes):
        if out and t == out[-1][1] + MINUTE:
            out[-1] = (out[-1][0], t)
        else:
            out.append((t, t))
    return out


class RollupRepair:
    def __init__(self, session: aiohttp.ClientSession, targets: dict[str, str], lag: Callable[[str], float],
                 *, interval: float = 300.0, clock: Clock | None = None) -> None:
        self.session = session
        self.targets = targets  # name -> base URL
        self.lag = lag  # target -> seconds of outbox backlog
        self.interval = interval
        self.clock = clock or Clock()
        self.repaired: Counter[str] = Counter()  # minutes, by target
        self.stale: dict[str, int] = {}  # minutes with a rollup from partial data, by target (last run)
        self.runs: Counter[tuple[str, str]] = Counter()  # (target, ok | skipped | error)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await _sleep(stop, self.clock, self.interval)
            for name, url in self.targets.items():
                if stop.is_set():
                    return
                if self.lag(name) > BACKLOG_SECONDS:
                    self.runs[(name, "skipped")] += 1
                    continue
                try:
                    await self.repair(name, url, self.clock.time())
                    self.runs[(name, "ok")] += 1
                except VmError as err:
                    self.runs[(name, "error")] += 1
                    log.warning("rollup repair on %s: %s", name, err)

    async def repair(self, name: str, url: str, now: float, *, start: float | None = None, end: float | None = None,
                     cap: int | None = MAX_MINUTES_PER_RUN) -> int:
        """Fill the missing minutes of one store; returns how many minutes (per series) were filled.

        By default the last LOOKBACK up to SETTLE ago, at most `cap` minutes per series (the periodic task).
        `ting-exporter repair` passes a window (minutes T with start < T <= end) and no cap, for imported history.
        """
        vm = VmQuery(self.session, url)
        end = int(min(end, now - SETTLE) if end is not None else now - SETTLE) // MINUTE * MINUTE
        start = int(start) // MINUTE * MINUTE + MINUTE if start is not None else end - LOOKBACK + MINUTE
        if start > end:
            return 0
        metric = signals.PRIMARY.metric
        raw = await vm.query_range(f"count_over_time({metric}[1m])", start, end, MINUTE)
        done = await vm.query_range(f"last_over_time({COUNT_RULE}[1m])", start, end, MINUTE)
        rolled: dict[tuple[str, str], dict[int, float]] = {}
        for labels, values in done:
            rolled[_key(labels)] = {int(t): float(v) for t, v in values}
        missing: dict[tuple[str, str], list[int]] = {}
        stale = 0
        for labels, values in raw:
            key = _key(labels)
            have = rolled.get(key, {})
            for t, v in values:
                count = float(v)
                if count <= 0:
                    continue
                if int(t) not in have:
                    missing.setdefault(key, []).append(int(t))
                elif have[int(t)] < count:
                    stale += 1
        self.stale[name] = stale
        filled = 0
        for (serial, site), minutes in sorted(missing.items()):
            minutes = sorted(minutes)[-cap:] if cap else sorted(minutes)
            lines = await self._evaluate(vm, serial, site, minutes)
            if lines:
                await vm.import_lines(lines)
                filled += len(minutes)
                log.info("rollup repair on %s: filled %d minutes of %s (site %s)", name, len(minutes), serial, site)
        self.repaired[name] += filled
        return filled

    async def _evaluate(self, vm: VmQuery, serial: str, site: str, minutes: list[int]) -> list[str]:
        wanted = set(minutes)
        labels = label_text({"serial": serial, "site": site})
        selector = f'serial="{serial}",site="{site}"'
        lines: list[str] = []
        for first, last in runs(minutes):
            for signal, rollup in rules.every_rule():
                result = await vm.query_range(rules.expression(signal, rollup, selector), first, last, MINUTE, nocache=True)
                for _labels, values in result:
                    for t, v in values:
                        if int(t) in wanted:
                            lines.append(f"{signal.record_name(rollup)}{{{labels}}} {v} {int(t) * 1000}\n")
        return lines


def _key(labels: dict[str, str]) -> tuple[str, str]:
    return labels.get("serial", ""), labels.get("site", "")


async def _sleep(stop: asyncio.Event, clock: Clock, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), clock.real(seconds))
    except asyncio.TimeoutError:
        pass


DAY = 86_400


async def repair_range(session: aiohttp.ClientSession, targets: dict[str, str], start: float, end: float, now: float,
                       report: Callable[[str, str, int, int], None]) -> int:
    """`ting-exporter repair`: fill every missing minute in (start, end] of each store, a day per pass.
    `report(target, day, filled, stale)` after each pass. Raises VmError."""
    repair = RollupRepair(session, targets, lambda _name: 0.0)
    total = 0
    for name, url in targets.items():
        t = start
        while t < end:
            stop = min(t + DAY, end)
            filled = await repair.repair(name, url, now, start=t, end=stop, cap=None)
            total += filled
            report(name, f"{_day(t)}..{_day(stop)}", filled, repair.stale.get(name, 0))
            t = stop
    return total


def _day(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M")
