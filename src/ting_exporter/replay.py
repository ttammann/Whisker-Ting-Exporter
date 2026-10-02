"""`replay` / `import`: feed record files through the serve pipeline (design 6.7, 7.7).

    ting-exporter import FILE|DIR ... --dry-run              1-min rollups as CSV
    ting-exporter import FILE|DIR ... --vm-url URL           push the samples into that store
    ting-exporter import ... --from 2026-04-01T10:00 --to 2026-04-01T11:00

Records are processed in time order with their recorded receive time as the
arrival time, and the recorded session events and restarts go in too, so the
samples, timestamps, storage decisions and inferred cuts are exactly what
`serve` pushed (or would have pushed) when they arrived. Pushing the same files
twice is harmless: identical (series, timestamp, value) samples are collapsed by
VictoriaMetrics' dedup. A sample that exists with a different value is only
replaced if the new value is larger; correcting values means deleting the
series first.

--dry-run prints, for every rollup rule in the signal registry, one CSV row per
series and minute: `window_end,serial,site,record,value`, windows (T-60 s, T]
stamped with their end T, the same convention as the vmalert rules. It is the
reference the golden test compares against tools/reference_rollups.py.
"""

from __future__ import annotations

import asyncio
import logging
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TextIO

import aiohttp

from . import recorder, signals
from .pipeline import Pipeline
from .pipeline.model import Sample
from .vmquery import VmError, VmQuery

log = logging.getLogger(__name__)

MINUTE_MS = 60_000
PUSH_BATCH = 20_000
MAX_ATTEMPTS = 8


def average(values: list[float], step: float) -> str:
    """`round(avg_over_time(x[1m]), step)`, as close to VictoriaMetrics as float64 allows.

    VM sums the samples in timestamp order in plain float64 and divides by the
    count; this does the same, then rounds with `v + step/2 - mod(v, step)`.
    The samples were rounded before the push, so a mean can sit exactly on a
    tie (121.86255 at 0.0001), and then float error decides the direction.
    Against VictoriaMetrics v1.152.0 nearly every average matches; an exact tie
    (a Hi-Fi mean such as 8.525 at 0.01) can differ by one unit in the last
    place. min, max and count match exactly.
    """
    total = 0.0
    for v in values:
        total += v
    mean = total / len(values) + 0.5 * step
    return f"{mean - math.fmod(mean, step):.{_decimals(step)}f}"


def _decimals(step: float) -> int:
    return max(0, -Decimal(repr(step)).normalize().as_tuple().exponent)


class Rollups:
    """In-memory 1-minute rollups over pushed samples (dry run)."""

    def __init__(self) -> None:
        self.by_metric = {s.metric: s for s in signals.REGISTRY if s.rollups}
        # (serial, site, metric) -> ts -> value: one sample per timestamp, the larger, as VictoriaMetrics' dedup keeps
        self.series: dict[tuple[str, str, str], dict[int, float]] = defaultdict(dict)

    def add(self, samples: list[Sample], sites: dict[str, str]) -> None:
        for s in samples:
            if s.metric in self.by_metric:
                series = self.series[(s.serial, sites.get(s.serial, "unknown"), s.metric)]
                series[s.ts_ms] = max(s.value, series.get(s.ts_ms, s.value))

    def rows(self) -> list[tuple[str, ...]]:
        out = []
        for (serial, site, metric), samples in self.series.items():
            signal = self.by_metric[metric]
            windows: dict[int, list[float]] = defaultdict(list)
            for ts in sorted(samples):
                windows[-(-ts // MINUTE_MS) * MINUTE_MS].append(samples[ts])
            for end, values in windows.items():
                when = datetime.fromtimestamp(end / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                for rollup in signal.rollups:
                    if rollup.agg == "count":
                        text = str(len(values))
                    elif rollup.agg == "avg":
                        text = average(values, rollup.round_to or 1)
                    else:
                        text = f"{(min if rollup.agg == 'min' else max)(values):.{signal.decimals}f}"
                    out.append((when, serial, site, signal.record_name(rollup), text))
        return sorted(out)


async def _push(client: VmQuery, lines: list[str]) -> None:
    delay = 1.0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            await client.import_lines(lines)
            return
        except VmError as err:
            if err.refused:
                raise RuntimeError(f"VictoriaMetrics refused a batch of {len(lines)} samples: {err}") from None
            log.warning("push failed (%s; attempt %d of %d); retrying in %.0f s", err, attempt, MAX_ATTEMPTS, delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)
    raise RuntimeError("VictoriaMetrics is not taking data; giving up")


async def replay(
    paths: list[Path],
    pipeline: Pipeline,
    *,
    vm_url: str | None = None,
    dry_run: bool = False,
    speed: float = 0.0,
    window: tuple[float | None, float | None] = (None, None),
    out: TextIO | None = None,
) -> dict[str, int]:
    """Run the files through `pipeline`; push to `vm_url` or print rollups. Returns totals."""
    rollups = Rollups() if dry_run else None
    totals = {"records": 0, "invocations": 0, "samples": 0, "pushed": 0}
    first_t: float | None = None
    started = time.monotonic()
    session = aiohttp.ClientSession() if vm_url else None
    client = VmQuery(session, vm_url) if session and vm_url else None
    batch: list[str] = []
    last_t: float | None = None
    try:
        for rec in recorder.read_records(paths, window):
            totals["records"] += 1
            t = float(rec["t"])
            kind = rec.get("kind")
            previous, last_t = last_t, t
            if kind == "start":  # the exporter restarted: as the live one, it restores what it knew
                pipeline.silences.restart(None if previous is None else max(0.0, t - previous))
                continue
            if kind == "event":
                fields = {k: v for k, v in rec.items() if k not in ("t", "serial", "kind", "event")}
                pipeline.event(str(rec.get("serial")), str(rec.get("event")), fields, t)
                continue
            if kind != "invocation":
                continue
            totals["invocations"] += 1
            if speed > 0:
                first_t = t if first_t is None else first_t
                if (ahead := (t - first_t) / speed - (time.monotonic() - started)) > 0:
                    await asyncio.sleep(ahead)
            samples, _ = pipeline.process(str(rec.get("serial")), str(rec.get("target")), rec.get("args") or [], t)
            totals["samples"] += len(samples)
            if rollups is not None:
                rollups.add(samples, pipeline.sites)
            if client is not None:
                batch.extend(s.line() for s in samples)
                if len(batch) >= PUSH_BATCH:
                    await _push(client, batch)
                    totals["pushed"] += len(batch)
                    batch = []
        if client is not None and batch:
            await _push(client, batch)
            totals["pushed"] += len(batch)
    finally:
        if session is not None:
            await session.close()
    if rollups is not None:
        out = out or sys.stdout
        for row in rollups.rows():
            out.write(",".join(row) + "\n")
    return totals


def summary(pipeline: Pipeline) -> str:
    """Per-sensor counters after a replay, for humans (stderr)."""
    lines = []
    for serial, st in sorted(pipeline.sensors.items()):
        t = st.timing
        found = pipeline.silences.sensors[serial].found if serial in pipeline.silences.sensors else {}
        lines.append(
            f"{serial} site={st.site} late={t.late} gap_slots={t.gap_slots} fallbacks={t.fallbacks} duplicates={st.duplicates}"
            f" silences={dict(found)}"
        )
        lines.append(f"  hub messages: {dict(sorted(st.hub_messages.items()))}")
        lines.append(f"  emitted:      {dict(sorted(st.emitted.items()))}")
        if st.discarded:
            lines.append(f"  discarded:    {dict(sorted(st.discarded.items()))}")
    return "\n".join(lines)
