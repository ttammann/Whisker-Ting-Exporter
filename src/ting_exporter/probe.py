"""The interactive commands: `probe` (stream, notifications, REST, voltage history) and `record`.

None of them releases a hub subscription unless TING_RELEASE_OTHERS=true says so (`probe --no-release` never
does), so they can run next to both exporters without ending their streams (design 2.3, 7.7).
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp

from . import __version__, rest
from .auth.identity import AuthUnavailable
from .clock import Clock
from .cloud import api
from .cloud.hub import HubSession, SessionState
from .config import Config
from .notifications import event_ms
from .pipeline import Pipeline
from .pipeline.decode import parse_time, to_ms
from .recorder import Recorder
from .serve import discover_once, identities_for, signal_stop

log = logging.getLogger(__name__)


def _site(cfg: Config, serial: str) -> str:
    return cfg.sites.get(serial, "unknown")


async def _stream_for(cfg: Config, seconds: float, on_invocation, on_event, report=None, clock: Clock | None = None,
                      release: bool = False) -> dict[str, SessionState]:
    clock = clock or Clock()
    stop = asyncio.Event()
    signal_stop(stop)
    async with aiohttp.ClientSession() as session:
        identities = identities_for(session, cfg, clock)
        try:
            identity = await identities.get()
        except AuthUnavailable as err:
            raise SystemExit(f"sign-in failed: {identities.last_error or err}") from None
        print(f"signed in: user_id={identity.user_id}", file=sys.stderr)
        try:
            devices = await discover_once(session, cfg, identities)
        except (api.ApiError, AuthUnavailable) as err:
            print(f"device list unavailable: {err}", file=sys.stderr)
            devices = []
        for d in devices:
            print(f"  sensor {d.serial} {d.name!r} {d.type} {d.firmware} site={_site(cfg, d.serial)}", file=sys.stderr)
        serials = list(cfg.streamed_serials) or [d.serial for d in devices]
        if not serials:
            raise SystemExit("no sensors to stream")
        states = {s: SessionState(s, _site(cfg, s)) for s in serials}
        tasks = [
            asyncio.create_task(HubSession(session, st, identities, on_invocation, on_event=on_event, stale=cfg.stale,
                                           hub_url=cfg.hub_url, clock=clock, release=release).run(stop))
            for st in states.values()
        ]
        started = clock.monotonic()
        while not stop.is_set() and clock.monotonic() - started < seconds:
            try:
                await asyncio.wait_for(stop.wait(), 1.0)
            except asyncio.TimeoutError:
                pass
            if report is not None:
                report(clock.monotonic() - started, states)
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        return states


async def probe(cfg: Config, seconds: float, release: bool | None = None) -> None:
    """Human-readable check: sign in, list sensors, stream, print decoded samples with delay and mode.

    Like serve, it releases no subscriptions unless TING_RELEASE_OTHERS (or release=True) says so,
    so it can run next to the exporters without ending their streams.
    """
    release = cfg.release_others if release is None else release
    pipeline = Pipeline(cfg.sites, cfg.timestamp_source)
    latest: dict[str, dict[str, str]] = {}
    targets: Counter[str] = Counter()

    def on_invocation(serial: str, target: str, args: list[Any], arrival: float) -> bool:
        if not targets[target]:
            print(f"  first {target!r} invocation, args[0] is {type(args[0]).__name__ if args else 'none'}", file=sys.stderr)
        targets[target] += 1
        samples, primary = pipeline.process(serial, target, args, arrival)
        for s in samples:
            latest.setdefault(serial, {})[s.metric.removeprefix("ting_")] = s.text
        return primary

    def on_event(serial: str, kind: str, fields: dict[str, Any], t: float) -> None:
        print(f"  {serial} {kind} {fields}", file=sys.stderr)

    def report(elapsed: float, states: dict[str, SessionState]) -> None:
        for serial, state in states.items():
            st = pipeline.sensor(serial)
            off = st.timing.last_offset
            mode = "-" if off is None else ("live" if off < 1.5 else "buffered")
            values = " ".join(f"{k}={v}" for k, v in sorted(latest.get(serial, {}).items()))
            delay = "-" if off is None else f"{off:.2f}s"
            print(f"{serial} t+{elapsed:4.0f}s {'up' if state.connected else 'down'} delay={delay} ({mode}) {values}")

    if release:
        print("releasing subscriptions: a running exporter's stream for these sensors ends", file=sys.stderr)
    await _stream_for(cfg, seconds, on_invocation, on_event, report, release=release)
    print(f"invocation targets seen: {dict(targets)}")
    for serial, st in sorted(pipeline.sensors.items()):
        print(f"{serial}: emitted {dict(st.emitted)} late={st.timing.late} fallbacks={st.timing.fallbacks} discarded={dict(st.discarded)}")


async def notifications(cfg: Config, raw: bool = False) -> None:
    """Read-only: the account's device fields and notification history (what the phone app alerts on)."""
    async with aiohttp.ClientSession() as session:
        identities = identities_for(session, cfg, Clock())
        try:
            identity = await identities.get()
        except AuthUnavailable as err:
            raise SystemExit(f"sign-in failed: {identities.last_error or err}") from None
        print(f"signed in: user_id={identity.user_id}", file=sys.stderr)

        try:
            user = await api.get_user(session, identity, cfg.api_url)
        except api.ApiError as err:
            print(f"device list unavailable: {err}", file=sys.stderr)
            user = {}
        interesting = ("hazard", "status", "connect", "online", "power", "outage", "alert", "condition")
        for dev in (user or {}).get("devices") or []:
            if not isinstance(dev, dict):
                continue
            serial = dev.get("serialNumber")
            print(f"device {serial} site={_site(cfg, str(serial))}: fields {sorted(dev)}")
            for key in sorted(dev):
                if any(w in key.lower() for w in interesting):
                    print(f"    {key} = {dev[key]!r}")

        try:
            history = await api.list_notifications(session, identity, cfg.api_url)
        except api.ApiError as err:
            raise SystemExit(f"notification history unavailable: {err}") from None
        fields = sorted({k for r in history for k in r})
        types = Counter(str(r.get("eventType")) for r in history)
        print(f"\n{len(history)} notifications; fields {fields}")
        print(f"by eventType: {dict(types.most_common())}\n")
        def when(r: dict[str, Any]) -> str:  # the record's own local time text, sorted by the real event time
            return str(r.get("eventTimestampLocal") or r.get("sentUtc") or "")

        for r in sorted(history, key=lambda r: event_ms(r) or 0, reverse=True):
            serial = str(r.get("serialNumber") or "")
            text = " / ".join(str(r[k]) for k in ("title", "subtitle", "message") if r.get(k))
            flags = "".join(f for f, k in (("A", "isAcknowledged"), ("C", "isCleared")) if r.get(k))
            print(f"{when(r)[:25]:25}  {str(r.get('eventType')):24} {str(r.get('eventCategory') or ''):14} "
                  f"{serial:11} site={_site(cfg, serial):8} {flags:2} {text[:110]}")
        if raw:
            print("\n" + json.dumps(history, indent=2, ensure_ascii=False, default=str))


async def voltage_history(cfg: Config, start: float, end: float, raw: bool = False) -> None:
    """Read-only: the cloud's own voltage history per sensor, in 24 h requests, summarised (--raw: full JSON)."""
    t0, t1 = datetime.fromtimestamp(start, timezone.utc), datetime.fromtimestamp(end, timezone.utc)
    if not t0 < t1 or t1 - t0 > timedelta(days=31):
        raise SystemExit("--from must be before --to, at most 31 days apart")
    async with aiohttp.ClientSession() as session:
        identities = identities_for(session, cfg, Clock())
        try:
            identity = await identities.get()
        except AuthUnavailable as err:
            raise SystemExit(f"sign-in failed: {identities.last_error or err}") from None
        serials = list(cfg.streamed_serials) or [d.serial for d in await discover_once(session, cfg, identities)]
        for serial in serials:
            print(f"\n== {serial} site={_site(cfg, serial)}  {t0:%Y-%m-%d %H:%M} .. {t1:%Y-%m-%d %H:%M} UTC ==")
            points: list[dict[str, Any]] = []
            units = set()
            a = t0
            while a < t1:
                b = min(a + timedelta(hours=24), t1)
                try:
                    data = await api.get_voltage_history(session, identity, serial, a, b, cfg.api_url)
                except api.ApiError as err:
                    print(f"  {a:%m-%d %H:%M}..{b:%m-%d %H:%M}: {err}")
                    a = b
                    continue
                if raw:
                    print(json.dumps(data, indent=2, default=str)[:20000])
                if isinstance(data, dict):
                    units.add(str(data.get("unit")))
                    data = next((v for v in data.values() if isinstance(v, list)), [])
                points += [p for p in data if isinstance(p, dict)] if isinstance(data, list) else []
                a = b
            print(f"  unit {sorted(units)}; {len(points)} points; fields {sorted({k for p in points for k in p})}")
            if not points:
                continue
            for p in points[:3] + (["..."] if len(points) > 5 else []) + points[-2:]:
                print(f"    {p}")
            times = []
            for p in points:
                when = next((parse_time(p.get(k)) for k in ("timestampUtc", "startUtc", "timestamp", "start") if p.get(k)), None)
                if when is not None:
                    times.append(to_ms(when))
            times.sort()
            steps = Counter(round((b - a) / 1000) for a, b in zip(times, times[1:]))
            print(f"  spacing between points (s): {dict(steps.most_common(6))}")
            typical = next((s for s, _ in steps.most_common() if s > 0), None)  # repeated timestamps are not a spacing
            gaps = [(a, b) for a, b in zip(times, times[1:]) if b - a > 2 * typical * 1000] if typical else []
            for a_ms, b_ms in gaps[:15]:
                print(f"  gap {datetime.fromtimestamp(a_ms / 1000, timezone.utc):%m-%d %H:%M:%S} .. "
                      f"{datetime.fromtimestamp(b_ms / 1000, timezone.utc):%H:%M:%S} UTC ({(b_ms - a_ms) / 60000:.1f} min)")


async def record(cfg: Config, seconds: float, out_dir: Path) -> None:
    """Stream for N seconds and write every raw hub message and event (same files as the flight recorder)."""
    recorder = Recorder(out_dir, retention_days=36500, keep_duplicates=True)  # everything, updateGraphMulti too
    recorder.start()
    recorder.meta("start", {"version": __version__, "host": socket.gethostname(), "command": "record", "seconds": seconds})
    counts: Counter[str] = Counter()
    last_progress = [0.0]

    def on_invocation(serial: str, target: str, args: list[Any], arrival: float) -> bool:
        counts[target] += 1
        recorder.invocation(serial, target, args, arrival)
        return target.casefold() == "updatecombobinarydata"

    def on_event(serial: str, kind: str, fields: dict[str, Any], t: float) -> None:
        recorder.event(serial, kind, fields, t)

    def report(elapsed: float, states: dict[str, SessionState]) -> None:
        recorder.flush()
        if elapsed - last_progress[0] >= 60:
            last_progress[0] = elapsed
            up = ",".join(f"{s}={'up' if st.connected else 'down'}" for s, st in sorted(states.items()))
            log.info("t+%.0fs %s lines=%d invocations=%s", elapsed, up, recorder.lines, dict(counts))

    try:
        await _stream_for(cfg, seconds, on_invocation, on_event, report, release=cfg.release_others)
    finally:
        recorder.meta("end", {"invocations": dict(counts)})
        await recorder.close()
        log.info("done: %d lines in %s", recorder.lines, out_dir)


async def rest_values(cfg: Config, raw: bool = False) -> None:
    """Read-only: what the REST registry (hazards, conditions, frozen pipe) gets from this account (design 0.5)."""
    async with aiohttp.ClientSession() as session:
        identities = identities_for(session, cfg, Clock())
        try:
            identity = await identities.get()
        except AuthUnavailable as err:
            raise SystemExit(f"sign-in failed: {identities.last_error or err}") from None
        print(f"signed in: user_id={identity.user_id}", file=sys.stderr)
        replies: dict[str, Any] = {}
        for key, call in ((rest.USER, api.get_user(session, identity, cfg.api_url)),
                          (rest.CONDITIONS, api.get_conditions(session, identity, cfg.api_url))):
            try:
                replies[key] = await call
                print(f"{rest.PATHS[key]}: ok")
            except api.ApiError as err:
                replies[key] = None
                print(f"{rest.PATHS[key]}: {err}")
        devices = (replies[rest.USER] or {}).get("devices") if isinstance(replies[rest.USER], dict) else None
        serials = list(cfg.streamed_serials) or [d["serialNumber"] for d in devices or [] if isinstance(d, dict)
                                                 and isinstance(d.get("serialNumber"), str)]
        frozen: dict[str, Any] = {}
        for serial in serials:
            try:
                frozen[serial] = await api.get_frozen_pipe(session, identity, serial, cfg.api_url)
                print(f"/api/v1/FrozenPipe/{serial}: ok")
            except api.ApiError as err:
                print(f"/api/v1/FrozenPipe/{serial}: {err}")
        views = rest.views(replies[rest.USER], replies[rest.CONDITIONS], frozen)
        for serial in serials:
            view = views.get(serial)
            print(f"\n{serial} site={_site(cfg, serial)}" + ("" if view else ": not in the device list"))
            for sig in rest.REST_REGISTRY:
                value = sig.extract(view) if view else None
                print(f"  {sig.metric:36} {'-' if value is None else value}")
        if raw:
            print("\ndevices:", json.dumps(devices, indent=2, ensure_ascii=False, default=str))
            print("conditions:", json.dumps(replies[rest.CONDITIONS], indent=2, ensure_ascii=False, default=str))
            print("frozen pipe:", json.dumps(frozen, indent=2, ensure_ascii=False, default=str))
