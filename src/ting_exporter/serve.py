"""`serve`: wire the parts together and run until SIGTERM (design 4).

    IdentityManager --identity--> HubSession[serial] --frames--> Pipeline --lines--> Outbox --cursor--> Pusher[target] --> VictoriaMetrics
                                       |   events -------------------^  (inferred cuts)  ^                   x M
                                       +--> Recorder (flight recorder)                   |
    notifications, REST values (hazards, weather), context marks (inbox) ----------------+

Active-active: two exporters (TING_ROLE primary and secondary, at different
sites) each stream every sensor and push to both stores (TING_VM_URLS: the
local one and the peer's over the tunnel). Device timestamps make their
samples identical, so VictoriaMetrics keeps one; when one exporter or its link
misses a stretch, the other's samples fill it. Neither releases a hub
subscription (TING_RELEASE_OTHERS, off): the hub would end the other's stream.

Tasks, each under the supervisor: a hub session per sensor, the outbox writer,
a pusher per target, notifications, REST values, discovery, the rollup repair,
the external health checks, the password-file watch, the state file, the
flight recorder, the heartbeat and the watchdog, plus a loop-guard thread.

The watchdog: liveness (/healthz) failing WATCHDOG_STRIKES checks in a row
(2 minutes) stops the exporter with exit status 1, and the loop guard exits at
once if the event loop has not run for 2 minutes. Docker's restart policy then
starts a fresh process; a failing healthcheck alone only marks the container
unhealthy.

Shutdown (SIGTERM/SIGINT): sessions close (3 s, releasing only if
configured), the other tasks get 3 s and are then cancelled (a pusher stops
after its request in flight, or is cancelled: the outbox keeps the batch), the
outbox appends and fsyncs what is pending, the state file is written, the
recorder closes its file, exit 0. Well inside Docker's 20 s grace period.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import threading
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TypeVar

import aiohttp
from aiohttp import web

from . import __version__, expo, logs, rest, selfmetrics
from . import notifications as history
from .auth.identity import AuthUnavailable, IdentityManager, SecretFile
from .clock import Clock
from .cloud import api
from .cloud.hub import HubSession, SessionState
from .config import Config
from .health import HealthWatch
from .outbox import Outbox
from .pipeline import Pipeline
from .pipeline.model import Sample
from .pusher import Pusher, VmClient
from .recorder import Recorder
from .repair import RollupRepair
from .supervisor import supervise

log = logging.getLogger(__name__)

DISCOVERY_INTERVAL = 3600.0
DISCOVERY_RETRY_MIN, DISCOVERY_RETRY_MAX = 30.0, 3600.0
FORBIDDEN_RETRY = 3600.0  # an endpoint the account may not use is asked again hourly
STATE_INTERVAL = 10.0
SESSION_STOP_TIMEOUT = 3.0
TASK_STOP_TIMEOUT = 3.0
WATCHDOG_INTERVAL = 30.0
WATCHDOG_STRIKES = 4
LOOP_STUCK_AFTER = 120.0  # real seconds without a heartbeat: the event loop itself is stuck
AUTH_WAIT_MAX = 300.0

R = TypeVar("R")


@dataclass
class App:
    cfg: Config
    clock: Clock
    identities: IdentityManager
    pipeline: Pipeline
    outbox: Outbox
    pushers: dict[str, Pusher]
    recorder: Recorder | None
    sessions: dict[str, SessionState] = field(default_factory=dict)
    devices: dict[str, api.Device] = field(default_factory=dict)
    restarts: Counter[str] = field(default_factory=Counter)
    task_names: set[str] = field(default_factory=set)
    tasks: dict[str, asyncio.Task] = field(default_factory=dict)  # watched by liveness
    heartbeat: float = 0.0
    heartbeat_real: float = field(default_factory=time.monotonic)  # wall-clock seconds, for the loop guard thread
    writer_last: float = 0.0
    unhealthy: str | None = None  # why the watchdog stopped the exporter
    notes: history.Tracker | None = None
    notes_polls: Counter[str] = field(default_factory=Counter)
    notes_last_ok: float | None = None
    rest_polls: Counter[tuple[str, str]] = field(default_factory=Counter)
    rest_missing: dict[str, int] = field(default_factory=dict)
    repair: RollupRepair | None = None
    health: HealthWatch | None = None

    def put(self, samples: list[Sample]) -> None:
        """Every sample leaves through the outbox, formatted once, for every target."""
        if samples:
            self.outbox.append([s.line() for s in samples])


def identities_for(session: aiohttp.ClientSession, cfg: Config, clock: Clock) -> IdentityManager:
    return IdentityManager(session, cfg.username, SecretFile(cfg.password_file), cognito_url=cfg.cognito_url,
                           hold_seconds=cfg.auth_hold_seconds, clock=clock, on_secret=logs.register_secret,
                           on_retire=logs.retire_secret)


def signal_stop(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # not the main thread (tests)
            pass


async def with_renewal(identities: IdentityManager, call: Callable[[Any], Awaitable[R]]) -> R:
    """`await call(identity)`, renewing the identity once on a 401. A 403 is never renewed: the account may not
    use the resource, and renewing would spend the sign-in budget. Raises AuthUnavailable or ApiError."""
    identity = await identities.get()
    try:
        return await call(identity)
    except api.ApiError as err:
        if not err.unauthorized:
            raise
        return await call(await identities.get(stale=identity))


async def sleep(stop: asyncio.Event, clock: Clock, seconds: float, identities: IdentityManager | None = None) -> None:
    """Sleep `seconds` of policy time, or until stop (or, with identities, until the auth state changes)."""
    waits = [asyncio.ensure_future(stop.wait()),
             asyncio.ensure_future(identities.wait(seconds) if identities else clock.sleep(seconds))]
    try:
        await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waits:
            w.cancel()


# ---- the state file (design 5.4: a restart during a cut still infers it) ---------------------------------


def load_state(app: App) -> None:
    path = app.cfg.state_file
    if path is None:
        return
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        saved_at = float(state["saved_at"])
        silences = state["silences"]
    except FileNotFoundError:
        return
    except (OSError, ValueError, KeyError, TypeError) as err:
        log.warning("state file %s unusable (%s); starting without it", path, type(err).__name__)
        return
    down = max(0.0, app.clock.time() - saved_at)
    app.pipeline.silences.load(silences, down)
    log.info("state file: last samples of %d sensors, saved %.0f s ago", len(silences), down)


def save_state(app: App) -> None:
    path = app.cfg.state_file
    if path is None:
        return
    body = {"version": 1, "saved_at": app.clock.time(), "silences": app.pipeline.silences.state()}
    try:
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(body), encoding="utf-8")
        os.replace(tmp, path)
    except OSError as err:
        log.debug("state file %s: %s", path, err)


async def _state_loop(app: App, stop: asyncio.Event) -> None:
    while not stop.is_set():
        save_state(app)
        await sleep(stop, app.clock, STATE_INTERVAL)


# ---- cloud loops -------------------------------------------------------------------------------------


async def discover_once(session: aiohttp.ClientSession, cfg: Config, identities: IdentityManager) -> list[api.Device]:
    return await with_renewal(identities, lambda identity: api.list_devices(session, identity, cfg.api_url))


async def _discovery_loop(app: App, session: aiohttp.ClientSession, stop: asyncio.Event, found: asyncio.Event,
                          start_session: Callable[[str], None]) -> None:
    retry = DISCOVERY_RETRY_MIN
    while not stop.is_set():
        try:
            devices = await discover_once(session, app.cfg, app.identities)
        except AuthUnavailable as err:
            await sleep(stop, app.clock, min(err.retry_in, AUTH_WAIT_MAX), app.identities)
            continue
        except api.ApiError as err:
            log.warning("device discovery failed (%s); retrying in %.0f s", err, retry)
            await sleep(stop, app.clock, retry)
            retry = min(retry * 2, DISCOVERY_RETRY_MAX)
            continue
        retry = DISCOVERY_RETRY_MIN
        new = {d.serial: d for d in devices}
        for serial in sorted(new.keys() - app.devices.keys()):
            d = new[serial]
            if not app.cfg.streamed_serials:  # nothing configured: stream everything on the account
                start_session(serial)
            streamed = "streaming" if serial in app.sessions else "not configured, not streamed"
            log.info("found %s %r (%s, firmware %s), site=%s, %s", serial, d.name, d.type, d.firmware,
                     app.pipeline.site(serial), streamed)
        for serial in sorted(app.devices.keys() - new.keys()):
            log.warning("%s is no longer on the account", serial)
        if not app.devices:
            for serial in app.cfg.streamed_serials:
                if serial not in new:
                    log.warning("%s is configured but not on this account; streaming it anyway", serial)
        app.devices = new
        found.set()
        await sleep(stop, app.clock, DISCOVERY_INTERVAL)


async def _notification_loop(app: App, session: aiohttp.ClientSession, stop: asyncio.Event) -> None:
    """Poll the account's notification history; push what is new; record new raw records (design 5.3)."""
    assert app.notes is not None
    loaded = False  # the first successful poll loads the whole history and is summarised, not listed
    forbidden_logged = False
    while not stop.is_set():
        delay = app.cfg.notifications_interval
        try:
            records = await with_renewal(app.identities, lambda i: api.list_notifications(session, i, app.cfg.api_url))
        except AuthUnavailable as err:
            await sleep(stop, app.clock, min(err.retry_in, AUTH_WAIT_MAX), app.identities)
            continue
        except api.ApiError as err:
            app.notes_polls["error"] += 1
            if err.forbidden:
                delay = max(delay, FORBIDDEN_RETRY)
                if not forbidden_logged:
                    forbidden_logged = True
                    log.warning("notification history not allowed for this account (%s); asking again hourly", err)
            else:
                log.warning("notification history unavailable (%s); next try in %.0f s", err, delay)
        else:
            forbidden_logged = False
            app.notes_polls["ok"] += 1
            app.notes_last_ok = app.clock.time()
            samples, fresh = app.notes.update(records, int(app.clock.time() * 1000))
            app.put(samples)
            for note, record in fresh:
                if app.recorder is not None:
                    app.recorder.event(note.serial, "notification", {"record": record})
                if loaded:
                    when = datetime.fromtimestamp(note.ts_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    log.info("Ting notification: %s at %s for %s (site %s)", note.type, when, note.serial,
                             app.pipeline.site(note.serial))
            if not loaded:
                loaded = True
                log.info("notification history: %d records, %d samples", len(app.notes.notes), len(samples))
        await sleep(stop, app.clock, delay)


async def _rest_loop(app: App, session: aiohttp.ClientSession, stop: asyncio.Event) -> None:
    """Poll hazards, conditions and frozen-pipe records; push one sample per value and poll (rest.py)."""
    cfg = app.cfg
    closed: dict[str, float] = {}  # endpoint key -> monotonic time to ask again (403/404)

    async def fetch(key: str, endpoint: str, call: Callable[[Any], Awaitable[Any]]) -> Any:
        if app.clock.monotonic() < closed.get(key, 0.0):
            return None
        try:
            data = await with_renewal(app.identities, call)
        except api.ApiError as err:
            app.rest_polls[(endpoint, "unavailable" if err.unavailable else "error")] += 1
            if err.unavailable:
                if key not in closed:
                    log.info("REST %s not available for this account (%s); asking again hourly", key, err)
                closed[key] = app.clock.monotonic() + FORBIDDEN_RETRY
            else:
                log.warning("REST %s failed (%s)", key, err)
            return None
        app.rest_polls[(endpoint, "ok")] += 1
        closed.pop(key, None)
        return data

    while not stop.is_set():
        now = app.clock.time()
        try:
            user = await fetch(rest.USER, rest.USER, lambda i: api.get_user(session, i, cfg.api_url))
            conditions = await fetch(rest.CONDITIONS, rest.CONDITIONS, lambda i: api.get_conditions(session, i, cfg.api_url))
            serials = sorted(app.sessions) or list(cfg.streamed_serials)
            frozen: dict[str, Any] = {}
            for serial in serials:
                record = await fetch(f"{rest.FROZEN_PIPE}:{serial}", rest.FROZEN_PIPE,
                                     lambda i, s=serial: api.get_frozen_pipe(session, i, s, cfg.api_url))
                if record is not None:
                    frozen[serial] = record
        except AuthUnavailable as err:
            await sleep(stop, app.clock, min(err.retry_in, AUTH_WAIT_MAX), app.identities)
            continue
        got = {rest.USER: user is not None, rest.CONDITIONS: conditions is not None, rest.FROZEN_PIPE: bool(frozen)}
        views = rest.views(user, conditions, frozen)
        ts_ms = int(now // 60 * 60_000)
        samples, missing = [], Counter()
        for serial in serials:
            view = views.get(serial)
            labels = app.pipeline.sensor(serial).labels
            for sig in rest.REST_REGISTRY:
                if not got[sig.endpoint] or view is None:
                    continue
                value = sig.extract(view)
                if value is None:
                    missing[sig.key] += 1
                    continue
                samples.append(Sample(sig.metric, serial, labels, ts_ms, value, expo.format_number(value)))
        app.rest_missing = {s.key: missing[s.key] for s in rest.REST_REGISTRY}
        app.put(samples)
        await sleep(stop, app.clock, cfg.rest_interval)


# ---- liveness --------------------------------------------------------------------------------------


async def _writer_loop(app: App, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await app.outbox.flush()
        app.writer_last = app.clock.monotonic()
        await sleep(stop, app.clock, app.cfg.push_interval)


async def _heartbeat(app: App, stop: asyncio.Event) -> None:
    while not stop.is_set():
        app.heartbeat = app.clock.monotonic()
        app.heartbeat_real = time.monotonic()
        await sleep(stop, app.clock, 1.0)


async def _watchdog(app: App, stop: asyncio.Event) -> None:
    """After WATCHDOG_STRIKES failed liveness checks in a row, stop with exit status 1 so the container restarts."""
    strikes = 0
    while not stop.is_set():
        await sleep(stop, app.clock, WATCHDOG_INTERVAL)
        if stop.is_set():
            return
        ok, why = selfmetrics.liveness(app)
        strikes = 0 if ok else strikes + 1
        if not ok:
            log.warning("liveness check failed (%d of %d): %s", strikes, WATCHDOG_STRIKES, why)
        if strikes >= WATCHDOG_STRIKES:
            app.unhealthy = why
            log.error("unhealthy for %d checks in a row (%s); stopping so the container restarts", strikes, why)
            stop.set()
            return


def _loop_guard(app: App, done: threading.Event, poll: float = LOOP_STUCK_AFTER / 4, limit: float = LOOP_STUCK_AFTER) -> None:
    """A thread: when the event loop stops running (a blocking call, a deadlock), nothing on it can shut down, so
    exit at once and let the restart policy start a fresh process. Wall-clock seconds, not policy time."""
    while not done.wait(poll):
        stuck = time.monotonic() - app.heartbeat_real
        if stuck > limit:
            log.critical("the event loop has not run for %.0f s; exiting so the container restarts", stuck)
            os._exit(1)
            return


# ---- serve -----------------------------------------------------------------------------------------


def make_outbox(cfg: Config, clock: Clock) -> Outbox:
    outbox = Outbox(cfg.outbox_dir, [n for n, _ in cfg.vm_targets], cfg.outbox_max_bytes,
                    replay_new=cfg.outbox_replay_new, wall=clock.time)
    try:
        outbox.open()
    except OSError as err:
        log.error("outbox %s unusable (%s); keeping samples in memory and trying the disk every minute", cfg.outbox_dir, err)
        outbox.open_in_memory()
    return outbox


async def serve(cfg: Config, stop: asyncio.Event | None = None, clock: Clock | None = None) -> int:
    """Run until `stop` (SIGTERM/SIGINT). Returns the exit status: 1 if the watchdog stopped it."""
    clock = clock or Clock()
    if stop is None:
        stop = asyncio.Event()
        signal_stop(stop)
    session = aiohttp.ClientSession()
    identities = identities_for(session, cfg, clock)
    pipeline = Pipeline(cfg.sites, cfg.timestamp_source)
    outbox = make_outbox(cfg, clock)
    pushers = {name: Pusher(name, VmClient(session, url, name=f"VictoriaMetrics {name}"), outbox,
                            interval=cfg.push_interval, clock=clock) for name, url in cfg.vm_targets}
    recorder = None
    if cfg.record_dir is not None:
        recorder = Recorder(cfg.record_dir, cfg.record_retention_days, clock.time)
        recorder.start()
        recorder.meta("start", {"version": __version__, "host": socket.gethostname(), "command": "serve", "role": cfg.role,
                                "targets": [n for n, _ in cfg.vm_targets], "serials": list(cfg.streamed_serials),
                                "sites": cfg.sites})
    app = App(cfg, clock, identities, pipeline, outbox, pushers, recorder, heartbeat=clock.monotonic(),
              writer_last=clock.monotonic())
    load_state(app)

    def on_invocation(serial: str, target: str, args: list[Any], arrival: float) -> bool:
        if recorder is not None:
            recorder.invocation(serial, target, args, arrival)
        samples, primary = pipeline.process(serial, target, args, arrival)
        app.put(samples)
        return primary

    def on_event(serial: str, kind: str, fields: dict[str, Any], t: float) -> None:
        if recorder is not None:
            recorder.event(serial, kind, fields, t)
        pipeline.event(serial, kind, fields, t)

    runner = web.AppRunner(selfmetrics.make_app(app), access_log=None)
    await runner.setup()
    await web.TCPSite(runner, cfg.listen_host, cfg.listen_port).start()
    log.info("ting-exporter %s (%s on %s): /metrics on %s:%d, pushing to %s, timestamps from %s%s", __version__, cfg.role,
             cfg.host, cfg.listen_host, cfg.listen_port, ", ".join(f"{n} {u}" for n, u in cfg.vm_targets),
             cfg.timestamp_source, ", releasing other subscriptions" if cfg.release_others else "")

    background: list[asyncio.Task] = []
    session_tasks: list[asyncio.Task] = []

    def start(name: str, factory: Callable[[], Awaitable[None]], watched: bool = True) -> None:
        task = asyncio.create_task(supervise(name, factory, stop, app.restarts, clock), name=name)
        background.append(task)
        app.task_names.add(name)
        if watched:
            app.tasks[name] = task

    def start_session(serial: str) -> None:
        if serial in app.sessions:
            return
        state = app.sessions[serial] = SessionState(serial, pipeline.site(serial))
        pipeline.sensor(serial)
        hub = HubSession(session, state, identities, on_invocation, on_event=on_event, stale=cfg.stale, hub_url=cfg.hub_url,
                         clock=clock, release=cfg.release_others)
        name = f"hub_session:{serial}"
        app.task_names.add(name)
        session_tasks.append(asyncio.create_task(supervise(name, lambda: hub.run(stop), stop, app.restarts, clock)))

    start("outbox_writer", lambda: _writer_loop(app, stop))
    for name, pusher in pushers.items():
        start(f"pusher:{name}", lambda pusher=pusher: pusher.run(stop))
    start("secret_watch", lambda: identities.watch(stop))
    start("heartbeat", lambda: _heartbeat(app, stop))
    start("watchdog", lambda: _watchdog(app, stop), watched=False)
    start("state", lambda: _state_loop(app, stop))
    guard_done = threading.Event()
    threading.Thread(target=_loop_guard, args=(app, guard_done), name="loop-guard", daemon=True).start()
    if recorder is not None:
        start("recorder", lambda: recorder.run(stop))
    found = asyncio.Event()
    start("discovery", lambda: _discovery_loop(app, session, stop, found, start_session))
    if cfg.notifications_interval > 0:
        app.notes = history.Tracker(cfg.sites)
        start("notifications", lambda: _notification_loop(app, session, stop))
    if cfg.rest_interval > 0:
        start("rest", lambda: _rest_loop(app, session, stop))
    if cfg.repair_interval > 0:
        app.repair = RollupRepair(session, dict(cfg.vm_targets), lambda name: outbox.lag(name)[1],
                                  interval=cfg.repair_interval, clock=clock)
        start("rollup_repair", lambda: app.repair.run(stop))
    if cfg.health_checks:
        app.health = HealthWatch(session, list(cfg.health_checks), cfg.alert_webhook_file, host=cfg.host, role=cfg.role,
                                 for_seconds=cfg.health_for, clock=clock)
        start("health", lambda: app.health.run(stop))

    for serial in cfg.streamed_serials:
        start_session(serial)
    try:
        if not cfg.streamed_serials:  # stream whatever the account has, once we know
            waiter, stopper = asyncio.ensure_future(found.wait()), asyncio.ensure_future(stop.wait())
            await asyncio.wait({waiter, stopper}, return_when=asyncio.FIRST_COMPLETED)
            waiter.cancel()
            stopper.cancel()
            if not stop.is_set():
                if not app.devices:
                    log.error("no Ting sensors on this account; set TING_SITES or TING_SERIALS")
                for serial in sorted(app.devices):
                    start_session(serial)
        await stop.wait()
    finally:
        guard_done.set()
        log.info("shutting down")
        stop.set()
        await _finish(session_tasks, SESSION_STOP_TIMEOUT)
        await _finish(background, TASK_STOP_TIMEOUT)
        await outbox.close()
        save_state(app)
        results = {name: dict(p.results) for name, p in pushers.items()}
        if recorder is not None:
            recorder.meta("end", {"results": results})
            await recorder.close()
        await runner.cleanup()
        await session.close()
        log.info("stopped: pushed %s; behind: %s", results, {n: outbox.lag(n)[0] for n in pushers})
    return 1 if app.unhealthy else 0


async def _finish(tasks: list[asyncio.Task], timeout: float) -> None:
    if not tasks:
        return
    _, pending = await asyncio.wait(tasks, timeout=timeout)
    for task in pending:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
