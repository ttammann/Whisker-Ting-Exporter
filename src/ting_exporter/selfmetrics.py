"""The exporter's own metrics and health endpoints, on TING_LISTEN (design 5.6, 7.4).

    /metrics   Prometheus text; VictoriaMetrics scrapes it every 30 s
    /healthz   liveness for Docker and the watchdog: the event loop runs, the outbox writer and every pusher run
    /readyz    readiness for humans and `probe`: JSON per sensor and target, 503 unless every sensor is up

A cloud or VictoriaMetrics outage never fails /healthz: a restart would only
cost a sign-in. Everything is read from the live objects at scrape time.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from aiohttp import web

from . import __version__, expo
from .expo import counter, gauge, histogram

if TYPE_CHECKING:
    from .serve import App

LIVENESS_HEARTBEAT = 5.0  # the loop heartbeat must have ticked this recently
LIVENESS_RUN = 30.0  # the writer and each pusher must have run this recently, or within 3 push intervals
S = ("serial", "site")
T = ("target",)


def families(app: App) -> list[expo.Family]:
    now = app.clock.monotonic()
    cfg, pipeline = app.cfg, app.pipeline
    out: list[expo.Family] = []

    out.append(gauge("ting_exporter_build_info", "Exporter version, role and host.", ("version", "role", "host"))
               .add((__version__, cfg.role, cfg.host), 1))
    info = gauge("ting_device_info", "Ting sensor metadata from the account (refreshed hourly).",
                 ("serial", "site", "name", "type", "firmware"))
    connected = gauge("ting_stream_connected", "1 while the WebSocket is open and ComboBinaryData is subscribed.", S)
    up = gauge("ting_stream_up", "1 while voltage samples arrive within the stale limit.", S)
    connects = counter("ting_stream_connects_total", "Hub connection attempts by result.", (*S, "result"))
    disconnects = counter("ting_stream_disconnects_total", "Hub session ends by reason.", (*S, "reason"))
    errors = counter("ting_stream_errors_total", "Unexpected errors and undecodable frames in the hub session.", (*S, "kind"))
    last_sample = gauge("ting_last_sample_timestamp_seconds", "Device time of the newest voltage sample.", S)
    last_receive = gauge("ting_last_receive_timestamp_seconds", "Host time the newest voltage sample arrived.", S)
    offset = gauge("ting_clock_offset_seconds", "Arrival minus device time of the newest voltage sample "
                   "(live path ~0.55 s, buffered 4-8 s).", S)
    delay = histogram("ting_delivery_delay_seconds", "Arrival minus device time per voltage sample.", S)
    received = counter("ting_samples_received_total", "Readings decoded, by metric.", (*S, "metric"))
    emitted = counter("ting_samples_emitted_total", "Samples written to the outbox (after storage rule and dedup).",
                      (*S, "metric"))
    discarded = counter("ting_samples_discarded_total", "Payloads or records without a usable reading, by reason.",
                        (*S, "reason"))
    duplicates = counter("ting_samples_duplicate_total", "Exact repeats of (serial, metric, timestamp) dropped.", S)
    late = counter("ting_samples_late_total", "Voltage samples older than the newest seen (interleaved delivery).", S)
    gaps = counter("ting_device_gap_slots_total", "Upper bound on missing 0.25 s device-time slots.", S)
    fallback = counter("ting_timestamp_fallback_total", "Readings whose device time failed the arrival guard.", S)
    hub_messages = counter("ting_hub_messages_total", "Hub invocations by target (updateGraphMulti is expected).",
                           (*S, "target"))
    silences = counter("ting_inferred_silences_total", "Silences of 60 s or more: inferred power cuts, data gaps, "
                       "and those this exporter did not fully watch.", (*S, "kind"))
    for serial in sorted(set(app.sessions) | set(pipeline.sensors)):
        st = pipeline.sensor(serial)
        lv = (serial, st.site)
        if (dev := app.devices.get(serial)) is not None:
            info.add((*lv, dev.name, dev.type, dev.firmware), 1)
        if (state := app.sessions.get(serial)) is not None:
            fresh = state.last_primary is not None and now - state.last_primary <= cfg.stale
            connected.add(lv, int(state.connected))
            up.add(lv, int(state.connected and fresh))
            for result in ("ok", "refused", "error"):
                connects.add((*lv, result), state.connects[result])
            for reason, n in sorted(state.disconnects.items()):
                disconnects.add((*lv, reason), n)
            for kind, n in sorted(state.errors.items()):
                errors.add((*lv, kind), n)
        t = st.timing
        if t.max_device_ms is not None:
            last_sample.add(lv, t.max_device_ms / 1000)
        if t.last_receive is not None:
            last_receive.add(lv, t.last_receive)
        if t.last_offset is not None:
            offset.add(lv, t.last_offset)
        delay.histogram(lv, t.delay.cumulative(), t.delay.sum)
        for metric, n in sorted(st.received.items()):
            received.add((*lv, metric), n)
        for metric, n in sorted(st.emitted.items()):
            emitted.add((*lv, metric), n)
        for reason, n in sorted(st.discarded.items()):
            discarded.add((*lv, reason), n)
        for target, n in sorted(st.hub_messages.items()):
            hub_messages.add((*lv, target[:64]), n)
        duplicates.add(lv, st.duplicates)
        late.add(lv, t.late)
        gaps.add(lv, t.gap_slots)
        fallback.add(lv, t.fallbacks)
        tracker = pipeline.silences.sensors.get(serial)
        for kind in ("cut", "gap", "unattributed"):
            silences.add((*lv, kind), tracker.found[kind] if tracker else 0)
    out += [info, connected, up, connects, disconnects, errors, last_sample, last_receive, offset, delay, received,
            emitted, discarded, duplicates, late, gaps, fallback, hub_messages, silences]

    push = counter("ting_push_samples_total", "Push outcomes in samples (dropped: lost from the outbox for this target).",
                   (*T, "result"))
    requests = counter("ting_push_requests_total", "POSTs to VictoriaMetrics by HTTP status (or error).", (*T, "code"))
    duration = histogram("ting_push_duration_seconds", "POST latency.", T)
    push_up = gauge("ting_push_up", "1 while the target takes data.", T)
    lag_samples = gauge("ting_push_lag_samples", "Outbox samples this target has not taken yet.", T)
    lag_seconds = gauge("ting_push_lag_seconds", "Age of the oldest sample this target has not taken yet.", T)
    for name, pusher in app.pushers.items():
        push.add((name, "ok"), pusher.results["ok"])
        push.add((name, "rejected"), pusher.results["rejected"])
        push.add((name, "dropped"), app.outbox.dropped[name])
        for code, n in sorted(pusher.client.requests.items()):
            requests.add((name, code), n)
        duration.histogram((name,), pusher.client.duration.cumulative(), pusher.client.duration.sum)
        push_up.add((name,), int(pusher.up))
        behind, age = app.outbox.lag(name)
        lag_samples.add((name,), behind)
        lag_seconds.add((name,), round(age, 3))
    out += [push, requests, duration, push_up, lag_samples, lag_seconds]
    out.append(gauge("ting_outbox_bytes", "Bytes of outbox segments on disk.").add((), app.outbox.disk_bytes()))
    out.append(gauge("ting_outbox_segments", "Outbox segments (on disk and in memory).").add((), len(app.outbox.segments)))
    out.append(gauge("ting_outbox_disk_ok", "0 while the outbox cannot write its disk and keeps samples in memory.")
               .add((), int(app.outbox.disk_ok)))
    out.append(counter("ting_outbox_errors_total", "Outbox disk errors (writes, compression).").add((), app.outbox.errors))
    out.append(counter("ting_outbox_rejected_files_total", "Unreadable segments and refused inbox files moved to "
                       "rejected/.").add((), app.outbox.rejected_files))
    out.append(counter("ting_outbox_inbox_samples_total", "Samples taken from the outbox inbox (`mark`).")
               .add((), app.outbox.inbox_lines))

    ids = app.identities
    out.append(gauge("ting_auth_state", "0 ok, 1 backing off, 2 held (credentials rejected: fix the password file).")
               .add((), ids.state_value))
    signins = counter("ting_auth_signins_total", "Cognito sign-ins by method and result.", ("method", "result"))
    for method in ("srp", "refresh"):
        for result in ("ok", "rejected", "error"):
            signins.add((method, result), ids.signins[(method, result)])
    out.append(signins)
    restarts = counter("ting_task_restarts_total", "Supervisor restarts by task.", ("task",))
    for task in sorted(app.task_names):
        restarts.add((task,), app.restarts[task])
    out.append(restarts)

    rec = app.recorder
    out.append(gauge("ting_record_enabled", "1 if the flight recorder is configured.").add((), int(rec is not None)))
    if rec is not None:
        out.append(counter("ting_record_lines_total", "Lines written by the flight recorder.").add((), rec.lines))
        out.append(counter("ting_record_errors_total", "Flight recorder errors (failed writes pause it 5 min).").add((), rec.errors))
        out.append(gauge("ting_record_bytes", "Bytes of finished flight-recorder files on disk.").add((), rec.bytes))

    tracker = app.notes
    out.append(gauge("ting_notifications_enabled", "1 if the notification history is polled.").add((), int(tracker is not None)))
    if tracker is not None:
        notes = gauge("ting_notifications_in_history", "Ting notifications in the account history (about 3 months).",
                      (*S, "type"))
        for (serial, kind), n in sorted(tracker.counts().items()):
            notes.add((serial, pipeline.site(serial), kind), n)
        active = gauge("ting_power_outage_active", "1 while an outage Ting reported has not ended (not after 24 h "
                       "without news).", (*S, "kind"))
        for serial, kind in sorted(tracker.open_outages(int(app.clock.time() * 1000)).items()):
            active.add((serial, pipeline.site(serial), kind), 1)
        polls = counter("ting_notification_polls_total", "Polls of the notification history by result.", ("result",))
        for result in ("ok", "error"):
            polls.add((result,), app.notes_polls[result])
        out += [notes, active, polls,
                gauge("ting_notifications_unparsable", "Records of the last poll without id, serial or plausible time.")
                .add((), tracker.unparsable)]
        if app.notes_last_ok is not None:
            out.append(gauge("ting_notification_last_poll_timestamp_seconds", "When the last poll succeeded.")
                       .add((), app.notes_last_ok))

    if app.rest_polls:
        polls = counter("ting_rest_polls_total", "REST polls (hazards, conditions, frozen pipe) by endpoint and result.",
                        ("endpoint", "result"))
        for (endpoint, result), n in sorted(app.rest_polls.items()):
            polls.add((endpoint, result), n)
        missing = gauge("ting_rest_missing", "REST values the last poll did not contain, by key.", ("key",))
        for key, n in sorted(app.rest_missing.items()):
            missing.add((key,), n)
        out += [polls, missing]

    if app.repair is not None:
        repaired = counter("ting_rollup_repaired_minutes_total", "Rollup minutes filled by the repair.", T)
        stale = gauge("ting_rollup_stale_minutes", "Minutes in the last 48 h whose rollup came from partial data.", T)
        runs = counter("ting_rollup_repair_runs_total", "Repair runs by result.", (*T, "result"))
        for name in app.pushers:
            repaired.add((name,), app.repair.repaired[name])
            if name in app.repair.stale:
                stale.add((name,), app.repair.stale[name])
            for result in ("ok", "skipped", "error"):
                runs.add((name, result), app.repair.runs[(name, result)])
        out += [repaired, stale, runs]

    if app.health is not None:
        check_up = gauge("ting_health_check_up", "1 while the external health check passes (TING_HEALTH_CHECKS).", ("check",))
        for check in app.health.checks:
            if check.up is not None:
                check_up.add((check.name,), int(check.up))
        sent = counter("ting_health_notifications_total", "Notifications the health checks sent to the webhook.", ("result",))
        for result in ("ok", "error"):
            sent.add((result,), app.health.notifications[result])
        out += [check_up, sent]
    return out + expo.process_families()


def readiness(app: App) -> tuple[bool, dict]:
    now = app.clock.monotonic()
    sensors = {}
    ready = bool(app.sessions)
    for serial, state in sorted(app.sessions.items()):
        st = app.pipeline.sensor(serial)
        age = None if state.last_primary is None else round(now - state.last_primary, 1)
        is_up = state.connected and age is not None and age <= app.cfg.stale
        ready = ready and is_up
        sensors[serial] = {"site": st.site, "up": is_up, "connected": state.connected, "last_sample_age_seconds": age,
                           "delay_seconds": None if st.timing.last_offset is None else round(st.timing.last_offset, 3),
                           "subscribed": state.subscribed, "last_error": state.last_error}
    targets = {}
    for name, pusher in app.pushers.items():
        behind, age = app.outbox.lag(name)
        targets[name] = {"up": pusher.up, "lag_samples": behind, "lag_seconds": round(age, 1),
                         "failing_seconds": None if pusher.failing_since is None else round(now - pusher.failing_since)}
    body = {"ready": ready, "role": app.cfg.role, "host": app.cfg.host, "sensors": sensors, "targets": targets,
            "auth_state": app.identities.state, "auth_error": app.identities.last_error,
            "outbox": {"disk_ok": app.outbox.disk_ok, "bytes": app.outbox.disk_bytes()}}
    return ready, body  # a target that is down does not make the exporter unready: the outbox keeps its samples


def liveness(app: App) -> tuple[bool, str]:
    now = app.clock.monotonic()
    if now - app.heartbeat > LIVENESS_HEARTBEAT:
        return False, f"event loop heartbeat {now - app.heartbeat:.0f} s old"
    window = max(LIVENESS_RUN, 3 * app.cfg.push_interval)
    for name, task in app.tasks.items():
        if task.done():
            return False, f"task {name} stopped"
    if now - app.writer_last > window:
        return False, f"outbox writer last ran {now - app.writer_last:.0f} s ago"
    for name, pusher in app.pushers.items():
        if pusher.failing_since is None and now - pusher.last_run > window:  # a long POST or catch-up counts as running
            return False, f"pusher {name} last ran {now - pusher.last_run:.0f} s ago"
    return True, "ok"


def make_app(app: App) -> web.Application:
    async def metrics(_: web.Request) -> web.Response:
        return web.Response(body=expo.render(families(app)), headers={"Content-Type": expo.CONTENT_TYPE})

    async def healthz(_: web.Request) -> web.Response:
        ok, text = liveness(app)
        return web.Response(text=text + "\n", status=200 if ok else 503)

    async def readyz(_: web.Request) -> web.Response:
        ok, body = readiness(app)
        return web.Response(text=json.dumps(body, indent=2) + "\n", content_type="application/json", status=200 if ok else 503)

    web_app = web.Application()
    web_app.router.add_get("/metrics", metrics)
    web_app.router.add_get("/healthz", healthz)
    web_app.router.add_get("/readyz", readyz)
    return web_app
