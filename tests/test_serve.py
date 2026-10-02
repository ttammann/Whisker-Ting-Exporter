"""End to end: FakeCognito + fake REST + FakeHub -> serve -> outbox -> FakeVM (local and peer), recorder, /metrics."""

import asyncio
import gzip
import io
import json
import logging
import socket

import aiohttp
import pytest
from aiohttp import web

from ting_exporter import logs
from ting_exporter.clock import ScaledClock
from ting_exporter.config import Config
from ting_exporter.pipeline import Pipeline
from ting_exporter.recorder import expand
from ting_exporter.replay import replay
from ting_exporter.serve import serve

from . import fakes

PASSWORD = "s3cret-Password!"
SCALE = 40.0


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def cloud_app(cognito, hub, vm, peer, notifications=fakes.fake_notifications):
    app = web.Application()
    app.router.add_post("/cognito/", cognito.handle)
    app.router.add_get("/api/v1/Users/{user_id}", fakes.fake_users)
    app.router.add_get("/api/v1/Users/{user_id}/conditions", fakes.fake_conditions)
    app.router.add_get("/api/v1/FrozenPipe/{serial}", fakes.fake_frozen_pipe)
    app.router.add_get("/api/v1/Notifications/history/{user_id}", notifications)
    app.router.add_get("/dataHub", hub.handle)
    vm.routes(app)
    peer.routes(app, "/peer")
    return app


@pytest.fixture
async def cloud():
    cognito = fakes.FakeCognito(PASSWORD)
    hub = fakes.FakeHub(speed=SCALE, rows=fakes.capture_rows(fakes.CAPTURE[:1]))
    vm, peer = fakes.FakeVM(), fakes.FakeVM()  # this site's store, and the other site's at {base}/peer
    runner, base = await fakes.start_app(cloud_app(cognito, hub, vm, peer))
    yield cognito, hub, vm, base, peer
    await runner.cleanup()


def env_for(base, tmp_path, port, **extra):
    secret = tmp_path / "ting_password"
    secret.write_text(PASSWORD + "\n")
    secret.chmod(0o600)
    env = {
        "TING_USERNAME": "me@example.com",
        "TING_PASSWORD_FILE": str(secret),
        "TING_SITES": "TNG000001=a,TNG000002=b",
        "TING_LISTEN": f"127.0.0.1:{port}",
        "TING_VM_URLS": f"local={base}",
        "TING_OUTBOX_DIR": str(tmp_path / "outbox"),
        "TING_STATE_FILE": str(tmp_path / "state.json"),
        "TING_RECORD_DIR": str(tmp_path / "raw"),
        "TING_REPAIR_INTERVAL_SECONDS": "0",
        "TING_COGNITO_URL": f"{base}/cognito/",
        "TING_API_URL": base,
        "TING_HUB_URL": f"{base.replace('http', 'ws')}/dataHub",
        "TING_HOST": "test-host",
    }
    env.update(extra)
    return env


def two_targets(base, local="", peer="/peer"):
    return {"TING_VM_URLS": f"local={base}{local},peer={base}{peer}"}


async def until(cond, timeout=10.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not cond():
        assert loop.time() < end, "condition not reached"
        await asyncio.sleep(0.02)


async def scrape(port, path="/metrics"):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://127.0.0.1:{port}{path}") as resp:
            return await resp.text()


def clock_for(hub):
    return ScaledClock(SCALE, start_wall=hub.rows[0]["t"])


async def test_serve_end_to_end(cloud, tmp_path):
    cognito, hub, vm, base, peer = cloud
    port = free_port()
    cfg = Config.from_env(env_for(base, tmp_path, port, TING_RELEASE_OTHERS="true", **two_targets(base)))
    log_stream = io.StringIO()
    logs.setup("DEBUG", "text", log_stream)
    stop = asyncio.Event()
    task = asyncio.create_task(serve(cfg, stop, clock_for(hub)))
    await until(lambda: vm.count("ting_voltage_volts", serial="TNG000001") > 40 and vm.count("ting_voltage_volts", serial="TNG000002") > 40
                and vm.count("ting_outdoor_temperature_celsius") >= 2 and peer.count("ting_voltage_volts") > 80)
    metrics = await scrape(port)
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://127.0.0.1:{port}/healthz") as resp:
            health = resp.status
        async with session.get(f"http://127.0.0.1:{port}/readyz") as resp:
            ready, readiness = resp.status, await resp.json()
    from contextlib import redirect_stdout

    from ting_exporter.__main__ import status

    shown = io.StringIO()
    with redirect_stdout(shown):
        status_code = await asyncio.to_thread(status, cfg)
    stop.set()
    await asyncio.wait_for(task, 15)
    logging.getLogger().handlers.clear()

    assert health == 200 and ready == 200 and readiness["sensors"]["TNG000002"]["site"] == "b"
    assert status_code == 0 and shown.getvalue().startswith("ready  role=primary host=test-host sign-in=ok")
    assert "  sensor TNG000002 site=b up" in shown.getvalue() and "  store peer up" in shown.getvalue()
    assert readiness["host"] == "test-host" and readiness["targets"]["peer"]["up"]
    assert cognito.calls.count("srp") == 1  # one sign-in for two sensors, discovery, notifications and REST
    assert vm.count("ting_frequency_hertz", serial="TNG000002", site="b") > 40
    assert vm.count("ting_thd_ratio", serial="TNG000002") >= 1
    stream = [ts for (n, _l, ts) in vm.samples if n.startswith(("ting_voltage", "ting_frequency", "ting_hifi", "ting_thd"))]
    assert stream and all(1773172740000 < ts < 1773173100000 for ts in stream)  # device time: the recording's minutes

    # REST values: the shapes a real account sends, with the conditions' fresher device fields
    assert vm.count("ting_outdoor_temperature_celsius", serial="TNG000002", site="b") >= 1
    temps = {dict(lab)["serial"]: v for (n, lab, _ts), v in vm.samples.items() if n == "ting_outdoor_temperature_celsius"}
    assert temps == {"TNG000001": 21.5, "TNG000002": -3.25}
    risks = {dict(lab)["serial"]: v for (n, lab, _ts), v in vm.samples.items() if n == "ting_outage_risk"}
    assert risks == {"TNG000001": 29.0, "TNG000002": 41.0}
    hazard = {dict(lab)["serial"]: v for (n, lab, _ts), v in vm.samples.items() if n == "ting_hazard_state"}
    assert hazard == {"TNG000001": 0.0, "TNG000002": 3.0}  # B: ElevatedSuspicious from the conditions
    assert vm.count("ting_frozen_pipe_level", serial="TNG000002") >= 1 and not vm.count("ting_frozen_pipe_level", serial="TNG000001")

    for line in (
        'ting_device_info{serial="TNG000001",site="a",name="Example Home",type="FireSensor",firmware="SparkFault 2.6.17"} 1',
        'ting_stream_up{serial="TNG000002",site="b"} 1',
        "ting_auth_state 0",
        'ting_auth_signins_total{method="srp",result="ok"} 1',
        'ting_timestamp_fallback_total{serial="TNG000002",site="b"} 0',
        "ting_record_enabled 1",
        'ting_push_up{target="local"} 1',
        'ting_push_up{target="peer"} 1',
        "ting_outbox_disk_ok 1",
        'ting_exporter_build_info{version="3.0.0",role="primary",host="test-host"} 1',
        'ting_rest_polls_total{endpoint="conditions",result="ok"}',
    ):
        assert line in metrics, line
    assert 'ting_hub_messages_total{serial="TNG000002",site="b",target="updateGraphMulti"}' in metrics
    assert vm.count("ting_notification", serial="TNG000001", type="CommunityPowerOutage") == 1
    assert vm.count("ting_power_outage", serial="TNG000001") == 3  # start, the 22:49:00 UTC minute, the closing 0
    assert "ting_delivery_delay_seconds_bucket{" in metrics and "process_resident_memory_bytes" in metrics
    assert "# TYPE ting_push_samples_total counter" in metrics

    # TING_RELEASE_OTHERS=true: released before subscribing and on shutdown
    released = {(s, e) for t, s, e in hub.calls if t == "UnInitializeStreaming"}
    assert ("TNG000002", "thdMax") in released and ("TNG000001", "ComboBinaryData") in released

    # flight recorder: hourly gzip files, updateGraphMulti only as an hourly count, replayable into the same samples
    files = expand([tmp_path / "raw"])
    assert files and all(p.name.endswith(".jsonl.gz") for p in files)
    rows = [json.loads(line) for p in files for line in gzip.open(p, "rt")]
    assert rows[0]["kind"] == "start" and rows[-1]["kind"] == "end"
    assert not [r for r in rows if r.get("target") == "updateGraphMulti" and r["kind"] == "invocation"]
    counted = sum(r["count"] for r in rows if r.get("event") == "graph_multi_count")
    assert counted > 0
    outbox_text = "".join(gzip.decompress(p.read_bytes()).decode() if p.suffix == ".gz" else p.read_text()
                          for p in sorted((tmp_path / "outbox").glob("seg-*")))
    pipeline = Pipeline(cfg.sites)
    await replay(files, pipeline, dry_run=True, out=io.StringIO())
    in_outbox = outbox_text.count('ting_voltage_volts{serial="TNG000002"')
    assert pipeline.sensors["TNG000002"].emitted["ting_voltage_volts"] == in_outbox >= vm.count("ting_voltage_volts", serial="TNG000002")

    # the outbox delivered to both stores, identically (what the last flush appended waits for the next start)
    assert peer.samples == vm.samples
    assert json.loads((tmp_path / "state.json").read_text())["silences"]["TNG000002"]["last_ms"] > 0

    # design 4.1 principle 6, T18: no secret anywhere
    record_text = "\n".join(gzip.open(p, "rt").read() for p in files)
    state_text = (tmp_path / "state.json").read_text()
    for secret in (PASSWORD, fakes.API_KEY, fakes.ACCESS_TOKEN, fakes.REFRESH_TOKEN):
        for name, text in (("log", log_stream.getvalue()), ("metrics", metrics), ("record", record_text),
                           ("readyz", json.dumps(readiness)), ("repr", repr(cfg)), ("outbox", outbox_text), ("state", state_text)):
            assert secret not in text, f"{secret!r} leaked into {name}"
    for personal in ("me@example.com", "person@example.org"):
        assert personal not in log_stream.getvalue() and personal not in repr(cfg) and personal not in outbox_text


async def test_a_store_that_is_down_keeps_its_backlog_in_the_outbox_and_catches_up(cloud, tmp_path):
    """T12: the peer (or the tunnel) is down; the local store gets everything meanwhile; nothing is lost."""
    cognito, hub, vm, base, peer = cloud
    port = free_port()
    peer.mode = "500"
    cfg = Config.from_env(env_for(base, tmp_path, port, TING_RECORD_DIR="off", TING_SITES="TNG000002=b",
                                  TING_REST_INTERVAL_SECONDS="0", **two_targets(base)))
    stop = asyncio.Event()
    task = asyncio.create_task(serve(cfg, stop, clock_for(hub)))
    await until(lambda: vm.count("ting_voltage_volts") > 80)
    metrics = await scrape(port)
    assert 'ting_push_up{target="peer"} 0' in metrics and 'ting_push_up{target="local"} 1' in metrics
    lag = next(float(x.split()[-1]) for x in metrics.splitlines() if x.startswith('ting_push_lag_samples{target="peer"}'))
    assert lag > 80 and peer.received == 0
    peer.mode = "ok"
    await until(lambda: peer.count("ting_voltage_volts") >= vm.count("ting_voltage_volts") > 0)
    stop.set()
    await asyncio.wait_for(task, 15)
    assert peer.samples == vm.samples


async def test_a_failing_flight_recorder_never_stops_the_stream(cloud, tmp_path, monkeypatch):
    """T19: the record directory cannot be written: samples still arrive, no hub session restarts."""
    from ting_exporter import recorder

    cognito, hub, vm, base, _ = cloud
    port = free_port()

    def read_only(*_a, **_k):
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(recorder, "open", read_only, raising=False)
    monkeypatch.setattr(recorder, "ERROR_PAUSE", 5.0)  # policy seconds: retry many times within the test
    cfg = Config.from_env(env_for(base, tmp_path, port, TING_SITES="TNG000002=b"))
    stop = asyncio.Event()
    task = asyncio.create_task(serve(cfg, stop, clock_for(hub)))
    await until(lambda: vm.count("ting_voltage_volts", serial="TNG000002") > 40)
    metrics = await scrape(port)
    stop.set()
    await asyncio.wait_for(task, 15)
    errors = next(float(x.split()[-1]) for x in metrics.splitlines() if x.startswith("ting_record_errors_total "))
    assert errors >= 2
    assert 'ting_task_restarts_total{task="hub_session:TNG000002"} 0' in metrics


async def test_bad_password_holds_without_restart_loop(cloud, tmp_path):
    cognito, hub, vm, base, _ = cloud
    port = free_port()
    cfg = Config.from_env(env_for(base, tmp_path, port, TING_RECORD_DIR="off"))
    (tmp_path / "ting_password").write_text("wrong")
    stop = asyncio.Event()
    task = asyncio.create_task(serve(cfg, stop, ScaledClock(1000.0)))
    await asyncio.sleep(1.0)  # ~17 simulated minutes
    metrics = await scrape(port)
    async with aiohttp.ClientSession() as session:
        async with session.get(f"http://127.0.0.1:{port}/healthz") as resp:
            health = resp.status
    stop.set()
    await asyncio.wait_for(task, 15)
    assert cognito.calls.count("verifier") == 1 and hub.connections == 0
    assert "ting_auth_state 2" in metrics and health == 200  # alive, not restarting


async def test_discovery_starts_a_new_sensor_when_no_serials_are_configured(monkeypatch):
    from types import SimpleNamespace

    from ting_exporter import serve as servemod
    from ting_exporter.cloud.api import Device

    def dev(serial):
        return Device(serial, "Home", "FireSensor", "SparkFault 2.6.17")

    accounts = [[dev("TNG000002")], [dev("TNG000002"), dev("TNG000001")]]  # a sensor is added between runs

    async def discover(session, cfg, identities):
        return accounts[0] if len(accounts) == 1 else accounts.pop(0)

    monkeypatch.setattr(servemod, "discover_once", discover)
    cfg = Config.from_env({"TING_SITES": ""}, need_credentials=False)
    app = SimpleNamespace(cfg=cfg, identities=None, clock=ScaledClock(1e6), sessions={}, devices={},
                          pipeline=Pipeline({}))
    started = []

    def start_session(serial):
        started.append(serial)
        app.sessions[serial] = object()

    stop, found = asyncio.Event(), asyncio.Event()
    task = asyncio.create_task(servemod._discovery_loop(app, None, stop, found, start_session))
    await until(lambda: len(started) == 2, 2)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert started == ["TNG000002", "TNG000001"]


async def test_discovery_only_logs_a_new_sensor_when_serials_are_configured(monkeypatch, caplog):
    from types import SimpleNamespace

    from ting_exporter import serve as servemod
    from ting_exporter.cloud.api import Device

    async def discover(session, cfg, identities):
        return [Device("TNG000002", "Home", "FireSensor", "x"), Device("TNG000009", "Home", "FireSensor", "x")]

    monkeypatch.setattr(servemod, "discover_once", discover)
    cfg = Config.from_env({"TING_SITES": "TNG000002=b"}, need_credentials=False)
    app = SimpleNamespace(cfg=cfg, identities=None, clock=ScaledClock(1e6), sessions={"TNG000002": object()},
                          devices={}, pipeline=Pipeline(cfg.sites))
    started = []
    stop, found = asyncio.Event(), asyncio.Event()
    with caplog.at_level(logging.INFO, logger="ting_exporter.serve"):
        task = asyncio.create_task(servemod._discovery_loop(app, None, stop, found, started.append))
        await asyncio.wait_for(found.wait(), 2)
        stop.set()
        await asyncio.wait_for(task, 2)
    assert started == []
    assert "TNG000009" in caplog.text and "not configured, not streamed" in caplog.text


async def test_shutdown_fits_the_grace_period_while_a_store_and_the_cloud_hang(tmp_path):
    """T13: Docker kills the container 20 s after SIGTERM. A history request and the store both hang; shutdown
    still finishes quickly and nothing pending is lost: it is in the outbox for the next start."""
    cognito, hub = fakes.FakeCognito(PASSWORD), fakes.FakeHub(speed=SCALE, rows=fakes.capture_rows(fakes.CAPTURE[:1]))
    vm, peer = fakes.FakeVM(), fakes.FakeVM()
    polled, release = asyncio.Event(), asyncio.Event()

    async def hang(_request):
        polled.set()
        await release.wait()
        return web.json_response([])

    runner, base = await fakes.start_app(cloud_app(cognito, hub, vm, peer, notifications=hang))
    try:
        cfg = Config.from_env(env_for(base, tmp_path, free_port(), TING_SITES="TNG000002=b"))
        stop = asyncio.Event()
        task = asyncio.create_task(serve(cfg, stop, clock_for(hub)))
        await until(lambda: polled.is_set() and vm.count("ting_voltage_volts") > 40)
        vm.mode = "hang"
        await asyncio.sleep(0.3)
        loop = asyncio.get_running_loop()
        started = loop.time()
        stop.set()
        await asyncio.wait_for(task, 40)
        assert loop.time() - started < 10, f"shutdown took {loop.time() - started:.1f} s"
        raw = list((tmp_path / "raw").iterdir())
        assert raw and all(p.name.endswith(".jsonl.gz") for p in raw)  # the recorder closed its hour
        cursor = (tmp_path / "outbox" / "cursor-local").read_text().split()
        assert list((tmp_path / "outbox").glob("seg-*")) and cursor  # the unsent rest waits in the outbox
    finally:
        release.set()
        await runner.cleanup()


def exporter_env(base, tmp_path, name, local, peer, **extra):
    env = env_for(base, tmp_path, free_port(), TING_RECORD_DIR="off", TING_SITES="TNG000002=b", TING_ROLE=name,
                  TING_OUTBOX_DIR=str(tmp_path / name / "outbox"), TING_STATE_FILE=str(tmp_path / name / "state.json"),
                  TING_REST_INTERVAL_SECONDS="0", **two_targets(base, local, peer))
    (tmp_path / name).mkdir(exist_ok=True)
    env.update(extra)
    return env


async def test_active_active_the_other_exporter_stopping_does_not_interrupt_the_stream(cloud, tmp_path):
    """T11: two exporters stream the same sensor and write both stores; one stops, the other carries on alone."""
    cognito, hub, vm, base, peer = cloud
    a_cfg = Config.from_env(exporter_env(base, tmp_path, "primary", "", "/peer"))
    b_cfg = Config.from_env(exporter_env(base, tmp_path, "secondary", "/peer", ""))
    a_stop, b_stop = asyncio.Event(), asyncio.Event()
    a = asyncio.create_task(serve(a_cfg, a_stop, clock_for(hub)))
    b = asyncio.create_task(serve(b_cfg, b_stop, clock_for(hub)))
    await until(lambda: len(hub.streams.get("TNG000002", ())) == 2 and vm.count("ting_voltage_volts") > 40)
    a_stop.set()
    await asyncio.wait_for(a, 15)
    before, sent_before = vm.count("ting_voltage_volts"), hub.sent["TNG000002"]
    await until(lambda: vm.count("ting_voltage_volts") > before + 40)
    readiness = json.loads(await scrape(b_cfg.listen_port, "/readyz"))
    b_stop.set()
    await asyncio.wait_for(b, 15)
    assert "UnInitializeStreaming" not in {t for t, _s, _e in hub.calls}
    assert hub.connections == 2 and hub.knocked_off == 0
    assert hub.sent["TNG000002"] > sent_before and readiness["ready"] and readiness["role"] == "secondary"
    assert peer.samples == vm.samples  # identical device timestamps collapse into one sample: the stores agree
    assert vm.count("ting_voltage_volts") == len({ts for (n, _l, ts) in vm.samples if n == "ting_voltage_volts"})


async def test_releasing_subscriptions_would_end_the_other_exporters_stream(cloud, tmp_path):
    """T11: why TING_RELEASE_OTHERS is off: with it, each exporter's subscribe ends the other's stream."""
    cognito, hub, vm, base, peer = cloud
    stops, tasks = [], []
    for name, local, other in (("primary", "", "/peer"), ("secondary", "/peer", "")):
        cfg = Config.from_env(exporter_env(base, tmp_path, name, local, other, TING_RELEASE_OTHERS="true", TING_STALE_SECONDS="10"))
        stops.append(asyncio.Event())
        tasks.append(asyncio.create_task(serve(cfg, stops[-1], clock_for(hub))))
        await until(lambda: hub.streams.get("TNG000002"))
    await until(lambda: hub.knocked_off >= 1 and hub.connections >= 3)
    for stop in stops:
        stop.set()
    await asyncio.wait_for(asyncio.gather(*tasks), 15)


async def test_a_mark_goes_through_the_inbox_to_every_store(cloud, tmp_path):
    """Context marks (design 0.4): `mark` (run inside the container) hands its lines to the running exporter, which pushes them everywhere."""
    from ting_exporter import context

    cognito, hub, vm, base, peer = cloud
    cfg = Config.from_env(env_for(base, tmp_path, free_port(), TING_RECORD_DIR="off", TING_SITES="TNG000002=b",
                                  TING_REST_INTERVAL_SECONDS="0", **two_targets(base)))
    stop = asyncio.Event()
    task = asyncio.create_task(serve(cfg, stop, clock_for(hub)))
    await until(lambda: vm.count("ting_voltage_volts") > 10)
    async with aiohttp.ClientSession() as session:
        lines, where = await context.mark(session, store=cfg.vm_url, serial="TNG000002", site="b", key="source",
                                          value="inverter", at=1790550500.0, outbox_dir=cfg.outbox_dir)
    await until(lambda: vm.count("ting_context") and peer.count("ting_context"))
    stop.set()
    await asyncio.wait_for(task, 15)
    assert "outbox" in where and lines == ['ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 1 1790550500000\n']
    assert vm.samples[("ting_context", (("context", "source"), ("serial", "TNG000002"), ("site", "b"), ("value", "inverter")),
                       1790550500000)] == 1.0
    assert not list((tmp_path / "outbox" / "inbox").iterdir())


async def test_a_restart_during_a_cut_still_infers_it(cloud, tmp_path):
    """T16 with the state file: the exporter stops while the site is dark and starts again; when the sensor returns
    with its window reset, the cut is drawn from the last sample before the restart."""
    from ting_exporter import serve as servemod

    cognito, hub, vm, base, _ = cloud
    rows = fakes.capture_rows(fakes.CAPTURE[:1])
    b_rows = [r for r in rows if r.get("serial") == "TNG000002"]
    hub.rows = b_rows[:400]  # the sensor delivers, then goes dark
    cfg = Config.from_env(env_for(base, tmp_path, free_port(), TING_RECORD_DIR="off", TING_SITES="TNG000002=b",
                                  TING_REST_INTERVAL_SECONDS="0", TING_NOTIFICATIONS_INTERVAL_SECONDS="0"))
    clock = clock_for(hub)
    stop = asyncio.Event()
    task = asyncio.create_task(serve(cfg, stop, clock))
    await until(lambda: hub.sent.get("TNG000002", 0) >= 400 and vm.count("ting_voltage_volts") > 50)
    await asyncio.sleep(0.2)
    stop.set()
    await asyncio.wait_for(task, 15)
    state = json.loads((tmp_path / "state.json").read_text())["silences"]["TNG000002"]
    last_ms = state["last_ms"]
    assert state["watched"] and state["high"] - state["low"] > 2.5

    # 2 minutes later (within RESTORE_MAX_S) the exporter starts again; 3 minutes after that the sensor returns,
    # restarted: its high and low collapsed onto the voltage
    back = json.loads(json.dumps(b_rows[400:480]))
    shift = last_ms / 1000 + 300 - back[0]["t"]
    for r in back:
        r["t"] += shift
        if r["target"] == "updateComboBinaryData":
            p = r["args"][0].get("msgpack", r["args"][0])
            when = servemod.datetime.fromisoformat(p["DataTimeUtc"]).timestamp() + shift
            p["DataTimeUtc"] = servemod.datetime.fromtimestamp(when, servemod.timezone.utc).isoformat()
            p["VoltageHi"], p["VoltageLo"] = p["Voltage"] + 0.2, p["Voltage"] - 0.2
        elif r["target"] == "updateGraphMultiCategorical":
            for rec in r["args"][0]:
                when = servemod.datetime.fromisoformat(rec["ObsTime"]).timestamp() + shift
                rec["ObsTime"] = servemod.datetime.fromtimestamp(when, servemod.timezone.utc).isoformat()
    hub.rows = back
    vm_before = vm.count("ting_voltage_volts")
    stop = asyncio.Event()
    task = asyncio.create_task(serve(cfg, stop, ScaledClock(SCALE, start_wall=last_ms / 1000 + 120)))
    await until(lambda: vm.count("ting_power_cut") >= 2 and vm.count("ting_voltage_volts") > vm_before)
    stop.set()
    await asyncio.wait_for(task, 15)
    cut = sorted((ts, v) for (n, _l, ts), v in vm.samples.items() if n == "ting_power_cut")
    assert cut[0] == (last_ms + 250, 1.0) and cut[-1][1] == 0.0
    assert all(v == 1.0 for _, v in cut[1:-1]) and all(ts % 60_000 == 0 for ts, _ in cut[1:-1])
    assert not vm.count("ting_stream_gap")


# ---- liveness and the watchdog (an unhealthy exporter restarts itself) ------------------------------------


def test_liveness_allows_for_a_long_push_interval():
    from types import SimpleNamespace

    from ting_exporter.selfmetrics import liveness

    now = [70.0]
    pusher = SimpleNamespace(failing_since=None, last_run=0.0)
    app = SimpleNamespace(clock=SimpleNamespace(monotonic=lambda: now[0]), heartbeat=70.0, pushers={"local": pusher},
                          tasks={}, writer_last=0.0, cfg=SimpleNamespace(push_interval=30.0))
    assert liveness(app) == (True, "ok")  # TING_PUSH_INTERVAL_SECONDS=30: 70 s since the last run is normal
    now[0] = app.heartbeat = 200.0
    assert not liveness(app)[0]
    pusher.failing_since = 100.0  # a pusher backing off from a dead store is not a dead pusher
    app.writer_last = 199.0
    assert liveness(app) == (True, "ok")


async def test_the_watchdog_acts_only_on_failures_in_a_row(monkeypatch):
    from types import SimpleNamespace

    from ting_exporter import selfmetrics
    from ting_exporter import serve as servemod

    verdicts = iter([False, True, False, False, False, True] + [True] * 1000)
    monkeypatch.setattr(selfmetrics, "liveness", lambda app: (next(verdicts), "pusher local last ran 99 s ago"))
    app = SimpleNamespace(clock=ScaledClock(1e4), unhealthy=None)
    stop = asyncio.Event()
    task = asyncio.create_task(servemod._watchdog(app, stop))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert app.unhealthy is None  # never WATCHDOG_STRIKES in a row

    monkeypatch.setattr(selfmetrics, "liveness", lambda app: (False, "pusher local last ran 99 s ago"))
    stop = asyncio.Event()
    await asyncio.wait_for(servemod._watchdog(app, stop), 2)
    assert stop.is_set() and "last ran 99 s ago" in app.unhealthy


def test_the_loop_guard_exits_when_the_event_loop_stops_ticking(monkeypatch):
    import threading
    import time
    from types import SimpleNamespace

    from ting_exporter import serve as servemod

    exits = []
    monkeypatch.setattr(servemod.os, "_exit", exits.append)
    servemod._loop_guard(SimpleNamespace(heartbeat_real=time.monotonic()), threading.Event(), poll=0.01, limit=0.05)
    assert exits == [1]


async def test_an_exporter_that_stays_unhealthy_exits_with_1_so_docker_restarts_it(cloud, tmp_path, monkeypatch):
    """T14."""
    from ting_exporter import selfmetrics

    cognito, hub, vm, base, _ = cloud
    monkeypatch.setattr(selfmetrics, "liveness", lambda app: (False, "pusher local last ran 99 s ago"))
    cfg = Config.from_env(env_for(base, tmp_path, free_port(), TING_RECORD_DIR="off", TING_SITES="TNG000002=b"))
    code = await asyncio.wait_for(serve(cfg, asyncio.Event(), clock_for(hub)), 30)
    assert code == 1
