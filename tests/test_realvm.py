"""Optional (T22): the fixture recordings through a real VictoriaMetrics, vmalert and vmctl.

Runs with either
  TING_VM_BIN_DIR=DIR       the release binaries `victoria-metrics-prod` and `vmalert-prod` in DIR, or
  TING_VM_DOCKER=v1.152.0   the release images victoriametrics/{victoria-metrics,vmalert,vmctl} at that tag
                            (Docker Desktop on a Mac: vmalert reaches the stores via host.docker.internal)
and is skipped otherwise. With TING_EXPORTER_IMAGE=ting-exporter:3.0.0 the import rehearsal also runs the
built exporter image, as the runbook does. Every store gets a temporary data directory and a port on 127.0.0.1.

Checks:
- import pushes the fixture samples; importing the same files again changes nothing (-dedup.minScrapeInterval=1ms)
- `vmalert -replay` with deploy/vmalert/ting-rollups.yml writes rollups equal to the independent reference
- the deploy rule files (and the example overlay's) pass `vmalert -dryRun`
- equal timestamps keep the larger value (why the repair only fills)
- the exporter's rollup repair writes what vmalert would have (T17)
- a context mark is found again by the query the dashboard uses
- Docker only: the runbook's step C, vmctl moving an old store (pushed series and rollups, not self-metrics or
  alerts), then import and repair on top
"""

import asyncio
import calendar
import csv
import functools
import json
import os
import re
import socket
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

from ting_exporter.pipeline import Pipeline
from ting_exporter.replay import replay
from ting_exporter.rules import every_rule

from .fakes import CAPTURE, CUT, FIXTURES

BIN = Path(os.environ.get("TING_VM_BIN_DIR", "/nonexistent"))
VM, VMALERT = BIN / "victoria-metrics-prod", BIN / "vmalert-prod"
DOCKER = os.environ.get("TING_VM_DOCKER", "")
EXPORTER_IMAGE = os.environ.get("TING_EXPORTER_IMAGE", "")
ROOT = Path(__file__).parent.parent
SITES = {"TNG000001": "a", "TNG000002": "b"}
KEY = "test-admin-key"
BASE_FLAGS = ["-retentionPeriod=5y", "-dedup.minScrapeInterval=1ms", "-storage.minFreeDiskSpaceBytes=100MB",
              f"-deleteAuthKey={KEY}", f"-forceFlushAuthKey={KEY}", f"-forceMergeAuthKey={KEY}", "-loggerLevel=ERROR"]
FLAGS = [*BASE_FLAGS, "-search.latencyOffset=0s", "-search.cacheTimestampOffset=0s"]  # the newest samples searchable at once
RULES = len(list(every_rule()))  # the rollup rules, each written per sensor and minute
FILTER = '{__name__=~"ting_.*|ting:.*",job=""}'  # the runbook's, step C

pytestmark = [
    pytest.mark.realvm,
    pytest.mark.skipif(not ((VM.exists() and VMALERT.exists()) or DOCKER),
                       reason="set TING_VM_BIN_DIR (release binaries) or TING_VM_DOCKER (release image tag)"),
]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def get(url):
    with urllib.request.urlopen(url, timeout=30) as resp:
        return resp.status, resp.read().decode()


class Store:
    """A VictoriaMetrics: the binary, or the release image (optionally named, on a Docker network)."""

    def __init__(self, data: Path, name: str | None = None, network: str | None = None, flags: list[str] = FLAGS) -> None:
        data.mkdir(parents=True, exist_ok=True)
        port = free_port()
        self.url, self.proc, self.container = f"http://127.0.0.1:{port}", None, None
        if DOCKER:
            cmd = ["docker", "run", "-d", "--rm", "-p", f"127.0.0.1:{port}:8428", "-v", f"{data}:/storage"]
            cmd += ["--name", name] if name else []
            cmd += ["--network", network] if network else []
            cmd += [f"victoriametrics/victoria-metrics:{DOCKER}", "-storageDataPath=/storage", *flags]
            self.container = subprocess.run(cmd, check=True, capture_output=True, text=True).stdout.strip()
        else:
            self.proc = subprocess.Popen([str(VM), f"-httpListenAddr=127.0.0.1:{port}", f"-storageDataPath={data}", *flags],
                                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        for _ in range(300):
            try:
                if get(f"{self.url}/health")[0] == 200:
                    return
            except OSError:
                time.sleep(0.05)
        self.stop()
        raise RuntimeError(f"VictoriaMetrics at {self.url} did not come up")

    def stop(self) -> None:
        if self.container:
            subprocess.run(["docker", "rm", "-f", self.container], capture_output=True)
        elif self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def vmalert(*args: str) -> subprocess.CompletedProcess:
    """vmalert with these flags: the binary, or the image (the repository mounted at /repo, stores reached via the host)."""
    if DOCKER:
        args = tuple(a.replace("http://127.0.0.1:", "http://host.docker.internal:").replace(str(ROOT), "/repo") for a in args)
        cmd = ["docker", "run", "--rm", "--add-host=host.docker.internal:host-gateway", "-v", f"{ROOT}:/repo:ro",
               f"victoriametrics/vmalert:{DOCKER}", *args]
    else:
        cmd = [str(VMALERT), *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=180)


@pytest.fixture(scope="module")
def vm(tmp_path_factory):
    store = Store(tmp_path_factory.mktemp("vmdata"))
    yield store.url
    store.stop()


def flush(base):
    get(f"{base}/internal/force_flush?authKey={KEY}")


def query(base, q, at):
    _, body = get(f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": q, "time": at}))
    return json.loads(body)["data"]["result"]


def export(base, match):
    _, body = get(f"{base}/api/v1/export?" + urllib.parse.urlencode({"match[]": match}))
    return [json.loads(line) for line in body.splitlines() if line]


def post(base, text):
    req = urllib.request.Request(f"{base}/api/v1/import/prometheus", data=text.encode())
    with urllib.request.urlopen(req, timeout=10) as resp:
        assert resp.status == 204


def raw_count(base):
    got = query(base, "sum by (serial) (count_over_time(ting_voltage_volts[3d]))", "2026-03-12T00:00:00Z")
    return {r["metric"]["serial"]: int(float(r["value"][1])) for r in got}


@functools.cache
def golden():
    out = {}
    with open(FIXTURES / "golden-rollups.csv") as f:
        for when, serial, site, rec, value in csv.reader(f):
            out[(when, serial, site, rec)] = float(value)
    return out


def utc(text):
    return calendar.timegm(time.strptime(text, "%Y-%m-%dT%H:%M:%S"))


def check_rollups(base, minutes, every=False):
    """The store's rollups of these minutes equal the reference; with `every`, all its other rollups too, each of which
    must be in it. Float rounding on avg ties: one unit in the last place, at most twice. Returns the points checked
    in `minutes`."""
    ref, checked, off_by_one = golden(), 0, 0
    for series in export(base, '{__name__=~"ting:.*_1m"}'):
        labels = series["metric"]
        assert set(labels) == {"__name__", "serial", "site"}  # the series vmalert writes, no extra label
        for ts, value in zip(series["timestamps"], series["values"]):
            when = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts / 1000))
            key = (when, labels["serial"], labels["site"], labels["__name__"])
            if when not in minutes and not every:
                continue
            assert key in ref, f"{key} is not in the reference"
            if key[3].endswith(":avg_1m"):
                step = {"ting:voltage_volts:avg_1m": 1e-4, "ting:hifi:avg_1m": 1e-2}.get(key[3], 1e-5)
                assert value == pytest.approx(ref[key], abs=step * 1.01), key
                off_by_one += abs(value - ref[key]) > 1e-9
            else:
                assert value == ref[key], key
            checked += when in minutes
    assert off_by_one <= 2
    return checked


def test_import_dedup_and_rollups(vm):
    pipeline = Pipeline(SITES)
    totals = asyncio.run(replay(CAPTURE, pipeline, vm_url=vm))
    flush(vm)
    expected = {s: st.emitted["ting_voltage_volts"] for s, st in pipeline.sensors.items()}
    assert raw_count(vm) == expected and totals["pushed"] == totals["samples"]

    asyncio.run(replay(CAPTURE, Pipeline(SITES), vm_url=vm))  # the same files again
    flush(vm)
    assert raw_count(vm) == expected  # idempotent: no duplicates

    for start, end in (("2026-03-10T20:15:00Z", "2026-03-10T20:19:00Z"), ("2026-03-11T06:05:00Z", "2026-03-11T06:09:00Z")):
        result = vmalert(f"-rule={ROOT / 'deploy' / 'vmalert' / 'ting-rollups.yml'}", f"-datasource.url={vm}",
                         f"-remoteWrite.url={vm}", f"-replay.timeFrom={start}", f"-replay.timeTo={end}",
                         "-replay.disableProgressBar", "-replay.rulesDelay=0s", "-loggerLevel=ERROR")
        assert result.returncode == 0, result.stderr
    time.sleep(2.5)  # vmalert flushes remote write every 2 s before exiting
    flush(vm)
    for when, serial in (("2026-03-10T20:18:00Z", "TNG000001"), ("2026-03-11T06:08:00Z", "TNG000002")):
        assert golden()[(when, serial, SITES[serial], "ting:voltage_volts:count_1m")] == 240  # the reference: a full minute
    complete = {"2026-03-10T20:17:00Z", "2026-03-10T20:18:00Z", "2026-03-11T06:07:00Z", "2026-03-11T06:08:00Z"}
    assert check_rollups(vm, complete, every=True) == RULES * 2 * 4  # every rule, 2 sensors, the complete minutes


def test_deploy_rule_files_pass_vmalert_dry_run():
    files = sorted((ROOT / "deploy" / "vmalert").glob("*.yml")) + sorted((ROOT / "overlay.example" / "rules").glob("*.yml"))
    assert len(files) >= 3
    result = vmalert("-dryRun", *(f"-rule={r}" for r in files), "-loggerLevel=ERROR")
    assert result.returncode == 0, result.stdout + result.stderr


def test_duplicate_timestamp_keeps_the_larger_value(vm):
    """VictoriaMetrics' dedup rule: re-importing corrected values only wins if they are larger."""
    for value in ("5", "3", "9"):
        post(vm, f'ting_dedup_probe{{a="1"}} {value} 1773172800000\n')
    flush(vm)
    [series] = export(vm, "ting_dedup_probe")
    assert series["values"] == [9]


def test_the_rollup_repair_writes_what_vmalert_would_have(tmp_path):
    """A fresh store with the raw samples of a spot window and no rollups: the repair fills every minute with
    values equal to the independent reference."""
    import aiohttp

    from ting_exporter.repair import RollupRepair

    store = Store(tmp_path / "vmrepair")
    try:
        asyncio.run(replay([p for p in CAPTURE if "spot-1" in p.name], Pipeline(SITES), vm_url=store.url))
        flush(store.url)

        async def run():
            async with aiohttp.ClientSession() as session:
                repair = RollupRepair(session, {"store": store.url}, lambda name: 0.0)
                return await repair.repair("store", store.url, now=utc("2026-03-10T20:18:00") + 600)

        assert asyncio.run(run()) >= 2 * 2
        flush(store.url)
        assert check_rollups(store.url, {"2026-03-10T20:17:00Z", "2026-03-10T20:18:00Z"}) == RULES * 2 * 2
    finally:
        store.stop()


def test_a_context_mark_is_found_by_the_dashboard_query(vm):
    import aiohttp

    from ting_exporter import context
    from ting_exporter.vmquery import VmQuery

    async def run():
        async with aiohttp.ClientSession() as session:
            await context.mark(session, store=vm, serial="TNG000002", site="b", key="source", value="mains",
                               at=1790550000.0, outbox_dir=None)
            flush(vm)  # the next mark looks the current value up
            await context.mark(session, store=vm, serial="TNG000002", site="b", key="source", value="inverter",
                               at=1790553600.0, outbox_dir=None)
            flush(vm)
            return (await context.active(VmQuery(session, vm), at=1790551000.0),
                    await context.active(VmQuery(session, vm), at=1790600000.0))

    before, after = asyncio.run(run())
    assert [m["value"] for m in before] == ["mains"] and [m["value"] for m in after] == ["inverter"]


@pytest.mark.skipif(not DOCKER, reason="needs the release images (TING_VM_DOCKER)")
def test_the_runbooks_step_c_moves_an_old_store(tmp_path):
    """An older install's store (raw samples, a rollup, notifications, plus its self-metrics and alerts) is copied
    with vmctl and the runbook's filter: the pushed series and rollups arrive, unchanged; the rest stays behind.
    Then the exporter's import (the cut capture) and repair run on top, as in the runbook."""
    net = f"ting-realvm-{os.getpid()}"
    subprocess.run(["docker", "network", "create", net], check=True, capture_output=True)
    try:
        old = Store(tmp_path / "old", name=f"{net}-old", network=net)
        new = Store(tmp_path / "new", name=f"{net}-new", network=net)
        asyncio.run(replay([p for p in CAPTURE if "spot-1" in p.name], Pipeline(SITES), vm_url=old.url))
        t = utc("2026-03-10T20:16:00") * 1000
        post(old.url, f'ting:voltage_volts:count_1m{{serial="TNG000001",site="a"}} 999 {t}\n'
                      f'ting_notification{{category="PowerQuality",serial="TNG000002",site="b",title="Power Brownout",type="Sag"}} 1 {t}\n'
                      f'ting_stream_up{{host="host-old",instance="ting-exporter:9786",job="ting-exporter",serial="TNG000001",site="a"}} 1 {t}\n'
                      f'ALERTS{{alertname="TingSensorSilent",alertstate="firing",serial="TNG000001",site="a"}} 1 {t}\n')
        flush(old.url)
        moved = subprocess.run(
            ["docker", "run", "--rm", "--network", net, f"victoriametrics/vmctl:{DOCKER}", "vm-native", "-s",
             f"--vm-native-src-addr=http://{net}-old:8428", f"--vm-native-dst-addr=http://{net}-new:8428",
             f"--vm-native-filter-match={FILTER}", "--vm-native-filter-time-start=2026-03-01T00:00:00Z"],
            capture_output=True, text=True, timeout=300)
        assert moved.returncode == 0, moved.stdout + moved.stderr
        flush(new.url)
        names = {s["metric"]["__name__"] for s in export(new.url, '{__name__=~".+"}')}
        assert {"ting_voltage_volts", "ting_frequency_hertz", "ting_notification", "ting:voltage_volts:count_1m"} <= names
        assert "ALERTS" not in names and not export(new.url, '{job="ting-exporter"}')
        assert raw_count(new.url) == raw_count(old.url) and raw_count(new.url)
        [kept] = export(new.url, 'ting:voltage_volts:count_1m{serial="TNG000001"}')
        assert kept["values"] == [999]  # the old store's rollup, unchanged

        common = ["-e", "TING_SITES=TNG000001=a,TNG000002=b", "-e", f"TING_VM_URLS=local=http://{net}-new:8428"]
        if EXPORTER_IMAGE:  # the image, as `docker compose run ... ting-exporter import /import` runs it
            imported = subprocess.run(["docker", "run", "--rm", "--network", net, *common, "-v", f"{CUT}:/import/f-cut.jsonl.gz:ro",
                                       EXPORTER_IMAGE, "import", "/import"], capture_output=True, text=True, timeout=300)
            assert imported.returncode == 0, imported.stderr
            assert "silences={'cut': 1, 'gap': 1, 'unattributed': 1}" in imported.stderr
            flush(new.url)  # searchable before the repair looks for it
            repaired = subprocess.run(["docker", "run", "--rm", "--network", net, *common, EXPORTER_IMAGE, "repair",
                                       "--from", "2026-03-10T20:00:00Z", "--to", "2026-03-11T17:00:00Z"],
                                      capture_output=True, text=True, timeout=300)
            assert repaired.returncode == 0, repaired.stderr
            total = re.search(r"^filled (\d+) minutes in total$", repaired.stdout, re.M)
            assert total and int(total[1]) > 0, repaired.stdout
        else:
            import aiohttp

            from ting_exporter.repair import repair_range

            asyncio.run(replay([CUT], Pipeline(SITES), vm_url=new.url))
            flush(new.url)  # searchable before the repair looks for it

            async def run():
                async with aiohttp.ClientSession() as session:
                    return await repair_range(session, {"local": new.url}, utc("2026-03-10T20:00:00"),
                                              utc("2026-03-11T17:00:00"), time.time(), lambda *a: None)

            assert asyncio.run(run()) > 0
        flush(new.url)
        cut = export(new.url, "ting_power_cut")
        assert cut and cut[0]["values"][0] == 1 and cut[0]["values"][-1] == 0
        assert check_rollups(new.url, {"2026-03-10T20:17:00Z", "2026-03-10T20:18:00Z"}) == RULES * 2 * 2
        [kept] = export(new.url, 'ting:voltage_volts:count_1m{serial="TNG000001"}')
        assert 999 in kept["values"]  # the repair only fills: the old store's point is still there
    finally:  # every container on the network: the stores, and a vmctl or exporter run that timed out
        left = subprocess.run(["docker", "ps", "-aq", "--filter", f"network={net}"], capture_output=True, text=True)
        if left.stdout.split():
            subprocess.run(["docker", "rm", "-f", *left.stdout.split()], capture_output=True)
        subprocess.run(["docker", "network", "rm", net], capture_output=True)


def test_a_mark_is_seen_at_once(tmp_path):
    """A store with VictoriaMetrics' default -search.latencyOffset (30 s) hides the newest samples from queries:
    `mark --list`, and the next mark's lookup of the current value, must still see a mark written seconds ago
    (found running the stack on a Mac)."""
    import aiohttp

    from ting_exporter import context
    from ting_exporter.vmquery import VmQuery

    store = Store(tmp_path / "vmdefault", flags=BASE_FLAGS)
    try:
        async def run():
            async with aiohttp.ClientSession() as session:
                now = time.time()
                await context.mark(session, store=store.url, serial="TNG000001", site="a", key="probe", value="first",
                                   at=now, outbox_dir=None)
                flush(store.url)  # searchable, as on a host a moment later; still inside the 30 s
                listed = await context.active(VmQuery(session, store.url), at=time.time(), site="a", key="probe")
                hidden = await VmQuery(session, store.url).query('last_over_time(ting_context{context="probe"}[1h])',
                                                                 time.time())
                second, _ = await context.mark(session, store=store.url, serial="TNG000001", site="a", key="probe",
                                               value="second", at=now + 1, outbox_dir=None)
                return listed, hidden, second

        listed, hidden, second = asyncio.run(run())
    finally:
        store.stop()
    assert hidden == []  # what a plain query sees: nothing, the mark is too new
    assert [m["value"] for m in listed] == ["first"]
    assert second[0].startswith('ting_context{context="probe",serial="TNG000001",site="a",value="first"} 0 ')
