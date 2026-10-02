"""Deploy files (T20): pinned images, hardening, secrets as directories, restart policy, alert rules on real metrics."""

import re
from pathlib import Path

import pytest

from ting_exporter import rest, signals

yaml = pytest.importorskip("yaml")
ROOT = Path(__file__).resolve().parent.parent
DEPLOY = ROOT / "deploy"


def load(name):
    return yaml.safe_load((DEPLOY / name).read_text())


@pytest.fixture(scope="module")
def compose():
    return load("compose.yml")


def test_every_image_is_pinned_by_a_required_variable(compose):
    notify = load("compose.notify.yml")
    for name, svc in {**compose["services"], **notify["services"]}.items():
        if "image" not in svc or "build" in svc:
            continue  # ting-exporter is built here, from the pinned PYTHON_IMAGE
        image = svc["image"]
        assert "latest" not in image and re.search(r"\$\{[A-Z_]+(:\?[^}]*)?\}", image), (name, image)
    assert "${PYTHON_IMAGE:?" in str(compose["services"]["ting-exporter"]["build"]["args"]["PYTHON_IMAGE"])
    assert "--require-hashes" in (ROOT / "Dockerfile").read_text()
    lock = (ROOT / "requirements.lock").read_text()
    pinned = re.findall(r"^([a-z0-9-]+)==", lock, re.M)
    assert {"aiohttp", "msgpack"} <= set(pinned) and lock.count("--hash=sha256:") >= len(pinned)
    assert "prometheus" not in lock  # design 4.2: two runtime dependencies


def test_hardening_and_ports(compose):
    services = compose["services"]
    for name in ("vmalert", "ting-exporter"):
        svc = services[name]
        assert svc["read_only"] and svc["cap_drop"] == ["ALL"] and "no-new-privileges:true" in svc["security_opt"], name
        assert "ports" not in svc, f"{name} must not publish a port"
    for name, svc in services.items():
        assert svc["restart"] == "unless-stopped" and svc["logging"]["options"]["max-size"] == "10m", name
        assert "mem_limit" in svc, name
    assert services["victoriametrics"]["ports"] == ["${VM_LISTEN:?set VM_LISTEN in site.env}:8428:8428"]
    exporter = services["ting-exporter"]
    assert exporter["stop_grace_period"] == "20s" and exporter["tmpfs"] == ["/tmp"]
    assert "/healthz" in " ".join(exporter["healthcheck"]["test"])


def test_secrets_are_directory_mounts_that_must_exist(compose):
    """Design 6.9: a single-file bind mount pins the inode; an editor that replaces the file goes unseen."""
    volumes = [v for v in compose["services"]["ting-exporter"]["volumes"] if isinstance(v, dict)]
    mounts = {v["target"]: v for v in volumes}
    for target in ("/run/secrets/ting", "/run/secrets/alert"):
        assert mounts[target]["read_only"] and mounts[target]["bind"]["create_host_path"] is False
    env = compose["services"]["ting-exporter"]["environment"]
    assert env["TING_PASSWORD_FILE"] == "/run/secrets/ting/ting_password"
    assert env["TING_ALERT_WEBHOOK_FILE"] == "/run/secrets/alert/alert_webhook_url"
    assert "TING_PASSWORD" not in env
    am = load("alertmanager/alertmanager.yml")
    assert am["receivers"][0]["webhook_configs"][0]["url_file"] == "/run/secrets/alert/alert_webhook_url"
    assert am["receivers"][0]["webhook_configs"][0]["send_resolved"] is True


def test_the_ssd_guard_is_on_every_service_that_writes_the_ssd(compose):
    notify = load("compose.notify.yml")
    for name, svc in {**compose["services"], "alertmanager": notify["services"]["alertmanager"]}.items():
        if name == "vmalert":
            continue  # writes nothing to disk
        guards = [v for v in svc["volumes"] if (isinstance(v, dict) and v.get("target") == "/ssd-check") or v == compose["x-ssd-guard"]]
        assert guards, f"{name} lacks the data disk mount guard"
    assert compose["x-ssd-guard"]["bind"]["create_host_path"] is False


def test_victoriametrics_flags(compose):
    cmd = compose["services"]["victoriametrics"]["command"]
    assert "-retentionPeriod=5y" in cmd and "-dedup.minScrapeInterval=1ms" in cmd  # five years (design 6.11)
    assert "-storage.minFreeDiskSpaceBytes=10GB" in cmd and "-memory.allowedBytes=512MiB" in cmd
    assert not [c for c in cmd if c.startswith("-search.maxPointsSubqueryPerTimeseries")]  # design 10: default now
    keyed = [c for c in cmd if c.endswith("AuthKey=${VM_ADMIN_KEY}") or "AuthKey=${VM_ADMIN_KEY:?" in c]
    assert len(keyed) == 11
    vmalert = compose["services"]["vmalert"]["command"]
    assert "-rule=/etc/vmalert/*.yml" in vmalert and not [c for c in vmalert if c.startswith("-external.label")]
    assert any(c.startswith("${VMALERT_NOTIFIER:?") for c in vmalert)
    for svc in load("compose.yml")["services"].values():  # a ": " inside a ${...:?message} turns the item into a map
        assert all(isinstance(c, str) for c in svc.get("command", []))


def test_the_exporter_takes_every_host_value_from_site_env(compose):
    env = compose["services"]["ting-exporter"]["environment"]
    for key in ("TING_HOST", "TING_ROLE", "TING_SITES", "TING_VM_URLS", "TING_HEALTH_CHECKS", "TING_USERNAME"):
        assert key in env and "${" in str(env[key]), key
    assert "TING_RELEASE_OTHERS" not in env  # never with a second exporter (design 2.3)
    scrape = load("victoriametrics/scrape.yml")
    jobs = {j["job_name"]: j for j in scrape["scrape_configs"]}
    assert jobs["ting-exporter"]["static_configs"][0]["targets"] == ["ting-exporter:9786"]
    assert jobs["ting-exporter"]["static_configs"][0]["labels"] == {"host": "%{TING_HOST}"}


def _metric_names(expr):
    names = set(re.findall(r"\b([a-z_][a-z0-9_]*:?[a-z0-9_:]*)\s*(?:\{|\[|\)|>|<|==|$| )", expr))
    return {n for n in names if n.startswith(("ting", "vm_", "up")) or n == "ALERTS"}


def test_alert_rules_use_existing_metrics():
    from ting_exporter import selfmetrics

    exporter = set(re.findall(r'"(ting_[a-z_]+)"', Path(selfmetrics.__file__).read_text()))
    known = exporter | {s.metric for s in signals.REGISTRY} | set(signals.DERIVED_METRICS) | rest.REST_METRICS
    known |= {s.record_name(r) for s in signals.REGISTRY for r in s.rollups}
    known |= {"up", "vm_data_size_bytes", "vm_free_disk_space_bytes", "vm_storage_is_read_only", "vm_rows_invalid_total",
              "vm_rows_ignored_total"}
    rules = load("vmalert/ting-alerts.yml")["groups"][0]["rules"]
    names = {r["alert"] for r in rules}
    assert {"TingExporterDown", "TingSensorSilent", "TingAuthProblem", "TingPushLag", "TingDataDropped",
            "TingTimestampFallback", "TingDataIncomplete", "TingRollupsMissing", "TingFlightRecorderErrors",
            "TingOutboxWriteErrors", "TingTaskCrashLoop", "VictoriaMetricsDataSizeHigh", "RecordingsDiskLow",
            "RecordingsDiskCritical", "VictoriaMetricsRowsRejected", "VmalertDown", "TingVoltageOutsideRangeA",
            "TingHazard"} <= names
    for rule in rules:
        for name in re.findall(r"\b(ting[a-z0-9_:]*|vm_[a-z_]+)\b", re.sub(r'"[^"]*"', '""', rule["expr"])):
            assert name in known, f"{rule['alert']}: {name} does not exist"
    incomplete = next(r for r in rules if r["alert"] == "TingDataIncomplete")
    assert incomplete["expr"].startswith("sum_over_time(") and "offset 2m" in incomplete["expr"]


def test_alertmanager_inhibits_only_what_it_explains():
    am = load("alertmanager/alertmanager.yml")
    disk = [r for r in am["inhibit_rules"] if r["source_matchers"] == ['alertname="RecordingsDiskCritical"']]
    assert ['alertname="TingPushLag"', 'target="local"'] in [r["target_matchers"] for r in disk]
    assert am["route"]["repeat_interval"] == "12h" and am["route"]["routes"][0]["repeat_interval"] == "7d"


def test_home_assistant_automation_uses_a_secret_webhook_id():
    text = (DEPLOY / "homeassistant" / "ting-alerts-automation.yaml").read_text()
    assert "!secret ting_alert_webhook_id" in text and "trigger.json.alerts" in text


def test_no_data_directory_is_created_on_the_wrong_disk(compose):
    """With the data disk not mounted, Docker would create a missing bind source on the root disk."""
    notify = load("compose.notify.yml")
    for name, svc in {**compose["services"], **notify["services"]}.items():
        for v in svc.get("volumes", []):
            source = v["source"] if isinstance(v, dict) else v.split(":")[0]
            if "DATA_ROOT" in source or "secrets" in source:
                assert isinstance(v, dict) and v["bind"]["create_host_path"] is False, (name, source)
