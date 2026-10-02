"""Config parsing, CLI commands that need no network, supervisor, log scrubbing."""

import asyncio
import io
import logging
from collections import Counter

import pytest

from ting_exporter import __main__ as cli
from ting_exporter import logs, rules
from ting_exporter.config import Config, ConfigError
from ting_exporter.supervisor import supervise

from .fakes import CAPTURE, FIXTURES


def test_config_defaults_and_sites(tmp_path):
    secret = tmp_path / "pw"
    secret.write_text("x")
    cfg = Config.from_env({"TING_USERNAME": "someone@example.com", "TING_PASSWORD_FILE": str(secret),
                           "TING_SITES": "TNG000001=a, TNG000002=b"})
    assert cfg.sites == {"TNG000001": "a", "TNG000002": "b"}
    assert cfg.streamed_serials == ("TNG000001", "TNG000002")
    assert cfg.stale == 60 and cfg.record_retention_days == 90 and cfg.timestamp_source == "device"
    assert cfg.record_dir is not None and cfg.outbox_dir is not None and cfg.outbox_max_bytes == 256 * 2**20
    assert cfg.rest_interval == 300 and cfg.repair_interval == 300 and cfg.health_checks == ()
    assert "someone" not in repr(cfg) and "s***@example.com" in repr(cfg)


def test_config_collects_every_problem(tmp_path):
    with pytest.raises(ConfigError) as err:
        Config.from_env({"TING_PASSWORD": "x", "TING_STALE_SECONDS": "abc", "TING_SITES": "nope", "TING_TIMESTAMP_SOURCE": "gps",
                         "TING_PASSWORD_FILE": str(tmp_path / "missing"), "TING_LISTEN": "x"})
    text = str(err.value)
    for needle in ("TING_PASSWORD is not supported", "TING_USERNAME is required", "not readable", "TING_STALE_SECONDS",
                   "TING_SITES", "TING_TIMESTAMP_SOURCE", "TING_LISTEN"):
        assert needle in text


def test_push_targets_role_and_release(tmp_path):
    cfg = Config.from_env({}, need_credentials=False)
    assert cfg.vm_targets == (("local", "http://victoriametrics:8428"),) and cfg.vm_url == "http://victoriametrics:8428"
    assert cfg.role == "primary" and cfg.release_others is False
    cfg = Config.from_env({"TING_VM_URLS": "local=http://victoriametrics:8428, http://192.0.2.10:8428/,",
                           "TING_ROLE": "secondary", "TING_RELEASE_OTHERS": "yes", "TING_HOST": "host-b"},
                          need_credentials=False)
    assert cfg.vm_targets == (("local", "http://victoriametrics:8428"), ("192.0.2.10", "http://192.0.2.10:8428"))
    assert cfg.role == "secondary" and cfg.release_others is True and cfg.host == "host-b"
    with pytest.raises(ConfigError) as err:
        Config.from_env({"TING_VM_URLS": "a=http://a:8428,a=http://b:8428,rejected=http://c:8428,inbox=http://c:8428,"
                         "x=ftp://d,b c=http://e:8428,..=http://f:8428", "TING_ROLE": "two words", "TING_RELEASE_OTHERS": "maybe"},
                        need_credentials=False)
    text = str(err.value)
    for needle in ("'a' is used twice", "'rejected=http://c:8428'", "'inbox=http://c:8428'", "'x=ftp://d'",
                   "'b c=http://e:8428'", "'..=http://f:8428'", "TING_ROLE", "TING_RELEASE_OTHERS"):
        assert needle in text, needle
    with pytest.raises(ConfigError, match="TING_VM_URLS has no entry"):
        Config.from_env({"TING_VM_URLS": " , "}, need_credentials=False)


def test_variables_of_earlier_versions_are_reported_not_ignored():
    with pytest.raises(ConfigError) as err:
        Config.from_env({"TING_VM_URL": "http://vm:8428", "TING_SPOOL_DIR": "/data/spool"}, need_credentials=False)
    assert "TING_VM_URL is gone" in str(err.value) and "the outbox replaces them" in str(err.value)


def test_one_sensor_per_site():
    with pytest.raises(ConfigError, match="has two sensors"):
        Config.from_env({"TING_SITES": "TNG000001=a,TNG000002=a"}, need_credentials=False)


def test_health_checks_and_intervals():
    cfg = Config.from_env({"TING_HEALTH_CHECKS": "vm=http://victoriametrics:8428/health,peer-vm=http://192.0.2.10:8428/health",
                           "TING_REST_INTERVAL_SECONDS": "0", "TING_REPAIR_INTERVAL_SECONDS": "600"}, need_credentials=False)
    assert cfg.health_checks == (("vm", "http://victoriametrics:8428/health"), ("peer-vm", "http://192.0.2.10:8428/health"))
    assert cfg.rest_interval == 0 and cfg.repair_interval == 600
    for name, bad in (("TING_REST_INTERVAL_SECONDS", "10"), ("TING_REPAIR_INTERVAL_SECONDS", "5")):
        with pytest.raises(ConfigError, match=name):
            Config.from_env({name: bad}, need_credentials=False)


def test_config_off_switches_and_serials_override(tmp_path):
    cfg = Config.from_env({"TING_RECORD_DIR": "off", "TING_STATE_FILE": "off", "TING_SERIALS": "TNG000002",
                           "TING_SITES": "TNG000001=a,TNG000002=b"}, need_credentials=False)
    assert cfg.record_dir is None and cfg.state_file is None and cfg.streamed_serials == ("TNG000002",)
    assert cfg.site_serial("b") == "TNG000002" and cfg.site_serial("z") is None


def test_bare_ipv6_targets_get_distinct_names_and_a_bracketed_listen_address_works():
    cfg = Config.from_env({"TING_VM_URLS": "http://[fd00::1]:8428,http://[fd00::2]:8428/", "TING_LISTEN": "[::1]:9786"},
                          need_credentials=False)
    assert [name for name, _ in cfg.vm_targets] == ["fd00__1", "fd00__2"] and cfg.listen_host == "::1"


def test_check_config(tmp_path, monkeypatch, capsys):
    secret = tmp_path / "pw"
    secret.write_text("hunter2-secret")
    for k, v in {"TING_USERNAME": "a@example.org", "TING_PASSWORD_FILE": str(secret), "TING_OUTBOX_DIR": str(tmp_path / "o"),
                 "TING_RECORD_DIR": str(tmp_path / "r"), "TING_STATE_FILE": str(tmp_path / "state.json"),
                 "TING_SITES": "TNG000001=a"}.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(SystemExit) as code:
        cli.main(["check-config"])
    out = capsys.readouterr().out
    assert code.value.code == 0 and "ok" in out and "hunter2" not in out and "TNG000001 site=a" in out
    assert "pushes to:\n  local http://victoriametrics:8428" in out
    secret.write_text("")
    with pytest.raises(SystemExit) as code:
        cli.main(["check-config"])
    assert code.value.code == 2


def test_check_config_reports_a_password_file_that_is_not_utf8(tmp_path, monkeypatch, capsys):
    secret = tmp_path / "pw"
    secret.write_bytes("geheim-Größe".encode("latin-1"))
    for k, v in {"TING_USERNAME": "a@example.org", "TING_PASSWORD_FILE": str(secret), "TING_OUTBOX_DIR": str(tmp_path / "o"),
                 "TING_RECORD_DIR": str(tmp_path / "r"), "TING_STATE_FILE": str(tmp_path / "state.json")}.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(SystemExit) as code:
        cli.main(["check-config"])
    out = capsys.readouterr().out
    assert code.value.code == 2 and "not UTF-8" in out and "geheim" not in out


def test_check_config_reports_the_cursor_of_a_target_that_is_gone(tmp_path, monkeypatch, capsys):
    secret = tmp_path / "pw"
    secret.write_text("hunter2-secret")
    (tmp_path / "outbox").mkdir()
    (tmp_path / "outbox" / "cursor-peer").write_text("3 10\n")
    hook = tmp_path / "hook"
    hook.write_text("https://ha.example.org/api/webhook/abc")
    for k, v in {"TING_USERNAME": "a@example.org", "TING_PASSWORD_FILE": str(secret), "TING_OUTBOX_DIR": str(tmp_path / "outbox"),
                 "TING_RECORD_DIR": str(tmp_path / "r"), "TING_STATE_FILE": str(tmp_path / "state.json"),
                 "TING_VM_URLS": "local=http://vm:8428,remote=http://192.0.2.10:8428",
                 "TING_HEALTH_CHECKS": "vm=http://vm:8428/health", "TING_ALERT_WEBHOOK_FILE": str(hook)}.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(SystemExit) as code:
        cli.main(["check-config"])
    out = capsys.readouterr().out
    assert code.value.code == 0 and "a cursor for peer, which is no longer a push target" in out
    assert "health checks (notify after 300 s): vm http://vm:8428/health" in out and "abc" not in out
    hook.write_text("not a url")
    with pytest.raises(SystemExit) as code:
        cli.main(["check-config"])
    assert code.value.code == 2 and "holds no http(s) URL" in capsys.readouterr().out


@pytest.mark.parametrize("level", ["verbose", " debug", "info "])
def test_a_bad_or_padded_log_level_is_a_config_problem_not_a_crash(tmp_path, monkeypatch, capsys, level):
    """logging was set up from the raw variable before the config was checked, so every command crashed."""
    secret = tmp_path / "pw"
    secret.write_text("hunter2-secret")
    for k, v in {"TING_USERNAME": "a@example.org", "TING_PASSWORD_FILE": str(secret), "TING_OUTBOX_DIR": str(tmp_path / "o"),
                 "TING_RECORD_DIR": str(tmp_path / "r"), "TING_STATE_FILE": str(tmp_path / "state.json"),
                 "TING_LOG_LEVEL": level}.items():
        monkeypatch.setenv(k, v)
    with pytest.raises(SystemExit) as code:
        cli.main(["check-config"])
    out = capsys.readouterr().out
    if level == "verbose":
        assert code.value.code == 2 and "TING_LOG_LEVEL must be" in out
    else:
        assert code.value.code == 0 and "ok" in out  # Config strips and upper-cases it
    cli.main(["rules"])
    assert "ting:voltage_volts:count_1m" in capsys.readouterr().out
    logging.getLogger().handlers.clear()


def test_files_may_follow_the_options(tmp_path, capsys):
    """Python 3.11, 3.12.0-3.12.6 and 3.13.0 rejected files after options, as the README writes import."""
    path = tmp_path / "ting-20260310T20.jsonl"
    path.write_text("")
    cli.main(["replay", "--dry-run", "--from", "2026-03-10T20:00", str(path)])
    assert "1 files" in capsys.readouterr().err
    logging.getLogger().handlers.clear()


def test_rules_command(capsys):
    cli.main(["rules"])
    assert capsys.readouterr().out == rules.render()


def test_replay_dry_run_command(monkeypatch, capsys):
    monkeypatch.setenv("TING_SITES", "TNG000001=a,TNG000002=b")
    cli.main(["replay", "--dry-run", *map(str, CAPTURE)])
    assert capsys.readouterr().out == (FIXTURES / "golden-rollups.csv").read_text()


def test_import_without_files_is_an_error():
    with pytest.raises(SystemExit):
        cli.main(["import", "--dry-run"])


def test_unknown_command_and_version(capsys):
    with pytest.raises(SystemExit) as code:
        cli.main(["frobnicate"])
    assert code.value.code == 2
    cli.main(["--version"])
    assert capsys.readouterr().out.strip() == "3.0.0"


async def test_supervisor_restarts_a_crashed_task():
    stop, restarts, runs = asyncio.Event(), Counter(), []

    async def flaky():
        runs.append(1)
        if len(runs) < 3:
            raise RuntimeError("boom")
        stop.set()

    from ting_exporter.clock import ScaledClock

    await asyncio.wait_for(supervise("flaky", flaky, stop, restarts, ScaledClock(1000)), 2)
    assert restarts["flaky"] == 2 and len(runs) == 3


def test_log_scrubber_masks_registered_secrets():
    stream = io.StringIO()
    logs.setup("INFO", "json", stream)
    logs.register_secret("tok-very-secret")
    logging.getLogger("x").error("failed with token %s", "tok-very-secret")
    try:
        raise ValueError("tok-very-secret in an exception")
    except ValueError:
        logging.getLogger("x").exception("oops")
    logging.getLogger().handlers.clear()
    text = stream.getvalue()
    assert "tok-very-secret" not in text and text.count(logs.MASK) >= 2


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_secret_with_quotes_and_backslashes_is_masked_in_every_form(fmt):
    """the JSON formatter scrubbed after json.dumps, which escapes " and \\, and the repr form was never masked."""
    import json as jsonlib

    secret = 'pw-a"b\\c\'d-ü'
    stream = io.StringIO()
    logs.setup("INFO", fmt, stream)
    logs.register_secret(secret)
    log = logging.getLogger("x")
    log.error("plain %s", secret)
    log.error("repr %r", secret)
    log.error("in a dict %s", {"password": secret})
    try:
        raise ValueError(secret)
    except ValueError:
        log.exception("oops")
    logging.getLogger().handlers.clear()
    text = stream.getvalue()
    for form in (secret, repr(secret)[1:-1], jsonlib.dumps(secret)[1:-1], jsonlib.dumps(repr(secret)[1:-1])[1:-1], "a\"b"):
        assert form not in text, form
    assert text.count(logs.MASK) >= 4


def test_a_renewed_token_replaces_the_old_one_in_the_scrubber():
    """F-6: every renewed access token stayed registered for the life of the process (and the list was sorted per
    log line); a token that has been replaced is forgotten, the current one and the password are kept."""
    logs.register_secret("old-token-1234")
    logs.retire_secret("old-token-1234")
    logs.register_secret("new-token-5678")
    assert logs.scrub("x old-token-1234") == "x old-token-1234" and logs.scrub("x new-token-5678") == f"x {logs.MASK}"


def test_config_repr_masks_the_username_local_part():
    from ting_exporter.config import Config

    for name, shown in (("x@example.org", "x***@example.org"), ("someone@example.com", "s***@example.com"), ("qwzx", "q***")):
        text = repr(Config(username=name))
        assert f"username='{shown}'" in text and name not in text


def test_probe_notifications_prints_the_history(monkeypatch, tmp_path, capsys):
    import asyncio

    from aiohttp import web

    from ting_exporter import probe as serve
    from ting_exporter.config import Config

    from . import fakes

    async def run():
        cognito_fake = fakes.FakeCognito("pw-123456")
        app = web.Application()
        app.router.add_post("/cognito/", cognito_fake.handle)
        app.router.add_get("/api/v1/Users/{user_id}", fakes.fake_users)
        app.router.add_get("/api/v1/Notifications/history/{user_id}", fakes.fake_notifications)
        runner, base = await fakes.start_app(app)
        secret = tmp_path / "pw"
        secret.write_text("pw-123456")
        cfg = Config.from_env({"TING_USERNAME": "me@example.com", "TING_PASSWORD_FILE": str(secret),
                               "TING_SITES": f"{fakes.SERIAL_A}=home,{fakes.SERIAL_B}=cabin",
                               "TING_COGNITO_URL": f"{base}/cognito/", "TING_API_URL": base,
                               "TING_RECORD_DIR": "off"})
        try:
            await serve.notifications(cfg, raw=True)
        finally:
            await runner.cleanup()

    asyncio.run(run())
    out = capsys.readouterr().out
    lines = [l for l in out.splitlines() if "CommunityPowerOutage" in l and "site=" in l]
    assert "3 notifications" in out and lines and "site=home" in lines[0] and "Community Power Outage" in lines[0]
    assert out.index("PowerRestored  ") < out.index("CommunityPowerOutage  ") < out.index("Sag  ")  # newest first
    assert '"isCleared": true' in out  # --raw JSON


def test_probe_voltage_history_summarises_the_cloud_history(tmp_path, capsys):
    import asyncio

    from aiohttp import web

    from ting_exporter import probe as serve
    from ting_exporter.config import Config
    from ting_exporter.recorder import parse_when

    from . import fakes

    async def run():
        cognito_fake = fakes.FakeCognito("pw-123456")
        app = web.Application()
        app.router.add_post("/cognito/", cognito_fake.handle)
        app.router.add_get("/api/v3/Devices/{serial}/voltage/dateRange", fakes.fake_voltage_history)
        runner, base = await fakes.start_app(app)
        secret = tmp_path / "pw"
        secret.write_text("pw-123456")
        cfg = Config.from_env({"TING_USERNAME": "me@example.com", "TING_PASSWORD_FILE": str(secret),
                               "TING_SITES": f"{fakes.SERIAL_A}=home", "TING_COGNITO_URL": f"{base}/cognito/",
                               "TING_API_URL": base, "TING_RECORD_DIR": "off"})
        try:
            await serve.voltage_history(cfg, parse_when("2026-03-10T03:00:00Z"), parse_when("2026-03-12T02:00:00Z"))
        finally:
            await runner.cleanup()

    asyncio.run(run())
    out = capsys.readouterr().out
    assert "unit ['V']" in out and "6 points" in out  # two 24 h requests, 3 points each
    assert "spacing between points (s):" in out and "sampleCount" in out


def test_notification_interval_is_off_or_at_least_30_s():
    import pytest

    from ting_exporter.config import Config, ConfigError

    for ok in ("0", "30", "60", "3600"):
        assert Config.from_env({"TING_NOTIFICATIONS_INTERVAL_SECONDS": ok}, need_credentials=False).notifications_interval == float(ok)
    for bad in ("1", "0.5", "29"):
        with pytest.raises(ConfigError, match="TING_NOTIFICATIONS_INTERVAL_SECONDS"):
            Config.from_env({"TING_NOTIFICATIONS_INTERVAL_SECONDS": bad}, need_credentials=False)


def test_probe_rest_prints_what_the_registry_reads(tmp_path, capsys):
    from aiohttp import web

    from ting_exporter import probe
    from ting_exporter.config import Config

    from . import fakes

    async def run():
        cognito_fake = fakes.FakeCognito("pw-123456")
        app = web.Application()
        app.router.add_post("/cognito/", cognito_fake.handle)
        app.router.add_get("/api/v1/Users/{user_id}", fakes.fake_users)
        app.router.add_get("/api/v1/Users/{user_id}/conditions", fakes.fake_conditions)
        app.router.add_get("/api/v1/FrozenPipe/{serial}", fakes.fake_frozen_pipe)
        runner, base = await fakes.start_app(app)
        secret = tmp_path / "pw"
        secret.write_text("pw-123456")
        cfg = Config.from_env({"TING_USERNAME": "me@example.com", "TING_PASSWORD_FILE": str(secret),
                               "TING_SITES": f"{fakes.SERIAL_A}=a,{fakes.SERIAL_B}=b", "TING_COGNITO_URL": f"{base}/cognito/",
                               "TING_API_URL": base, "TING_RECORD_DIR": "off"})
        try:
            await probe.rest_values(cfg, raw=True)
        finally:
            await runner.cleanup()

    asyncio.run(run())
    out = capsys.readouterr().out
    assert "/api/v1/Users/{user_id}/conditions: ok" in out and f"/api/v1/FrozenPipe/{fakes.SERIAL_A}: GET" in out
    assert "ting_outdoor_temperature_celsius     -3.25" in out and "ting_frozen_pipe_level               55.0" in out
    assert "person@example.org" not in out  # the account record is shown by its devices only
