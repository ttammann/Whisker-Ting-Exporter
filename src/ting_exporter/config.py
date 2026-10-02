"""Configuration from environment variables: parsed, validated, and safe to print (design 7.6).

Config.from_env() raises ConfigError with every problem at once, never exits,
and reads no secrets: the password and webhook files are only checked for
readability here and read when they are needed.
"""

from __future__ import annotations

import os
import re
import socket
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from pathlib import Path
from urllib.parse import urlsplit

from .auth import cognito
from .cloud import api, hub

SERIAL = re.compile(r"^[A-Za-z0-9]{4,32}$")
NAME = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")  # sites, target and check names, the role and the host
_TRUE, _FALSE = ("1", "true", "yes", "on"), ("0", "false", "no", "off")
DEFAULT_VM_URL = "http://victoriametrics:8428"
DEFAULT_PASSWORD_FILE = "/run/secrets/ting/ting_password"
DEFAULT_WEBHOOK_FILE = "/run/secrets/alert/alert_webhook_url"
RESERVED_TARGETS = ("rejected", "inbox")  # directory names in the outbox


class ConfigError(ValueError):
    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


@dataclass(frozen=True)
class Config:
    username: str = ""
    password_file: Path = Path(DEFAULT_PASSWORD_FILE)
    serials: tuple[str, ...] = ()
    sites: dict[str, str] = field(default_factory=dict)
    vm_targets: tuple[tuple[str, str], ...] = (("local", DEFAULT_VM_URL),)  # (name, base URL)
    role: str = "primary"
    host: str = "localhost"
    release_others: bool = False
    push_interval: float = 5.0
    outbox_dir: Path = Path("/data/outbox")
    outbox_max_bytes: int = 268_435_456
    outbox_replay_new: bool = False
    state_file: Path | None = Path("/data/state.json")
    stale: float = 60.0
    timestamp_source: str = "device"
    auth_hold_seconds: float = 21_600.0
    record_dir: Path | None = Path("/data/raw")
    record_retention_days: float = 90.0
    notifications_interval: float = 60.0
    rest_interval: float = 300.0
    repair_interval: float = 300.0
    health_checks: tuple[tuple[str, str], ...] = ()  # (name, URL)
    health_for: float = 300.0
    alert_webhook_file: Path = Path(DEFAULT_WEBHOOK_FILE)
    listen_host: str = "0.0.0.0"
    listen_port: int = 9786
    log_level: str = "INFO"
    log_format: str = "text"
    cognito_url: str = cognito.ENDPOINT
    api_url: str = api.BASE_URL
    hub_url: str = hub.HUB_URL

    @property
    def vm_url(self) -> str:
        """The first push target (the local store: replay's, mark's and repair's default)."""
        return self.vm_targets[0][1]

    @property
    def streamed_serials(self) -> tuple[str, ...]:
        """TING_SERIALS if set, else the serials of TING_SITES; empty means all on the account."""
        return self.serials or tuple(self.sites)

    def site_serial(self, site: str) -> str | None:
        """The sensor of a site (one sensor per site, design 5.1)."""
        return next((serial for serial, s in self.sites.items() if s == site), None)

    def __repr__(self) -> str:
        parts = []
        for f in fields(self):
            value = getattr(self, f.name)
            if f.name == "username" and value:
                local, at, domain = value.partition("@")
                value = local[:1] + "***" + at + domain
            parts.append(f"{f.name}={value!r}")
        return f"Config({', '.join(parts)})"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, *, need_credentials: bool = True) -> Config:
        env = os.environ if env is None else env
        problems: list[str] = []

        def text(name: str, default: str) -> str:
            return env.get(name, default).strip()

        def number(name: str, default: float, lo: float, hi: float, integer: bool = False) -> float:
            raw = text(name, "")
            if not raw:
                return default
            try:
                value = int(raw) if integer else float(raw)
            except ValueError:
                problems.append(f"{name}={raw!r} is not a{'n integer' if integer else ' number'}")
                return default
            if not lo <= value <= hi:
                problems.append(f"{name}={raw} is outside {lo:g}..{hi:g}")
                return default
            return value

        def interval(name: str, default: float, lo: float) -> float:
            value = number(name, default, 0, 86_400)
            if 0 < value < lo:
                problems.append(f"{name} must be 0 (off) or at least {lo:g}")
                return default
            return value

        def flag(name: str, default: bool) -> bool:
            raw = text(name, "").lower()
            if not raw:
                return default
            if raw not in _TRUE + _FALSE:
                problems.append(f"{name}={raw!r} must be true or false")
                return default
            return raw in _TRUE

        def optional_path(name: str, default: str) -> Path | None:
            raw = text(name, default)
            return Path(raw) if raw and raw.lower() not in ("off", "none", "-") else None

        def named(name: str) -> str:
            value = text(name, "")
            if value and not NAME.match(value):
                problems.append(f"{name}={value!r} must be 1-32 letters, digits, '_', '.' or '-'")
            return value

        if "TING_PASSWORD" in env:
            problems.append("TING_PASSWORD is not supported (it would show in docker inspect); use TING_PASSWORD_FILE")
        username = text("TING_USERNAME", "")
        password_file = Path(text("TING_PASSWORD_FILE", DEFAULT_PASSWORD_FILE))
        if need_credentials:
            if not username:
                problems.append("TING_USERNAME is required")
            if not os.access(password_file, os.R_OK):
                problems.append(f"TING_PASSWORD_FILE {password_file} is not readable")

        serials = tuple(s.strip() for s in text("TING_SERIALS", "").split(",") if s.strip())
        for s in serials:
            if not SERIAL.match(s):
                problems.append(f"TING_SERIALS: {s!r} does not look like a serial")
        sites: dict[str, str] = {}
        for pair in (p.strip() for p in text("TING_SITES", "").split(",") if p.strip()):
            serial, sep, site = (x.strip() for x in pair.partition("="))
            if not sep or not SERIAL.match(serial) or not NAME.match(site):
                problems.append(f"TING_SITES: {pair!r} is not serial=site")
                continue
            if site in sites.values():  # restart detection compares a site's own high and low (design 5.1)
                problems.append(f"TING_SITES: site {site!r} has two sensors; give the second its own site name")
            sites[serial] = site

        source = text("TING_TIMESTAMP_SOURCE", "device").lower()
        if source not in ("device", "arrival"):
            problems.append("TING_TIMESTAMP_SOURCE must be device or arrival")
        log_format = text("TING_LOG_FORMAT", "text").lower()
        if log_format not in ("text", "json"):
            problems.append("TING_LOG_FORMAT must be text or json")
        log_level = text("TING_LOG_LEVEL", "INFO").upper()
        if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            problems.append("TING_LOG_LEVEL must be DEBUG, INFO, WARNING or ERROR")
        host, _, port = text("TING_LISTEN", "0.0.0.0:9786").rpartition(":")
        if host.startswith("[") and host.endswith("]"):  # [::1]:9786
            host = host[1:-1]
        if not port.isdigit() or not 0 < int(port) < 65536:
            problems.append("TING_LISTEN must be host:port")
            port = "9786"
        if text("TING_VM_URL", ""):
            problems.append("TING_VM_URL is gone in v3: use TING_VM_URLS=local=<url>")
        if text("TING_SPOOL_DIR", "") or text("TING_QUEUE_MAX_SAMPLES", ""):
            problems.append("TING_SPOOL_DIR and TING_QUEUE_MAX_SAMPLES are gone in v3: the outbox replaces them "
                            "(TING_OUTBOX_DIR, TING_OUTBOX_MAX_BYTES)")
        vm_targets = _named_urls("TING_VM_URLS", text("TING_VM_URLS", "") or f"local={DEFAULT_VM_URL}", problems,
                                 reserved=RESERVED_TARGETS)
        checks = _named_urls("TING_HEALTH_CHECKS", text("TING_HEALTH_CHECKS", ""), problems, allow_empty=True)
        role = named("TING_ROLE") or "primary"
        host_name = named("TING_HOST") or _hostname()

        cfg = cls(
            username=username,
            password_file=password_file,
            serials=serials,
            sites=sites,
            vm_targets=vm_targets,
            role=role,
            host=host_name,
            release_others=flag("TING_RELEASE_OTHERS", False),
            push_interval=number("TING_PUSH_INTERVAL_SECONDS", 5.0, 1, 30),
            outbox_dir=Path(text("TING_OUTBOX_DIR", "/data/outbox")),
            outbox_max_bytes=int(number("TING_OUTBOX_MAX_BYTES", 268_435_456, 16 * 1_048_576, 2**40, integer=True)),
            outbox_replay_new=flag("TING_OUTBOX_REPLAY_NEW", False),
            state_file=optional_path("TING_STATE_FILE", "/data/state.json"),
            stale=number("TING_STALE_SECONDS", 60.0, 10, 3600),
            timestamp_source=source,
            auth_hold_seconds=number("TING_AUTH_HOLD_SECONDS", 21_600.0, 600, 7 * 86400),
            record_dir=optional_path("TING_RECORD_DIR", "/data/raw"),
            record_retention_days=number("TING_RECORD_RETENTION_DAYS", 90.0, 1, 3650),
            notifications_interval=interval("TING_NOTIFICATIONS_INTERVAL_SECONDS", 60.0, 30),
            rest_interval=interval("TING_REST_INTERVAL_SECONDS", 300.0, 60),
            repair_interval=interval("TING_REPAIR_INTERVAL_SECONDS", 300.0, 60),
            health_checks=checks,
            health_for=number("TING_HEALTH_FOR_SECONDS", 300.0, 60, 86_400),
            alert_webhook_file=Path(text("TING_ALERT_WEBHOOK_FILE", DEFAULT_WEBHOOK_FILE)),
            listen_host=host or "0.0.0.0",
            listen_port=int(port),
            log_level=log_level,
            log_format=log_format,
            cognito_url=text("TING_COGNITO_URL", cognito.ENDPOINT),
            api_url=text("TING_API_URL", api.BASE_URL).rstrip("/"),
            hub_url=text("TING_HUB_URL", hub.HUB_URL),
        )
        if problems:
            raise ConfigError(problems)
        return cfg


def _hostname() -> str:
    name = re.sub(r"[^A-Za-z0-9_.-]", "_", socket.gethostname().split(".")[0])[:32]
    return name or "localhost"


def _named_urls(var: str, value: str, problems: list[str], *, reserved: tuple[str, ...] = (),
                allow_empty: bool = False) -> tuple[tuple[str, str], ...]:
    """`name=url,...`; a bare URL is named after its host."""
    out: list[tuple[str, str]] = []
    for entry in (e.strip() for e in value.split(",") if e.strip()):
        name, sep, url = entry.partition("=")
        if not sep or "://" in name:
            url = entry
            try:
                host = urlsplit(url.strip()).hostname or ""
            except ValueError:
                host = ""
            name = re.sub(r"[^A-Za-z0-9_.-]", "_", host)
        name, url = name.strip(), url.strip().rstrip("/")
        if not url.startswith(("http://", "https://")):
            problems.append(f"{var} {entry!r}: the URL must start with http:// or https://")
        elif not NAME.match(name) or name.startswith(".") or name in reserved:
            problems.append(f"{var} {entry!r}: the name must be 1-32 letters, digits, '_', '.' or '-', not start "
                            f"with '.'" + (f", and not be {' or '.join(map(repr, reserved))}" if reserved else ""))
        elif name in (n for n, _ in out):
            problems.append(f"{var}: the name {name!r} is used twice")
        else:
            out.append((name, url))
    if not out and not allow_empty:
        problems.append(f"{var} has no entry")
        out.append(("local", DEFAULT_VM_URL))
    return tuple(out)
