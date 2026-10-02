"""External health checks with their own notification path (design 0.3).

Every other alert goes VictoriaMetrics -> vmalert -> Alertmanager -> Home
Assistant. If one of those is down on the notifying host, or the whole host
is, nothing alerts. This task sits outside that chain: every CHECK_INTERVAL it
GETs each configured health URL (TING_HEALTH_CHECKS, e.g. the local
VictoriaMetrics /health, vmalert /health, Alertmanager /-/healthy, and the
peer's VictoriaMetrics over the tunnel), and when one has failed for
TING_HEALTH_FOR_SECONDS it POSTs straight to the Home Assistant webhook
(TING_ALERT_WEBHOOK_FILE), in Alertmanager's webhook format, so the same
automation shows it. A resolved message follows when the check passes again;
a firing one is repeated every REPEAT while it lasts. A notification that
cannot be delivered is tried again at the next check.

The webhook URL is a secret: it is read from its file when needed, registered
with the log scrubber, and never logged.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

from . import logs
from .clock import Clock

log = logging.getLogger(__name__)

CHECK_INTERVAL = 60.0
CHECK_TIMEOUT = 10.0
POST_TIMEOUT = 15.0
REPEAT = 12 * 3600.0
ALERT_NAME = "TingHealthCheckFailing"
NEVER = "0001-01-01T00:00:00Z"


@dataclass
class Check:
    name: str
    url: str
    up: bool | None = None
    failing_since: float | None = None  # wall clock
    reason: str = ""
    firing: bool = False  # a firing notification went out
    last_sent: float = 0.0
    owe_resolved: bool = False  # it recovered, the resolved notification is not delivered yet
    resolved_from: float | None = None  # when the failure that the owed resolved notification ends began


def _iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class HealthWatch:
    def __init__(self, session: aiohttp.ClientSession, checks: list[tuple[str, str]], webhook_file: Path, *,
                 host: str, role: str, for_seconds: float = 300.0, clock: Clock | None = None) -> None:
        self.session = session
        self.checks = [Check(name, url) for name, url in checks]
        self.webhook_file = webhook_file
        self.host, self.role = host, role
        self.for_seconds = for_seconds
        self.clock = clock or Clock()
        self.notifications: Counter[str] = Counter()  # ok | error
        self._webhook_error_logged = False

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.check_once()
            try:
                await asyncio.wait_for(stop.wait(), self.clock.real(CHECK_INTERVAL))
            except asyncio.TimeoutError:
                pass

    async def check_once(self) -> None:
        results = await asyncio.gather(*(self._probe(c) for c in self.checks))
        now = self.clock.time()
        for check, (ok, reason) in zip(self.checks, results):
            if ok:
                if check.up is False:
                    log.info("health check %s passes again", check.name)
                if check.firing:
                    check.firing, check.owe_resolved, check.resolved_from = False, True, check.failing_since
                check.up, check.failing_since, check.reason = True, None, ""
            else:
                if check.up is not False:
                    log.warning("health check %s failed: %s", check.name, reason)
                    check.failing_since = now
                check.up, check.reason = False, reason
                due = now - (check.failing_since or now) >= self.for_seconds
                if due and (not check.firing or now - check.last_sent >= REPEAT):
                    if await self._send(check, "firing", now):
                        check.firing, check.last_sent, check.owe_resolved = True, now, False
                continue
            if check.owe_resolved and await self._send(check, "resolved", now):
                check.owe_resolved = False

    async def _probe(self, check: Check) -> tuple[bool, str]:
        try:
            async with self.session.get(check.url, timeout=aiohttp.ClientTimeout(total=CHECK_TIMEOUT)) as resp:
                await resp.read()
                if 200 <= resp.status < 300:
                    return True, ""
                return False, f"HTTP {resp.status}"
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
            return False, type(err).__name__
        except Exception as err:  # never let a check end the task
            return False, f"{type(err).__name__}"

    def payload(self, check: Check, status: str, now: float) -> dict:
        labels = {"alertname": ALERT_NAME, "check": check.name, "host": self.host, "role": self.role,
                  "severity": "critical"}
        since = (check.resolved_from if status == "resolved" else check.failing_since) or now
        minutes = (now - since) / 60
        summary = (f"{self.host}: health check {check.name} ({check.url}) failing for {minutes:.0f} min ({check.reason}); "
                   "alerts through VictoriaMetrics may not arrive" if status == "firing"
                   else f"{self.host}: health check {check.name} ({check.url}) passes again")
        alert = {"status": status, "labels": labels, "annotations": {"summary": summary},
                 "startsAt": _iso(since), "endsAt": _iso(now) if status == "resolved" else NEVER,
                 "generatorURL": "", "fingerprint": f"ting-health-{self.host}-{check.name}"}
        return {"version": "4", "groupKey": f"ting-exporter-health/{self.host}", "truncatedAlerts": 0, "status": status,
                "receiver": "ting-exporter", "groupLabels": {"alertname": ALERT_NAME},
                "commonLabels": labels, "commonAnnotations": alert["annotations"], "externalURL": "", "alerts": [alert]}

    async def _send(self, check: Check, status: str, now: float) -> bool:
        try:
            url = self.webhook_file.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError) as err:
            self.notifications["error"] += 1
            if not self._webhook_error_logged:
                self._webhook_error_logged = True
                log.error("health check %s: cannot notify, the webhook file %s is unreadable (%s)", check.name,
                          self.webhook_file, type(err).__name__)
            return False
        logs.register_secret(url)
        if not url.startswith(("http://", "https://")):
            self.notifications["error"] += 1
            log.error("health check %s: the webhook file %s holds no http(s) URL", check.name, self.webhook_file)
            return False
        self._webhook_error_logged = False
        try:
            async with self.session.post(url, data=json.dumps(self.payload(check, status, now)),
                                         headers={"Content-Type": "application/json"},
                                         timeout=aiohttp.ClientTimeout(total=POST_TIMEOUT)) as resp:
                await resp.read()
                ok = 200 <= resp.status < 300
                why = f"HTTP {resp.status}"
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
            ok, why = False, type(err).__name__
        self.notifications["ok" if ok else "error"] += 1
        if ok:
            log.warning("health check %s: sent %s to Home Assistant", check.name, status)
        else:
            log.error("health check %s: the %s notification failed (%s); trying again at the next check", check.name, status, why)
        return ok
