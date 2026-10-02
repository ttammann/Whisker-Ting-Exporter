"""External health checks (design 0.3): a failing /health reaches Home Assistant without VictoriaMetrics or Alertmanager."""

import io
import logging

import aiohttp
from aiohttp import web

from ting_exporter import logs
from ting_exporter.clock import Clock
from ting_exporter.health import ALERT_NAME, HealthWatch

from . import fakes


class StepClock(Clock):
    def __init__(self):
        self.t = 1_790_600_000.0

    def time(self):
        return self.t

    def monotonic(self):
        return self.t


async def test_failing_then_resolved(tmp_path):
    state = {"vm": 200}
    posts = []

    async def vm_health(_request):
        return web.Response(status=state["vm"], text="OK")

    async def webhook(request):
        posts.append(await request.json())
        return web.Response(status=200)

    app = web.Application()
    app.router.add_get("/health", vm_health)
    app.router.add_post("/api/webhook/secret-id-123", webhook)
    runner, base = await fakes.start_app(app)
    hook = tmp_path / "alert_webhook_url"
    hook.write_text(f"{base}/api/webhook/secret-id-123\n")
    stream = io.StringIO()
    logs.setup("INFO", "text", stream)
    clock = StepClock()
    async with aiohttp.ClientSession() as session:
        watch = HealthWatch(session, [("vm", f"{base}/health"), ("gone", "http://127.0.0.1:9/health")], hook,
                            host="host-b", role="secondary", for_seconds=300, clock=clock)
        await watch.check_once()
        assert [c.up for c in watch.checks] == [True, False] and posts == []
        state["vm"] = 503
        clock.t += 1
        for _ in range(4):
            await watch.check_once()
            clock.t += 60
        assert posts == []  # failing for 240 s: not yet
        clock.t += 61
        await watch.check_once()
        firing = {p["alerts"][0]["labels"]["check"] for p in posts}
        assert firing == {"vm", "gone"} and all(p["status"] == "firing" for p in posts)
        clock.t += 60
        await watch.check_once()
        assert len(posts) == 2  # once, then every 12 h
        state["vm"] = 200
        clock.t += 60
        await watch.check_once()
    await runner.cleanup()
    logging.getLogger().handlers.clear()
    resolved = [p for p in posts if p["status"] == "resolved"]
    assert len(resolved) == 1 and resolved[0]["alerts"][0]["labels"] == {
        "alertname": ALERT_NAME, "check": "vm", "host": "host-b", "role": "secondary", "severity": "critical"}
    assert resolved[0]["alerts"][0]["endsAt"] != "0001-01-01T00:00:00Z" and "passes again" in resolved[0]["alerts"][0]["annotations"]["summary"]
    fired = next(p for p in posts if p["status"] == "firing" and p["alerts"][0]["labels"]["check"] == "vm")
    assert resolved[0]["alerts"][0]["startsAt"] == fired["alerts"][0]["startsAt"]  # when it began, not when it ended
    assert "secret-id-123" not in stream.getvalue() and watch.notifications["ok"] == 3


async def test_an_undeliverable_notification_is_tried_again(tmp_path):
    clock = StepClock()
    async with aiohttp.ClientSession() as session:
        watch = HealthWatch(session, [("gone", "http://127.0.0.1:9/health")], tmp_path / "missing", host="h", role="primary",
                            for_seconds=60, clock=clock)
        for _ in range(3):
            await watch.check_once()
            clock.t += 61
    assert watch.notifications["error"] >= 2 and not watch.checks[0].firing
