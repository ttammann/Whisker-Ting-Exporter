"""HubSession against FakeHub (recording replay) on a scaled clock.

The clock runs SCALE times faster than real time and FakeHub replays the
capture at the same speed, so 90 s of protocol time take about 2 s here and
arrival times line up with the recorded device timestamps.
"""

import asyncio

import aiohttp
import pytest
from aiohttp import web

from ting_exporter.auth.cognito import AuthError, Identity
from ting_exporter.auth.identity import IdentityManager, SecretFile
from ting_exporter.clock import ScaledClock
from ting_exporter.cloud import hub as hubmod
from ting_exporter.cloud.hub import HubSession, SessionState
from ting_exporter.pipeline import Pipeline

from . import fakes

SCALE = 40.0
ELEMENTS = {"ComboBinaryData", "frequency", "thdMin", "thdAvg", "thdMax"}


class Harness:
    def __init__(self, hub: fakes.FakeHub, base: str, tmp_path, scale: float = SCALE, password: str = "right"):
        self.hub = hub
        self.rows = hub.rows
        self.clock = ScaledClock(scale, start_wall=hub.rows[0]["t"])
        self.pipeline = Pipeline({fakes.SERIAL_B: "b", fakes.SERIAL_A: "a"})
        self.events: list[tuple[float, str, dict]] = []
        self.primaries: list[float] = []
        self.targets: dict[str, int] = {}
        self.cognito_calls: list[str] = []
        secret = tmp_path / "pw"
        secret.write_text(password)
        self.identities = IdentityManager(None, "me", SecretFile(secret), clock=self.clock, sign_in=self._sign_in, refresh=self._refresh)
        self.base = base
        self.stop = asyncio.Event()

    async def _sign_in(self, session, username, password, endpoint):
        self.cognito_calls.append("srp")
        if password != "right":
            raise AuthError("rejected", "NotAuthorizedException")
        return Identity(fakes.USER_ID, fakes.API_KEY, fakes.ACCESS_TOKEN, fakes.REFRESH_TOKEN)

    async def _refresh(self, session, token, endpoint):
        self.cognito_calls.append("refresh")
        return Identity(fakes.USER_ID, fakes.API_KEY, fakes.ACCESS_TOKEN, token)

    def on_invocation(self, serial, target, args, arrival):
        self.targets[target] = self.targets.get(target, 0) + 1
        _, primary = self.pipeline.process(serial, target, args, arrival)
        if primary:
            self.primaries.append(self.clock.monotonic())
        return primary

    def on_event(self, serial, kind, fields, t):
        self.events.append((self.clock.monotonic(), kind, fields))

    def kinds(self, kind):
        return [e for e in self.events if e[1] == kind]

    async def run(self, serial, until, timeout=10.0, stale=60.0, release=True):
        """Run one session until `until()` is true (checked every 20 ms real), then stop it."""
        async with aiohttp.ClientSession() as session:
            state = SessionState(serial, "b")
            s = HubSession(session, state, self.identities, self.on_invocation, on_event=self.on_event,
                           stale=stale, hub_url=f"{self.base.replace('http', 'ws')}/dataHub", clock=self.clock,
                           release=release)
            task = asyncio.create_task(s.run(self.stop))
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            while not until() and loop.time() < deadline:
                await asyncio.sleep(0.02)
            self.stop.set()
            await asyncio.wait_for(task, 5)
            return state


@pytest.fixture
async def server():
    holder = {}

    async def handle(request):
        return await holder["hub"].handle(request)

    app = web.Application()
    app.router.add_get("/dataHub", handle)
    runner, base = await fakes.start_app(app)
    yield holder, base
    await runner.cleanup()


async def test_subscribe_order_stream_and_release(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE)
    h = Harness(hub, base, tmp_path)
    state = await h.run(fakes.SERIAL_B, lambda: len(h.primaries) >= 200)

    calls = [(t, e) for t, _, e in hub.calls]
    assert set(calls[:5]) == {("UnInitializeStreaming", e) for e in ELEMENTS}  # release all five first
    assert calls[5] == ("InitializeStreaming", "ComboBinaryData")
    assert set(calls[6:10]) == {("InitializeStreaming", e) for e in ELEMENTS - {"ComboBinaryData"}}
    assert {e for t, e in calls[10:] if t == "UnInitializeStreaming"} == ELEMENTS  # released on shutdown
    assert hub.headers[0]["x-wl-api-key"] == fakes.API_KEY and hub.headers[0]["Origin"] == "ionic://localhost"

    assert set(h.targets) == {"updateComboBinaryData", "updateGraphMultiCategorical", "updateGraphMulti"}
    st = h.pipeline.sensors[fakes.SERIAL_B]
    assert st.timing.fallbacks == 0 and not st.discarded  # arrival times line up with device time
    assert state.connects == {"ok": 1} and state.disconnects == {"shutdown": 1}
    assert h.cognito_calls == ["srp"]


async def test_90s_silence_gives_one_reconnect_at_60s(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE, silent_after=300)  # every connection goes quiet after 300 messages
    h = Harness(hub, base, tmp_path)

    def silence_start():
        ended = h.kinds("ended")
        return max(p for p in h.primaries if p < ended[0][0]) if ended else None

    await h.run(fakes.SERIAL_B, lambda: silence_start() is not None and h.clock.monotonic() >= silence_start() + 90, timeout=8)
    start = silence_start()
    ended = [e for e in h.kinds("ended") if e[0] <= start + 90]
    assert len(ended) == 1 and ended[0][2]["reason"] == "stale"
    assert 60 <= ended[0][0] - start <= 61
    assert hub.connections == 2  # exactly one reconnect in the 90 s


async def test_sensor_outage_retries_every_minute_and_catches_the_return(server, tmp_path):
    # a site without power: the hub accepts every subscription but sends nothing for an hour
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=200, silent_after=0)
    h = Harness(hub, base, tmp_path, scale=200)
    back = {}

    def until():
        if len(h.kinds("ended")) >= 5 and "at" not in back:
            hub.silent_after = None  # power and network are back
            back["at"] = h.clock.monotonic()
        return bool(back) and any(p > back["at"] for p in h.primaries)

    await h.run(fakes.SERIAL_B, until, timeout=10)
    assert all(e[2]["reason"] == "stale" for e in h.kinds("ended"))
    assert all(e[2]["seconds"] <= 6 for e in h.kinds("backoff"))  # no growth to 5 min
    first = min(p for p in h.primaries if p > back["at"])
    assert first - back["at"] <= 70  # at most the rest of one 60 s stale wait plus the 5 s retry
    assert h.cognito_calls == ["srp"]


async def test_30s_silence_gives_no_reconnect(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE, pause=(300, 30.0))
    h = Harness(hub, base, tmp_path)
    await h.run(fakes.SERIAL_B, lambda: len(h.primaries) >= 250, timeout=8)
    assert max(b - a for a, b in zip(h.primaries, h.primaries[1:])) >= 29  # the pause happened
    assert h.kinds("ended") == [] and hub.connections == 1


async def test_refusal_renews_once_then_backs_off_5_min(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=100, refuse=True)
    h = Harness(hub, base, tmp_path, scale=100)
    state = await h.run(fakes.SERIAL_B, lambda: len(h.kinds("backoff")) >= 2, timeout=5)
    delays = [e[2]["seconds"] for e in h.kinds("backoff")]
    assert delays[0] <= 6 and delays[1] >= 300
    assert h.cognito_calls == ["srp", "refresh"]  # one renewal, not a sign-in per refusal
    assert state.connects["refused"] == 2 and hub.connections == 2


async def test_a_refusal_without_a_reason_is_still_a_refusal(server, tmp_path):
    """`if error:` took an error Completion with an empty text for a successful subscription."""
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=100, refuse=True, refuse_reason="")
    h = Harness(hub, base, tmp_path, scale=100)
    state = await h.run(fakes.SERIAL_B, lambda: bool(h.kinds("refused")), timeout=5)
    assert state.connects["refused"] >= 1 and state.connects["ok"] == 0


async def test_refusal_after_a_good_session_renews_again(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=100, refuse=True, close_after=40)
    h = Harness(hub, base, tmp_path, scale=100)

    def until():
        refused, subscribed = h.kinds("refused"), h.kinds("subscribed")
        if len(refused) == 1 and hub.refuse and not subscribed:
            hub.refuse = False  # the renewal fixed it
        elif len(refused) == 1 and subscribed and not hub.refuse:
            hub.refuse = True  # later (the session ends by server Close), refused again
        return len(refused) >= 2 and len(h.cognito_calls) >= 3  # the second refusal renewed too

    await h.run(fakes.SERIAL_B, until, timeout=5)
    second = h.kinds("refused")[1][0]
    delay = next(e[2]["seconds"] for e in h.kinds("backoff") if e[0] > second)
    assert delay <= 6  # a fresh streak: renew and retry soon, not the 5 min back-off
    assert h.cognito_calls == ["srp", "refresh", "refresh"]


async def test_server_close_and_socket_drop_reconnect(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE, close_after=40)
    h = Harness(hub, base, tmp_path, scale=200)
    state = await h.run(fakes.SERIAL_A, lambda: hub.connections >= 2, timeout=5)
    assert state.disconnects["server_close"] >= 1

    hub = holder["hub"] = fakes.FakeHub(speed=SCALE, drop_after=40)
    h = Harness(hub, base, tmp_path, scale=200)
    state = await h.run(fakes.SERIAL_A, lambda: hub.connections >= 2, timeout=5)
    assert state.disconnects["ws_error"] >= 1
    assert h.cognito_calls == ["srp"]  # reconnects reuse the identity


async def test_a_close_while_subscribing_ends_the_session_at_once_with_its_reason(server, tmp_path):
    """the pending subscription used to wait out INVOKE_TIMEOUT (10 s) after the receiver had died, and the
    session ended as a TimeoutError with the hub's reason lost."""
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE, close_on_subscribe="Server shutting down")
    h = Harness(hub, base, tmp_path, scale=1.0)  # real time: the old stall would take 10 s per attempt
    loop = asyncio.get_running_loop()
    started = loop.time()
    state = await h.run(fakes.SERIAL_B, lambda: bool(h.kinds("ended")), timeout=15)
    assert loop.time() - started < 3
    assert state.disconnects["server_close"] == 1 and "Server shutting down" in h.kinds("ended")[0][2]["error"]


async def test_bad_frame_is_counted_not_fatal(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE, garbage_at=5)
    h = Harness(hub, base, tmp_path)
    state = await h.run(fakes.SERIAL_B, lambda: len(h.primaries) >= 50)
    assert state.errors["protocol_error"] == 1 and hub.connections == 1


async def test_optional_streams_refused_voltage_still_flows(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE, optional_ok=False)
    h = Harness(hub, base, tmp_path)
    state = await h.run(fakes.SERIAL_B, lambda: len(h.primaries) >= 50)
    assert len(h.kinds("optional_refused")) == 4 and state.disconnects == {"shutdown": 1}


async def test_rejected_password_never_connects(server, tmp_path, monkeypatch):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE)
    h = Harness(hub, base, tmp_path, scale=1000, password="wrong")
    await h.run(fakes.SERIAL_B, lambda: h.clock.monotonic() > 1200, timeout=6)
    assert h.cognito_calls == ["srp"] and hub.connections == 0
    assert h.kinds("auth_wait")


async def test_handler_bug_does_not_kill_the_stream(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE)
    h = Harness(hub, base, tmp_path)
    original = h.on_invocation

    def flaky(serial, target, args, arrival):
        if target == "updateGraphMulti":
            raise RuntimeError("bug")
        return original(serial, target, args, arrival)

    h.on_invocation = flaky
    state = await h.run(fakes.SERIAL_B, lambda: len(h.primaries) >= 50)
    assert state.errors["handler"] > 0 and hub.connections == 1


def test_elements_come_from_the_registry():
    s = HubSession(None, SessionState("S", "x"), None, lambda *a: False)
    assert {s.required, *s.optional} == ELEMENTS
    assert hubmod.STABLE_AFTER == 60  # design 6.5: reset after a minute of streaming


async def test_no_release_leaves_other_subscriptions_alone(server, tmp_path):
    holder, base = server
    hub = holder["hub"] = fakes.FakeHub(speed=SCALE)
    h = Harness(hub, base, tmp_path)
    state = await h.run(fakes.SERIAL_B, lambda: len(h.primaries) >= 50, release=False)
    calls = [t for t, _, _ in hub.calls]
    assert "UnInitializeStreaming" not in calls  # neither before subscribing nor on shutdown
    assert calls.count("InitializeStreaming") == 5 and state.disconnects == {"shutdown": 1}


def test_the_refusal_backoff_never_overflows_and_never_passes_its_cap():
    """2 ** (refusals - 2) overflowed a float after 1026 refusals in a row (~47 days of hourly refusals for a
    configured serial that is not on the account), and the jitter came after the cap (up to 72 min)."""
    assert hubmod.REFUSAL_MIN <= hubmod._refusal_delay(2) <= hubmod.REFUSAL_MIN * 1.2
    assert all(hubmod._refusal_delay(n) <= hubmod.REFUSAL_MAX for n in range(2, 2000))
    assert hubmod._refusal_delay(5000) == hubmod.REFUSAL_MAX
