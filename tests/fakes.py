"""Local stand-ins for Cognito, the Ting REST API, the Ting SignalR hub and VictoriaMetrics.

They implement the server side of each protocol, so the client code under
test runs unmodified against them. FakeHub replays the fixture recordings
(tests/fixtures/capture: synthetic, with the live service's shapes,
interleaving and catch-up), only faster.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import hmac
import json
import os
import re
from pathlib import Path
from typing import Any

import msgpack
from aiohttp import WSMsgType, web

from ting_exporter.auth.cognito import G, K, N, USER_POOL_ID, _hash_ints, _hkdf16, _sha256_hex, pad_hex
from ting_exporter.cloud import signalr

FIXTURES = Path(__file__).parent / "fixtures"
CAPTURE = sorted((FIXTURES / "capture").glob("[a-e]-*.jsonl.gz"))  # the golden slices
CUT = FIXTURES / "capture" / "f-cut.jsonl.gz"  # sensor B: a gap, a brownout, a power cut, an unwatched silence

USER_ID_FOR_SRP = "5f1c0000-aaaa-4bbb-8ccc-000000000001"
USER_ID = "4242"
API_KEY = "test-api-key-0001"
ACCESS_TOKEN = "access-token-xyz"
REFRESH_TOKEN = "refresh-token-abc"
SERIAL_A, SERIAL_B = "TNG000001", "TNG000002"


class FakeCognito:
    def __init__(self, password: str, *, issue_refresh: bool = True) -> None:
        self.pool_name = USER_POOL_ID.split("_", 1)[1]
        self.set_password(password)
        self.secret_block = base64.standard_b64encode(os.urandom(48)).decode()
        self.session_keys: list[bytes] = []  # one per open SRP challenge: two clients may sign in at once
        self.issue_refresh = issue_refresh
        self.refresh_valid = True
        self.calls: list[str] = []  # "srp", "verifier", "refresh", "getuser"
        self.fail: tuple[int, str] | None = None  # (status, __type) for every call while set

    def set_password(self, password: str) -> None:
        self.salt = os.urandom(16).hex()
        inner = _sha256_hex(f"{self.pool_name}{USER_ID_FOR_SRP}:{password}".encode())
        x = int(_sha256_hex(bytes.fromhex(pad_hex(self.salt) + inner)), 16)
        self.verifier = pow(G, x, N)

    async def handle(self, request: web.Request) -> web.Response:
        target = request.headers["X-Amz-Target"].rsplit(".", 1)[1]
        assert request.headers["Content-Type"] == "application/x-amz-json-1.1"
        body = json.loads(await request.text())
        if self.fail is not None:
            self.calls.append(f"fail:{target}")
            status, code = self.fail
            return web.json_response({"__type": code, "message": "injected"}, status=status)
        if target == "InitiateAuth" and body["AuthFlow"] == "REFRESH_TOKEN_AUTH":
            self.calls.append("refresh")
            if not self.refresh_valid or body["AuthParameters"]["REFRESH_TOKEN"] != REFRESH_TOKEN:
                return web.json_response({"__type": "NotAuthorizedException", "message": "Invalid Refresh Token"}, status=400)
            return web.json_response({"AuthenticationResult": {"AccessToken": ACCESS_TOKEN, "ExpiresIn": 3600}})
        if target == "InitiateAuth":
            self.calls.append("srp")
            A = int(body["AuthParameters"]["SRP_A"], 16)
            b = int.from_bytes(os.urandom(64), "big")
            B = (K * self.verifier + pow(G, b, N)) % N
            u = _hash_ints(A, B)
            S = pow(A * pow(self.verifier, u, N), b, N)
            self.session_keys.append(_hkdf16(bytes.fromhex(pad_hex(S)), bytes.fromhex(pad_hex(f"{u:x}"))))
            return web.json_response(
                {
                    "ChallengeName": "PASSWORD_VERIFIER",
                    "ChallengeParameters": {
                        "USER_ID_FOR_SRP": USER_ID_FOR_SRP,
                        "SALT": self.salt,
                        "SRP_B": f"{B:x}",
                        "SECRET_BLOCK": self.secret_block,
                        "USERNAME": USER_ID_FOR_SRP,
                    },
                }
            )
        if target == "RespondToAuthChallenge":
            self.calls.append("verifier")
            r = body["ChallengeResponses"]
            msg = self.pool_name.encode() + USER_ID_FOR_SRP.encode() + base64.standard_b64decode(r["PASSWORD_CLAIM_SECRET_BLOCK"]) + r["TIMESTAMP"].encode()
            key = next((k for k in self.session_keys
                        if r["PASSWORD_CLAIM_SIGNATURE"] == base64.standard_b64encode(hmac.new(k, msg, hashlib.sha256).digest()).decode()), None)
            if key is None:
                return web.json_response({"__type": "NotAuthorizedException", "message": "Incorrect username or password."}, status=400)
            self.session_keys.remove(key)
            result = {"AccessToken": ACCESS_TOKEN, "ExpiresIn": 3600}
            if self.issue_refresh:
                result["RefreshToken"] = REFRESH_TOKEN
            return web.json_response({"AuthenticationResult": result})
        if target == "GetUser":
            self.calls.append("getuser")
            assert body["AccessToken"] == ACCESS_TOKEN
            return web.json_response(
                {"UserAttributes": [{"Name": "custom:user_id", "Value": USER_ID}, {"Name": "custom:api_key", "Value": API_KEY}]}
            )
        return web.json_response({"__type": "InvalidAction"}, status=400)


def _authorized(request: web.Request) -> bool:
    return request.headers.get("Authorization") == f"Bearer {ACCESS_TOKEN}" and request.headers.get("x-wl-api-key") == API_KEY


async def fake_users(request: web.Request) -> web.Response:
    if not _authorized(request):
        return web.Response(status=401)
    return web.json_response(
        {
            "id": int(request.match_info["user_id"]),
            "email": "person@example.org",
            "devices": [
                {"serialNumber": SERIAL_A, "name": "Example Home", "type": "FireSensor", "version": "SparkFault 2.6.17",
                 "siteId": 100, "isFire": False, "hasFrozenPipe": False,
                 "fireHazardStatus": {"learningMode": False, "message": "No Hazards Detected",
                                      "efhStatus": {"status": None, "level": None, "message": "No Hazards Detected"},
                                      "ufhStatus": {"status": None, "level": None, "message": "No Hazards Detected"}}},
                {"serialNumber": SERIAL_B, "name": "Example Home", "type": "FireSensor", "version": "SparkFault 2.6.17",
                 "siteId": 200},
                {"name": "no serial, skipped"},
            ],
            "sites": [{"id": 100, "displayName": "Home"}, {"id": 200, "displayName": "Cabin"}],
        }
    )


async def fake_conditions(request: web.Request) -> web.Response:
    """The shape a real account sends (field names; values invented)."""
    if not _authorized(request):
        return web.Response(status=401)
    return web.json_response({
        "currentTemperatures": {"100": 21.5, "200": -3.25},
        "currentOutageRisks": {"100": 29, "200": 41},
        "devices": [{"serialNumber": SERIAL_B, "isFire": False, "hasFrozenPipe": True,
                     "fireHazardStatus": {"efhStatus": {"status": "ElevatedSuspicious", "level": 2}}}],
    })


async def fake_frozen_pipe(request: web.Request) -> web.Response:
    if not _authorized(request):
        return web.Response(status=401)
    if request.match_info["serial"] == SERIAL_A:
        return web.Response(status=404)  # not every sensor has it
    return web.json_response({"level": 55, "outdoorTemperatureC": -8.5, "detectedLocationType": "UnconditionedSpace"})


NOTIFICATIONS = [  # in the history API's shape (eventTimestampUtc is a .NET placeholder); values invented, newest first
    {"id": "n3", "userId": 4242, "eventType": "PowerRestored", "eventCategory": "PowerQuality",
     "title": "Power Restoration", "subtitle": "Power Restoration", "message": "Power has been restored at Home.",
     "eventTimestampUtc": "0001-01-01T00:00:00", "eventTimestampLocal": "2026-03-11T15:49:06.428-07:00",
     "sentUtc": "2026-03-11T22:49:07Z", "serialNumber": SERIAL_A, "siteId": 1, "statuses": [],
     "isAcknowledged": False, "isCleared": False, "logoUrl": ""},
    {"id": "n2", "userId": 4242, "eventType": "CommunityPowerOutage", "eventCategory": "PowerQuality",
     "title": "Community Power Outage", "subtitle": "Community Power Outage",
     "message": "Ting has detected a Community Power Outage at Home.",
     "eventTimestampUtc": "0001-01-01T00:00:00", "eventTimestampLocal": "2026-03-11T15:48:57.412-07:00",
     "sentUtc": "2026-03-11T22:49:00Z", "serialNumber": SERIAL_A, "siteId": 1, "statuses": [],
     "isAcknowledged": True, "isCleared": False, "logoUrl": ""},
    {"id": "n1", "userId": 4242, "eventType": "Sag", "eventCategory": "PowerQuality", "title": "Power Brownout",
     "eventTimestampUtc": "2026-03-09T20:54:00Z", "sentUtc": "2026-03-09T20:56:00Z",
     "serialNumber": SERIAL_B, "siteId": 2, "statuses": [], "isAcknowledged": True, "isCleared": True},
]


async def fake_notifications(request: web.Request) -> web.Response:
    if request.headers.get("Authorization") != f"Bearer {ACCESS_TOKEN}" or request.headers.get("x-wl-api-key") != API_KEY:
        return web.Response(status=401)
    assert request.match_info["user_id"] == USER_ID
    return web.json_response(NOTIFICATIONS)


VOLTAGE_QUERIES: list[dict[str, str]] = []


async def fake_voltage_history(request: web.Request) -> web.Response:
    """The cloud's voltage history: 5-minute aggregates, shaped like the community integration's fixture."""
    if request.headers.get("Authorization") != f"Bearer {ACCESS_TOKEN}" or request.headers.get("x-wl-api-key") != API_KEY:
        return web.Response(status=401)
    VOLTAGE_QUERIES.append({"serial": request.match_info["serial"], **request.query})
    return web.json_response({"unit": "V", "data": [
        {"timestampUtc": "2026-03-11T22:40:00Z", "minimum": 121.9, "maximum": 123.4, "average": 122.6, "sampleCount": 1200, "expectedSampleCount": 1200},
        {"timestampUtc": "2026-03-11T22:45:00Z", "minimum": 118.5, "maximum": 125.6, "average": 122.9, "sampleCount": 1088, "expectedSampleCount": 1200},
        {"timestampUtc": "2026-03-11T23:00:00Z", "minimum": 122.0, "maximum": 123.0, "average": 122.5, "sampleCount": 1200, "expectedSampleCount": 1200},
    ]})


# ---- capture replay -----------------------------------------------------------


def capture_rows(paths: list[Path] | None = None) -> list[dict[str, Any]]:
    rows = []
    for path in paths or CAPTURE:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            rows += [json.loads(line) for line in f]
    return rows


_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?\+00:00$")


def to_wire(obj: Any) -> Any:
    """Undo `record.jsonable` for the hub: ISO strings become datetimes again.

    The live hub sends DataTimeUtc / ObsTime as MessagePack timestamps,
    which the record file stores as `isoformat()` strings.
    """
    from datetime import datetime

    if isinstance(obj, str) and _ISO.match(obj):
        return datetime.fromisoformat(obj)
    if isinstance(obj, dict):
        return {k: to_wire(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_wire(v) for v in obj]
    return obj


class FakeHub:
    """Speaks the SignalR MessagePack hub protocol like signalr.api.wskr.io.

    Each connection replays the recorded invocations of its StationId, in recorded
    order, `speed` times faster than recorded (arrival spacing is kept, so the
    buffered-path pauses and interleaving are there too). Several connections
    may stream the same sensor; an UnInitializeStreaming of ComboBinaryData ends
    the stream of every connection for that sensor (their sockets stay open), as
    the real hub does. Knobs:

    refuse          error Completion for ComboBinaryData (its text: refuse_reason)
    optional_ok     False: error Completion for frequency/THD
    silent_after    stop sending after this many invocations (the socket stays open)
    close_after     send a hub Close message after this many invocations
    drop_after      drop the TCP connection after this many invocations
    garbage_at      send a truncated frame instead of invocation number N
    pause           (N, seconds): after invocation N, pause that many capture seconds
    close_on_subscribe  a reason: answer the ComboBinaryData subscription with a hub Close message
    """

    def __init__(
        self,
        *,
        speed: float = 50.0,
        refuse: bool = False,
        optional_ok: bool = True,
        silent_after: int | None = None,
        close_after: int | None = None,
        drop_after: int | None = None,
        garbage_at: int | None = None,
        pause: tuple[int, float] | None = None,
        rows: list[dict[str, Any]] | None = None,
        close_on_subscribe: str | None = None,
        refuse_reason: str = "Unauthorized",
    ) -> None:
        self.pause = pause
        self.refuse_reason = refuse_reason
        self.close_on_subscribe = close_on_subscribe
        self.speed = speed
        self.refuse = refuse
        self.optional_ok = optional_ok
        self.silent_after = silent_after
        self.close_after = close_after
        self.drop_after = drop_after
        self.garbage_at = garbage_at
        self.rows = rows if rows is not None else capture_rows(CAPTURE[:1])
        self.calls: list[tuple[str, str, str]] = []  # (target, serial, element)
        self.headers: list[dict[str, str]] = []
        self.connections = 0
        self.sent: dict[str, int] = {}
        self.connected_at: list[float] = []
        self.disconnected_at: list[float] = []
        self.last_sent_at: dict[str, float] = {}
        self.streams: dict[str, set[asyncio.Task]] = {}  # serial -> streaming connections
        self.knocked_off = 0  # streams ended by a release from any connection

    async def handle(self, request: web.Request) -> web.WebSocketResponse:
        self.connections += 1
        self.headers.append(dict(request.headers))
        loop = asyncio.get_running_loop()
        self.connected_at.append(loop.time())
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        first = await ws.receive()
        assert first.type == WSMsgType.TEXT and first.data == signalr.HANDSHAKE_REQUEST
        await ws.send_bytes(b"{}\x1e")  # binary handshake reply, like Ting
        streamer: asyncio.Task | None = None
        try:
            async for msg in ws:
                if msg.type != WSMsgType.BINARY:
                    continue
                for m in signalr.decode_messages(msg.data):
                    if m[0] != signalr.INVOCATION:
                        continue
                    _, _, inv_id, target, args = m[:5]
                    serial, element = args[0]["StationId"], args[0]["DataElement"]
                    self.calls.append((target, serial, element))
                    assert args[1] == API_KEY and args[2] == USER_ID
                    ok = True
                    if target == "InitializeStreaming":
                        ok = not self.refuse if element == "ComboBinaryData" else self.optional_ok
                    reply = [3, {}, inv_id, 3, None] if ok else [3, {}, inv_id, 1, self.refuse_reason]
                    if target == "UnInitializeStreaming" and element == "ComboBinaryData":
                        for task in self.streams.pop(serial, set()):
                            if not task.done():
                                task.cancel()
                                self.knocked_off += 1
                        streamer = None
                    if ws.closed:
                        continue
                    if self.close_on_subscribe and target == "InitializeStreaming" and element == "ComboBinaryData":
                        await ws.send_bytes(signalr.frame([signalr.CLOSE, self.close_on_subscribe, True]))
                        continue
                    await ws.send_bytes(signalr.frame(reply))
                    if target == "InitializeStreaming" and element == "ComboBinaryData" and ok and streamer is None:
                        streamer = asyncio.create_task(self._stream(ws, serial))
                        self.streams.setdefault(serial, set()).add(streamer)
        finally:
            self.disconnected_at.append(loop.time())
            if streamer:
                streamer.cancel()
                for tasks in self.streams.values():
                    tasks.discard(streamer)
        return ws

    async def _stream(self, ws: web.WebSocketResponse, serial: str) -> None:
        rows = [r for r in self.rows if r.get("kind") == "invocation" and r.get("serial") == serial]
        loop = asyncio.get_running_loop()
        start_wall, start_rec = loop.time(), rows[0]["t"] if rows else 0.0
        for n, row in enumerate(rows):
            if self.silent_after is not None and n >= self.silent_after:
                return
            if self.close_after is not None and n >= self.close_after:
                await ws.send_bytes(signalr.frame([signalr.CLOSE, "Server shutting down", True]))
                return
            if self.drop_after is not None and n >= self.drop_after:
                await ws.close()
                return
            if self.pause is not None and n == self.pause[0]:
                start_wall += self.pause[1] / self.speed
            due = start_wall + (row["t"] - start_rec) / self.speed
            if (delay := due - loop.time()) > 0:
                await asyncio.sleep(delay)
            if self.garbage_at is not None and n == self.garbage_at:
                data = b"\x05\x91"  # truncated frame
            else:
                body = msgpack.packb([1, {}, None, row["target"], to_wire(row["args"])], use_bin_type=True, datetime=True)
                data = signalr.encode_varint(len(body)) + body
            try:
                await ws.send_bytes(data)
            except ConnectionError:
                return
            self.sent[serial] = self.sent.get(serial, 0) + 1
            self.last_sent_at[serial] = loop.time()


# ---- VictoriaMetrics ------------------------------------------------------------

_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\} (\S+) (-?\d+)$")


def parse_prometheus(text: str) -> list[tuple[str, dict[str, str], float, int]]:
    out = []
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _LINE.match(line)
        assert m, f"bad line {line!r}"
        labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2)))
        out.append((m.group(1), labels, float(m.group(3)), int(m.group(4))))
    return out


_EXPR = re.compile(
    r"^(?:round\()?(?P<agg>min|max|avg|count|last)_over_time\((?P<metric>[a-zA-Z_:][a-zA-Z0-9_:]*)"
    r"(?:\{(?P<matchers>[^}]*)\})?\[(?P<window>\d+)(?P<unit>[smhdy])\]\)(?:, (?P<step>[0-9.]+)\))?(?: == (?P<eq>[0-9.]+))?$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "y": 365 * 86400}


class FakeVM:
    """POST /api/v1/import/prometheus stores samples keyed like VM dedup (equal timestamps keep the larger value).

    GET /api/v1/query and /api/v1/query_range evaluate the few expressions the exporter sends:
    [round(]<agg>_over_time(<metric>{matchers}[<window>])[, step)][ == v], window (T - w, T].
    mode: "ok" (204), "500", "400", "429", "hang" (never answers in time).
    """

    def __init__(self) -> None:
        self.mode = "ok"
        self.requests: list[str] = []  # the mode each request met
        self.samples: dict[tuple[str, tuple[tuple[str, str], ...], int], float] = {}
        self.received = 0
        self.encodings: list[str] = []
        self.queries: list[str] = []
        self.query_params: list[dict[str, str]] = []  # every instant query's arguments

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append(self.mode)
        if self.mode == "hang":
            await asyncio.sleep(1.0)  # longer than the client's timeout in the tests
            return web.Response(status=204)
        if self.mode in ("500", "429"):
            return web.Response(status=int(self.mode), text="injected")
        body = await request.read()
        self.encodings.append(request.headers.get("Content-Encoding", ""))
        if body[:2] == b"\x1f\x8b":  # aiohttp usually inflates Content-Encoding: gzip itself
            body = gzip.decompress(body)
        if self.mode == "400":
            return web.Response(status=400, text="cannot parse")
        self.load(body.decode())
        return web.Response(status=204)

    def load(self, text: str) -> None:
        for name, labels, value, ts in parse_prometheus(text):
            key = (name, tuple(sorted(labels.items())), ts)
            self.samples[key] = max(value, self.samples.get(key, value))
            self.received += 1

    def count(self, name: str, **labels: str) -> int:
        return sum(1 for (n, lab, _ts) in self.samples if n == name and all(dict(lab).get(k) == v for k, v in labels.items()))

    def evaluate(self, expr: str, at: float) -> list[tuple[dict[str, str], float]]:
        self.queries.append(expr)
        m = _EXPR.match(expr)
        assert m, f"FakeVM cannot evaluate {expr!r}"
        want = dict(re.findall(r'(\w+)="([^"]*)"', m["matchers"] or ""))
        window_ms = int(m["window"]) * _UNITS[m["unit"]] * 1000
        end = int(round(at * 1000))
        series: dict[tuple[tuple[str, str], ...], list[tuple[int, float]]] = {}
        for (name, lab, ts), value in self.samples.items():
            if name == m["metric"] and end - window_ms < ts <= end and all(dict(lab).get(k) == v for k, v in want.items()):
                series.setdefault(lab, []).append((ts, value))
        out = []
        for lab, points in series.items():
            values = [v for _, v in sorted(points)]
            agg = m["agg"]
            v = {"min": min, "max": max, "count": len, "last": lambda xs: xs[-1],
                 "avg": lambda xs: sum(xs) / len(xs)}[agg](values)
            if m["step"]:
                step = float(m["step"])
                v = round(v / step) * step
            if m["eq"] is not None and v != float(m["eq"]):
                continue
            labels = dict(lab) if m["eq"] is not None and agg == "last" else {k: x for k, x in lab}
            out.append((labels, float(v)))
        return out

    async def query(self, request: web.Request) -> web.Response:
        q = request.query
        self.query_params.append(dict(q))
        if self.mode != "ok":
            return web.Response(status=503, text="injected")
        result = [{"metric": labels, "value": [float(q["time"]), _num(v)]} for labels, v in self.evaluate(q["query"], float(q["time"]))]
        return web.json_response({"status": "success", "data": {"resultType": "vector", "result": result}})

    async def query_range(self, request: web.Request) -> web.Response:
        q = request.query
        if self.mode != "ok":
            return web.Response(status=503, text="injected")
        start, end, step = int(q["start"]), int(q["end"]), int(q["step"].rstrip("s"))
        series: dict[tuple, dict] = {}
        for t in range(start, end + 1, step):
            for labels, v in self.evaluate(q["query"], t):
                key = tuple(sorted(labels.items()))
                series.setdefault(key, {"metric": labels, "values": []})["values"].append([t, _num(v)])
        return web.json_response({"status": "success", "data": {"resultType": "matrix", "result": list(series.values())}})

    def routes(self, app: web.Application, prefix: str = "") -> None:
        app.router.add_post(f"{prefix}/api/v1/import/prometheus", self.handle)
        app.router.add_get(f"{prefix}/api/v1/query", self.query)
        app.router.add_get(f"{prefix}/api/v1/query_range", self.query_range)


def _num(v: float) -> str:
    return str(int(v)) if v == int(v) else repr(v)


async def start_app(app: web.Application) -> tuple[web.AppRunner, str]:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}"
