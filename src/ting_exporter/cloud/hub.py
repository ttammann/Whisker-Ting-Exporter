"""One SignalR connection per Ting sensor, kept alive forever.

Connection sequence (what the official app and the working community clients
do, unchanged from v1 because it is proven on the wire):

1. WebSocket to wss://signalr.api.wskr.io/dataHub with headers
   Origin: ionic://localhost and x-wl-api-key: <api_key> (15 s connect timeout)
2. Handshake: send {"protocol":"messagepack","version":1}<0x1E>, expect {}<0x1E> (10 s)
3. Only with release=True (TING_RELEASE_OTHERS): UnInitializeStreaming for all
   five elements, to release subscriptions a previous client left behind;
   errors ignored. The hub serves several clients per sensor in parallel, but
   a release ends every client's stream for that sensor, so active-active
   exporters must not release
4. InitializeStreaming for ComboBinaryData (required), then frequency,
   thdAvg, thdMin, thdMax in parallel (optional; a refusal is logged once)
   Arguments: [{"StationId": serial, "DataElement": name}, api_key, user_id]
   The hub acknowledges with a Completion; only result kind 1 is a refusal.
5. Receive invocations; each frame goes synchronously to `on_invocation`
6. Send a Ping every 5 s
7. On exit (release=True only), UnInitializeStreaming everything (3 s), then close

The session ends when no primary (voltage) sample arrived for `stale` seconds
(also the first-data grace after subscribing), when the hub sends Close, when
the socket or the pinger fails, or on shutdown. Reconnects back off 5 s to
300 s with +-20 % jitter, reset once a session streamed for 60 s (design 6.5:
a session that streamed fine and was then closed should not wait minutes). A stale end is
different: the socket and the subscription worked and only the sensor is silent
(a power or network outage at its site), so the next attempt follows after
STALE_RETRY and the back-off resets. While the sensor is gone that is one short
connection a minute, and when it returns at most about 5 s of data are missed
instead of up to 5 min. A refused
subscription renews the identity once (refresh token first); further
consecutive refusals back off 5 min to 1 h. A successful subscription ends
the streak, so a refusal weeks later renews the identity again. Sign-in failures never cause a
reconnect loop: the session waits for the IdentityManager instead.

Every step is reported through `on_event` (connecting, subscribed, ended with a
reason, refused, auth_wait, backoff, ...): the flight recorder keeps them, and
the pipeline uses them to tell whether this exporter was listening during a
silence (pipeline/cuts.py).
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from .. import signals
from ..auth.cognito import Identity
from ..auth.identity import AuthUnavailable, IdentityManager
from ..clock import Clock
from . import signalr

log = logging.getLogger(__name__)

HUB_URL = "wss://signalr.api.wskr.io/dataHub"
CONNECT_TIMEOUT = 15.0
HANDSHAKE_TIMEOUT = 10.0
PING_INTERVAL = 5.0
INVOKE_TIMEOUT = 10.0
RELEASE_TIMEOUT = 3.0
BACKOFF_MIN, BACKOFF_MAX = 5.0, 300.0
STALE_RETRY = 5.0  # after a stale end: the hub works, only the sensor is silent
STABLE_AFTER = 60.0
REFUSAL_MIN, REFUSAL_MAX = 300.0, 3600.0
AUTH_WAIT_MAX = 300.0

# failures that are the network's or the hub's fault, not ours
EXPECTED = (ConnectionError, aiohttp.ClientError, asyncio.TimeoutError, signalr.ProtocolError, OSError)

# (serial, target, args, arrival wall time) -> True if the frame carried the primary signal
InvocationHandler = Callable[[str, str, list[Any], float], bool]
EventHandler = Callable[[str, str, dict[str, Any], float], None]


def _refusal_delay(refusals: int) -> float:
    """After the second refusal in a row: 5 min doubling to 1 h, jittered up to +20 %, never past the hour. The
    exponent is clamped, so a serial refused for weeks does not overflow a float."""
    return min(REFUSAL_MIN * 2 ** min(refusals - 2, 20) * random.uniform(1.0, 1.2), REFUSAL_MAX)


class Refused(Exception):
    """The hub refused the required subscription (credentials or serial)."""


class ServerClosed(ConnectionError):
    """The hub sent a Close message."""


class Stale(Exception):
    """No primary sample for the stale limit."""


@dataclass
class SessionState:
    """What the self-metrics and /readyz report for one sensor."""

    serial: str
    site: str
    connected: bool = False
    last_primary: float | None = None  # monotonic time of the newest primary sample
    subscribed: list[str] = field(default_factory=list)
    connects: Counter[str] = field(default_factory=Counter)  # ok | refused | error
    disconnects: Counter[str] = field(default_factory=Counter)  # stale | server_close | ws_error | shutdown | error
    errors: Counter[str] = field(default_factory=Counter)  # by kind
    last_error: str | None = None


class HubSession:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        state: SessionState,
        identities: IdentityManager,
        on_invocation: InvocationHandler,
        *,
        on_event: EventHandler | None = None,
        stale: float = 60.0,
        hub_url: str = HUB_URL,
        clock: Clock | None = None,
        release: bool = False,
    ) -> None:
        self.release = release  # True: UnInitializeStreaming before subscribing and on exit (ends other clients' streams)
        self.session = session
        self.state = state
        self.serial = state.serial
        self.identities = identities
        self.on_invocation = on_invocation
        self.on_event = on_event
        self.stale = stale
        self.hub_url = hub_url
        self.clock = clock or Clock()
        self.required, self.optional = signals.elements()
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._pending: dict[str, asyncio.Future[str | None]] = {}
        self._next_id = 0
        self._last_primary = 0.0
        self._optional_logged: set[str] = set()
        self._handler_error_logged = False
        self._refusals = 0  # consecutive refusals; reset by a successful subscription

    # ---- lifecycle -------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        backoff = BACKOFF_MIN
        refused: Identity | None = None
        while not stop.is_set():
            try:
                identity = await self.identities.get(stale=refused)
            except AuthUnavailable as err:
                self._event("auth_wait", reason=err.reason, seconds=round(min(err.retry_in, AUTH_WAIT_MAX)))
                await self._wait(stop, self.identities.wait(min(err.retry_in, AUTH_WAIT_MAX)))
                continue
            refused = None
            started = self.clock.monotonic()
            delay: float | None = None
            try:
                reason = await self._session_once(identity, stop)
            except Refused as err:
                self.state.connects["refused"] += 1
                self._refusals += 1
                refusals = self._refusals
                reason = "refused"
                if refusals == 1:
                    log.error("%s: subscription refused (%s); renewing the identity once", self.serial, err)
                    refused, delay = identity, BACKOFF_MIN * random.uniform(0.8, 1.2)
                else:
                    delay = _refusal_delay(refusals)
                    log.error("%s: subscription refused %d times in a row (%s); next try in %.0f min",
                              self.serial, refusals, err, delay / 60)
                self._event("refused", error=str(err)[:200])
            except Stale as err:
                reason = "stale"
                log.warning("%s: %s; reconnecting", self.serial, err)
                self._event("ended", reason=reason, error=str(err))
            except ServerClosed as err:
                reason = "server_close"
                log.warning("%s: %s", self.serial, err)
                self._event("ended", reason=reason, error=str(err))
            except EXPECTED as err:
                reason = "ws_error"
                self.state.last_error = f"{type(err).__name__}: {err}"[:200]
                log.warning("%s: stream ended: %s", self.serial, self.state.last_error)
                self._event("ended", reason=reason, error=self.state.last_error)
            except Exception as err:  # a bug: say so loudly, then carry on like any other failure
                reason = "error"
                self.state.errors[type(err).__name__] += 1
                self.state.last_error = f"{type(err).__name__}: {err}"[:200]
                log.exception("%s: unexpected error in the hub session", self.serial)
                self._event("ended", reason=reason, error=self.state.last_error)
            finally:
                await self._teardown()
            if reason != "refused":
                self.state.disconnects[reason] += 1
            if stop.is_set():
                break
            if delay is None and reason == "stale":
                backoff = BACKOFF_MIN  # the connection and subscription worked
                delay = STALE_RETRY * random.uniform(0.8, 1.2)
            if delay is None:
                streamed = self.state.last_primary is not None and self.state.last_primary - started > STABLE_AFTER
                if streamed:  # samples kept coming for a minute: whatever ended it, it was not a failing hub
                    backoff = BACKOFF_MIN
                delay = backoff * random.uniform(0.8, 1.2)
                backoff = min(backoff * 2, BACKOFF_MAX)
            log.info("%s: reconnecting in %.0f s", self.serial, delay)
            self._event("backoff", seconds=round(delay, 1), session_seconds=round(self.clock.monotonic() - started, 1))
            await self._wait(stop, self.clock.sleep(delay))

    async def _wait(self, stop: asyncio.Event, awaitable: Any) -> None:
        """Await `awaitable` unless `stop` is set first."""
        tasks = {asyncio.ensure_future(awaitable), asyncio.ensure_future(stop.wait())}
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _session_once(self, identity: Identity, stop: asyncio.Event) -> str:
        log.info("%s: connecting", self.serial)
        self._event("connecting")
        try:
            self._ws = await asyncio.wait_for(
                self.session.ws_connect(
                    self.hub_url,
                    headers={"Origin": "ionic://localhost", "x-wl-api-key": identity.api_key},
                    heartbeat=None,
                    max_msg_size=4 * 1024 * 1024,
                ),
                self.clock.real(CONNECT_TIMEOUT),
            )
            await self._ws.send_str(signalr.HANDSHAKE_REQUEST)
            reply = await self._ws.receive(timeout=self.clock.real(HANDSHAKE_TIMEOUT))
            if reply.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                raise signalr.ProtocolError(f"handshake got {reply.type.name}")
            leftover = signalr.parse_handshake(reply.data)
        except EXPECTED:
            self.state.connects["error"] += 1
            raise

        receiver = asyncio.create_task(self._receive_loop(leftover))
        receiver.add_done_callback(self._fail_pending)
        pinger = asyncio.create_task(self._ping_loop())
        try:
            try:
                await self._subscribe(identity)
            except EXPECTED:
                self.state.connects["error"] += 1
                raise
            self.state.connects["ok"] += 1
            self._refusals = 0
            self.state.connected = True
            self._last_primary = self.clock.monotonic()  # the first-data grace starts now
            return await self._watchdog(receiver, pinger, stop)
        finally:
            self.state.connected = False
            if not receiver.done() and self.release:
                await self._release(identity)  # needs the receiver alive to see the acks
            for task in (pinger, receiver):
                task.cancel()
            await asyncio.gather(pinger, receiver, return_exceptions=True)

    def _fail_pending(self, receiver: asyncio.Task) -> None:
        """The receiver ended, so no Completion can arrive any more: fail every invocation waiting for one at once,
        with the reason (the hub's Close, the socket error), instead of letting each wait out INVOKE_TIMEOUT."""
        reason = None if receiver.cancelled() else receiver.exception()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(reason if reason is not None else ConnectionError("connection closed"))

    def _args(self, identity: Identity, element: str) -> list[Any]:
        return [{"StationId": self.serial, "DataElement": element}, identity.api_key, identity.user_id]

    async def _subscribe(self, identity: Identity) -> None:
        everything = [self.required, *self.optional]
        if self.release:
            await asyncio.gather(
                *(self._invoke("UnInitializeStreaming", self._args(identity, e), RELEASE_TIMEOUT) for e in everything),
                return_exceptions=True,  # releasing nothing is fine
            )
        error = await self._invoke("InitializeStreaming", self._args(identity, self.required))
        if error is not None:  # an error Completion, even one with an empty text
            raise Refused(error or "refused without a reason")
        subscribed = [self.required]
        results = await asyncio.gather(
            *(self._invoke("InitializeStreaming", self._args(identity, e)) for e in self.optional),
            return_exceptions=True,
        )
        for element, result in zip(self.optional, results):
            if result is None:
                subscribed.append(element)
                continue
            level = logging.INFO if element not in self._optional_logged else logging.DEBUG
            self._optional_logged.add(element)
            log.log(level, "%s: optional stream %s unavailable: %s", self.serial, element, result)
            self._event("optional_refused", element=element, error=str(result)[:200])
        self.state.subscribed = subscribed
        log.info("%s: subscribed to %s", self.serial, ", ".join(subscribed))
        self._event("subscribed", elements=list(subscribed))

    async def _release(self, identity: Identity) -> None:
        """UnInitializeStreaming every element, so the hub frees the sensor for the next session."""
        elements, self.state.subscribed = self.state.subscribed, []
        if not elements or self._ws is None or self._ws.closed:
            return
        results = await asyncio.gather(
            *(self._invoke("UnInitializeStreaming", self._args(identity, e), RELEASE_TIMEOUT) for e in elements),
            return_exceptions=True,
        )
        failed = [e for e, r in zip(elements, results) if r is not None]
        if failed:
            log.debug("%s: release not acknowledged for %s", self.serial, ", ".join(failed))

    async def _teardown(self) -> None:
        ws, self._ws = self._ws, None
        self.state.connected = False
        self.state.subscribed = []
        for future in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionError("connection closed"))
        self._pending.clear()
        if ws is not None and not ws.closed:
            try:
                await asyncio.wait_for(ws.close(), 5)
            except (asyncio.TimeoutError, *EXPECTED):
                pass

    def _event(self, kind: str, **fields: Any) -> None:
        if self.on_event is not None:
            self.on_event(self.serial, kind, fields, self.clock.time())

    # ---- protocol --------------------------------------------------------

    def _new_id(self) -> str:
        self._next_id += 1
        return str(self._next_id)

    async def _invoke(self, target: str, args: list[Any], timeout: float = INVOKE_TIMEOUT) -> str | None:
        """Send an Invocation and return the Completion error (None = accepted)."""
        assert self._ws is not None
        invocation_id = self._new_id()
        future: asyncio.Future[str | None] = asyncio.get_running_loop().create_future()
        self._pending[invocation_id] = future
        try:
            await self._ws.send_bytes(signalr.encode_invocation(invocation_id, target, args))
            return await asyncio.wait_for(future, self.clock.real(timeout))
        finally:
            self._pending.pop(invocation_id, None)

    async def _ping_loop(self) -> None:
        while self._ws is not None and not self._ws.closed:
            await self.clock.sleep(PING_INTERVAL)
            if self._ws is None or self._ws.closed:
                return
            await self._ws.send_bytes(signalr.encode_ping())

    async def _receive_loop(self, leftover: bytes | None) -> None:
        if leftover:
            self._handle(leftover)
        assert self._ws is not None
        async for msg in self._ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                try:
                    self._handle(msg.data)
                except signalr.ProtocolError as err:
                    log.debug("%s: undecodable frame: %s", self.serial, err)
                    self.state.errors["protocol_error"] += 1
                    self._event("protocol_error", error=str(err)[:200], bytes=len(msg.data))
            elif msg.type == aiohttp.WSMsgType.ERROR:
                raise ConnectionError(f"websocket error: {self._ws.exception()}")
        raise ConnectionError(f"websocket closed (code {self._ws.close_code})")

    def _handle(self, data: bytes) -> None:
        """Process one binary frame; raise ServerClosed if the hub closed the session."""
        arrival = self.clock.time()
        for message in signalr.decode_messages(data):
            if (completion := signalr.as_completion(message)) is not None:
                future = self._pending.get(completion.invocation_id)
                if future is not None and not future.done():
                    future.set_result(completion.error)
            elif (close := signalr.as_close(message)) is not None:
                raise ServerClosed(f"hub sent Close: {close.error or 'no reason'}")
            elif (invocation := signalr.as_invocation(message)) is not None:
                try:
                    primary = self.on_invocation(self.serial, invocation[0], invocation[1], arrival)
                except Exception:  # a pipeline bug must not stop the stream
                    self.state.errors["handler"] += 1
                    if not self._handler_error_logged:
                        self._handler_error_logged = True
                        log.exception("%s: error handling %s (logged once)", self.serial, invocation[0])
                    continue
                if primary:
                    now = self.clock.monotonic()
                    self._last_primary = self.state.last_primary = now

    async def _watchdog(self, receiver: asyncio.Task, pinger: asyncio.Task, stop: asyncio.Event) -> str:
        """Return the disconnect reason, or raise why the session failed."""
        stopper = asyncio.ensure_future(stop.wait())
        try:
            while True:
                remaining = self._last_primary + self.stale - self.clock.monotonic()
                if remaining <= 0:
                    raise Stale(f"no voltage for {self.stale:.0f} s")
                done, _ = await asyncio.wait(
                    {receiver, pinger, stopper}, timeout=self.clock.real(remaining), return_when=asyncio.FIRST_COMPLETED
                )
                if stopper in done:
                    return "shutdown"
                if receiver in done:
                    receiver.result()  # re-raises why the socket ended
                    raise ConnectionError("receiver ended")
                if pinger in done:
                    pinger.result()
                    raise ConnectionError("pinger ended")
        finally:
            stopper.cancel()
