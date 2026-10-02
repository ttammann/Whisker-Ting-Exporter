"""IdentityManager: who signs in, when, and how often.

The Ting account is the owner's phone-app account, so a retry storm that
locks it would also lock the owner out. Rules:

- One sign-in at a time; concurrent callers share its result.
- Rejected credentials (NotAuthorized, UserNotFound, PasswordResetRequired,
  UserNotConfirmed, an unexpected challenge such as MFA, unknown 4xx) put the
  manager in HOLD: no automatic attempt for `hold_seconds` (6 h), except right
  after the password file changes (checked every 60 s). So the owner fixes a
  bad password by rewriting the secret file; no restart needed.
- Throttling, 5xx and network errors put it in BACKOFF: 30 s doubling to 15 min.
- At most `max_per_hour` successful sign-ins (SRP or refresh) per hour.
- A hub refusal renews with REFRESH_TOKEN_AUTH first and falls back to SRP
  only if Cognito rejects the refresh token.
- Nothing is written to disk; logs carry user_id and error codes only.

Callers that cannot get an identity receive AuthUnavailable with a hint how
long to wait, and should `await manager.wait(hint)`: it returns early when the
state changes (a new password, a successful sign-in elsewhere).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from collections import Counter, deque
from collections.abc import Awaitable, Callable
from pathlib import Path

import aiohttp

from ..clock import Clock
from . import cognito
from .cognito import AuthError, Identity

log = logging.getLogger(__name__)

NO_IDENTITY, OK, BACKOFF, HOLD = "no_identity", "ok", "backoff", "hold"
STATE_VALUE = {NO_IDENTITY: 0, OK: 0, BACKOFF: 1, HOLD: 2}
BACKOFF_MIN, BACKOFF_MAX = 30.0, 900.0
SECRET_CHECK_INTERVAL = 60.0
RATE_WINDOW = 3600.0


class AuthUnavailable(Exception):
    """No identity right now. `retry_in` is a hint in seconds."""

    def __init__(self, reason: str, retry_in: float) -> None:
        super().__init__(f"{reason}, retry in {retry_in:.0f} s")
        self.reason = reason
        self.retry_in = retry_in


class SecretFile:
    """The password file. Read on demand; its fingerprint tells when it changed."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self) -> str:
        return self.read_with_fingerprint()[0]

    def read_with_fingerprint(self) -> tuple[str, tuple[int, str]]:
        """The password and the fingerprint of the very bytes it came from (raises OSError, UnicodeDecodeError)."""
        st = self.path.stat()
        data = self.path.read_bytes()
        text = data.decode("utf-8").strip()
        if not text:
            raise OSError(f"{self.path} is empty")
        return text, (st.st_mtime_ns, hashlib.sha256(data).hexdigest())

    def fingerprint(self) -> tuple[int, str] | None:
        try:
            st = self.path.stat()
            return st.st_mtime_ns, hashlib.sha256(self.path.read_bytes()).hexdigest()
        except OSError:
            return None

    def __repr__(self) -> str:
        return f"SecretFile({str(self.path)!r})"


SignIn = Callable[[aiohttp.ClientSession, str, str, str], Awaitable[Identity]]
Refresh = Callable[[aiohttp.ClientSession, str, str], Awaitable[Identity]]


class IdentityManager:
    def __init__(
        self,
        session: aiohttp.ClientSession | None,
        username: str,
        secret: SecretFile,
        *,
        cognito_url: str = cognito.ENDPOINT,
        hold_seconds: float = 21_600.0,
        max_per_hour: int = 6,
        clock: Clock | None = None,
        sign_in: SignIn = cognito.sign_in,
        refresh: Refresh = cognito.refresh,
        on_secret: Callable[[str], None] | None = None,
        on_retire: Callable[[str], None] | None = None,
    ) -> None:
        self.session = session
        self.username = username
        self.secret = secret
        self.cognito_url = cognito_url
        self.hold_seconds = hold_seconds
        self.max_per_hour = max_per_hour
        self.clock = clock or Clock()
        self._sign_in = sign_in
        self._refresh = refresh
        self._on_secret = on_secret or (lambda _value: None)
        self._on_retire = on_retire or (lambda _value: None)  # a token that has been replaced

        self.state = NO_IDENTITY
        self.current: Identity | None = None
        self.last_error: str | None = None
        self.signins: Counter[tuple[str, str]] = Counter()  # (method, result)
        self._lock = asyncio.Lock()
        self._changed = asyncio.Event()
        self._successes: deque[float] = deque()
        self._hold_until = 0.0
        self._retry_at = 0.0
        self._backoff = 0.0
        self._fingerprint = secret.fingerprint()

    # ---- public --------------------------------------------------------------

    @property
    def state_value(self) -> int:
        return STATE_VALUE[self.state]

    async def get(self, stale: Identity | None = None) -> Identity:
        """The current identity; renew it first if `stale` is the one in use (the hub refused it)."""
        async with self._lock:
            if self.current is not None and self.current is not stale:
                return self.current
            now = self.clock.monotonic()
            if self.state == HOLD and now < self._hold_until:
                raise AuthUnavailable("credentials held", self._hold_until - now)
            if self.state == BACKOFF and now < self._retry_at:
                raise AuthUnavailable("sign-in backing off", self._retry_at - now)
            while self._successes and now - self._successes[0] >= RATE_WINDOW:
                self._successes.popleft()
            if len(self._successes) >= self.max_per_hour:
                wait = RATE_WINDOW - (now - self._successes[0])
                log.warning("sign-in rate limit (%d per hour) reached; next attempt in %.0f s", self.max_per_hour, wait)
                raise AuthUnavailable("rate limited", wait)

            refresh_token = (stale or self.current or _NO).refresh_token
            if stale is not None and refresh_token:
                identity = await self._try_refresh(refresh_token)
                if identity is not None:
                    return self._succeeded(identity, "refresh")
            return self._succeeded(await self._srp(), "srp")

    async def wait(self, seconds: float) -> None:
        """Sleep up to `seconds` (policy time), or until the auth state changes."""
        event = self._changed
        try:
            await asyncio.wait_for(event.wait(), self.clock.real(max(0.0, seconds)))
        except asyncio.TimeoutError:
            pass

    def check_secret(self) -> bool:
        """Notice a rewritten password file; lift a HOLD at once. Returns True if it changed."""
        fingerprint = self.secret.fingerprint()
        if fingerprint == self._fingerprint:
            return False
        self._fingerprint = fingerprint
        if fingerprint is None:
            return True
        log.info("password file changed")
        if self.state in (HOLD, BACKOFF):
            log.info("retrying the sign-in with the new password")
            self.state, self._hold_until, self._retry_at = NO_IDENTITY, 0.0, 0.0
            self._notify()
        return True

    async def watch(self, stop: asyncio.Event) -> None:
        """Check the password file every 60 s."""
        while not stop.is_set():
            self.check_secret()
            try:
                await asyncio.wait_for(stop.wait(), self.clock.real(SECRET_CHECK_INTERVAL))
            except asyncio.TimeoutError:
                pass

    # ---- internals -------------------------------------------------------------

    def _notify(self) -> None:
        self._changed.set()
        self._changed = asyncio.Event()

    def _succeeded(self, identity: Identity, method: str) -> Identity:
        self._successes.append(self.clock.monotonic())
        self.signins[(method, "ok")] += 1
        for value in (identity.api_key, identity.access_token, identity.refresh_token):
            if value:
                self._on_secret(value)
        if self.current is not None:
            for old, new in ((self.current.access_token, identity.access_token), (self.current.refresh_token, identity.refresh_token)):
                if old and old != new:
                    self._on_retire(old)
        if self.state != OK:
            log.info("signed in (%s) as user_id %s", method, identity.user_id)
        else:
            log.info("renewed the identity (%s) for user_id %s", method, identity.user_id)
        self.state, self.current, self.last_error, self._backoff = OK, identity, None, 0.0
        self._notify()
        return identity

    async def _try_refresh(self, token: str) -> Identity | None:
        try:
            return await self._refresh(self.session, token, self.cognito_url)  # type: ignore[arg-type]
        except Exception as raised:
            err = _as_auth_error(raised)
        if err.hold:  # the refresh token was refused: fall back to the password
            self.signins[("refresh", "rejected")] += 1
            log.warning("refresh token refused (%s); signing in with the password", err.code)
            return None
        self.signins[("refresh", "error")] += 1
        self._enter_backoff(err)
        raise AuthUnavailable("sign-in backing off", self._retry_at - self.clock.monotonic()) from None

    async def _srp(self) -> Identity:
        now = self.clock.monotonic()
        try:
            password, read_from = self.secret.read_with_fingerprint()
        except (OSError, UnicodeDecodeError) as err:
            self.state, self._hold_until = HOLD, float("inf")
            if isinstance(err, UnicodeDecodeError):  # never log the error itself: it quotes the password's bytes
                self.last_error, why = "password file is not UTF-8 text", "not UTF-8 text: save it as UTF-8"
            else:
                self.last_error, why = "password file unreadable", err.strerror or err
            log.error("cannot read the password file (%s); waiting for it to change", why)
            self._fingerprint = self.secret.fingerprint()
            raise AuthUnavailable(self.last_error, SECRET_CHECK_INTERVAL) from None
        self._on_secret(password)
        try:
            return await self._sign_in(self.session, self.username, password, self.cognito_url)  # type: ignore[arg-type]
        except Exception as raised:
            err = _as_auth_error(raised)
        self.current = None
        if err.hold:
            self.signins[("srp", "rejected")] += 1
            self.state, self._hold_until, self.last_error = HOLD, now + self.hold_seconds, err.code
            self._fingerprint = read_from  # the rejected file, not one written while Cognito was answering
            log.error(
                "sign-in rejected (%s): holding for %.0f h; rewrite the password file to retry at once",
                err.code, self.hold_seconds / 3600,
            )
            raise AuthUnavailable("credentials held", self.hold_seconds) from None
        self.signins[("srp", "error")] += 1
        self._enter_backoff(err)
        raise AuthUnavailable("sign-in backing off", self._retry_at - now) from None

    def _enter_backoff(self, err: AuthError) -> None:
        self._backoff = min(BACKOFF_MAX, max(BACKOFF_MIN, self._backoff * 2))
        delay = self._backoff * random.uniform(0.8, 1.2)
        self.state, self._retry_at, self.last_error = BACKOFF, self.clock.monotonic() + delay, err.code
        log.warning("sign-in failed (%s); retrying in %.0f s", err.code, delay)


def _as_auth_error(err: Exception) -> AuthError:
    """What a sign-in raised, as an AuthError. Anything unexpected is a reply that could not be understood:
    it backs off like a 5xx (no HOLD), and it never escapes get() past the rate limit and backoff."""
    return err if isinstance(err, AuthError) else AuthError(f"sign-in failed ({type(err).__name__})", "bad_reply")


class _NoIdentity:
    refresh_token = None


_NO = _NoIdentity()
