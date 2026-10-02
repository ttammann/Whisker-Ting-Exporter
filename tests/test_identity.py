"""IdentityManager policy with a fake clock and fake Cognito calls (no network)."""

import asyncio
import os

import pytest

from ting_exporter.auth.cognito import AuthError, Identity
from ting_exporter.auth.identity import BACKOFF, HOLD, OK, AuthUnavailable, IdentityManager, SecretFile
from ting_exporter.clock import Clock


class FakeClock(Clock):
    def __init__(self):
        self.t = 1000.0

    def monotonic(self):
        return self.t

    def time(self):
        return 1_790_000_000 + self.t

    def real(self, seconds):
        return 0.0


class FakeCognito:
    """Records every sign-in; the password decides the outcome."""

    def __init__(self, good="right", issue_refresh=True):
        self.good = good
        self.issue_refresh = issue_refresh
        self.calls: list[str] = []
        self.error: AuthError | None = None
        self.refresh_ok = True
        self.gate: asyncio.Event | None = None

    async def sign_in(self, session, username, password, endpoint):
        self.calls.append("srp")
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        if password != self.good:
            raise AuthError("InitiateAuth failed: HTTP 400 NotAuthorizedException", "NotAuthorizedException")
        return Identity("42", "api", f"token-{len(self.calls)}", "refresh" if self.issue_refresh else None)

    async def refresh(self, session, token, endpoint):
        self.calls.append("refresh")
        if not self.refresh_ok:
            raise AuthError("refused", "NotAuthorizedException")
        return Identity("42", "api", f"token-r{len(self.calls)}", token)


@pytest.fixture
def setup(tmp_path):
    secret = tmp_path / "ting_password"
    secret.write_text("right\n")
    clock, fake = FakeClock(), FakeCognito()
    seen = []
    mgr = IdentityManager(None, "me@example.com", SecretFile(secret), clock=clock, sign_in=fake.sign_in, refresh=fake.refresh, on_secret=seen.append)
    return mgr, clock, fake, secret, seen


async def test_signs_in_once_and_reuses(setup):
    mgr, _, fake, _, seen = setup
    a = await mgr.get()
    b = await mgr.get()
    assert a is b and fake.calls == ["srp"] and mgr.state == OK
    assert "right" in seen and "api" in seen  # registered with the log scrubber


async def test_concurrent_callers_share_one_sign_in(setup):
    mgr, _, fake, _, _ = setup
    fake.gate = asyncio.Event()
    tasks = [asyncio.create_task(mgr.get()) for _ in range(5)]
    await asyncio.sleep(0.01)
    fake.gate.set()
    results = await asyncio.gather(*tasks)
    assert fake.calls == ["srp"] and len({id(r) for r in results}) == 1


async def test_wrong_password_one_attempt_per_hold_then_retry_on_file_change(setup):
    """exactly 1 attempt in 6 simulated hours, 1 more right after the password file changes."""
    mgr, clock, fake, secret, _ = setup
    secret.write_text("wrong\n")
    mgr.check_secret()
    for _ in range(6 * 12):  # a session asking every 5 minutes for 6 h
        with pytest.raises(AuthUnavailable):
            await mgr.get()
        mgr.check_secret()
        clock.t += 299
    assert fake.calls == ["srp"] and mgr.state == HOLD and mgr.state_value == 2
    secret.write_text("right\n")
    assert mgr.check_secret()
    identity = await mgr.get()
    assert fake.calls == ["srp", "srp"] and mgr.state == OK and identity.user_id == "42"


async def test_a_password_file_replaced_by_an_editor_lifts_the_hold(setup):
    """vim, sed -i and mv write a new file and rename it over the old one. The file is read by path, so with
    its directory mounted (deploy/), the new file is the one checked and read."""
    mgr, _, fake, secret, _ = setup
    secret.write_text("wrong\n")
    mgr.check_secret()
    with pytest.raises(AuthUnavailable):
        await mgr.get()
    assert mgr.state == HOLD
    swap = secret.with_name(".ting_password.swp")
    swap.write_text("right\n")
    os.replace(swap, secret)
    assert mgr.check_secret()
    assert (await mgr.get()).user_id == "42" and fake.calls == ["srp", "srp"]


async def test_a_password_file_that_is_not_utf8_holds_instead_of_crashing(setup):
    mgr, _, fake, secret, _ = setup
    secret.write_bytes("pässword".encode("latin-1"))
    with pytest.raises(AuthUnavailable):
        await mgr.get()
    assert mgr.state == HOLD and mgr.state_value == 2 and "UTF-8" in mgr.last_error and fake.calls == []
    secret.write_text("right\n")
    assert mgr.check_secret()
    assert (await mgr.get()).user_id == "42"


async def test_an_unexpected_sign_in_failure_backs_off_instead_of_retrying_at_once(setup):
    """A reply the client cannot parse used to escape get(): every caller retried a full sign-in at once."""
    mgr, clock, fake, _, _ = setup
    fake.error = ValueError("non-hexadecimal number found in fromhex() arg")
    for _ in range(180):  # 30 simulated minutes of callers asking every 10 s
        with pytest.raises(AuthUnavailable):
            await mgr.get()
        clock.t += 10
    assert mgr.state == BACKOFF and mgr.last_error == "bad_reply"
    assert 3 <= fake.calls.count("srp") <= 7  # 30 s, 60 s, 120 s, ... not 180


async def test_an_unexpected_refresh_failure_backs_off(setup):
    mgr, _, fake, _, _ = setup
    identity = await mgr.get()

    async def broken(session, token, endpoint):
        raise TypeError("'str' object has no attribute 'get'")

    mgr._refresh = broken
    with pytest.raises(AuthUnavailable):
        await mgr.get(stale=identity)
    assert mgr.state == BACKOFF and mgr.last_error == "bad_reply"


async def test_a_renewed_access_token_retires_the_old_one_from_the_scrubber(tmp_path):
    secret = tmp_path / "ting_password"
    secret.write_text("right\n")
    fake, retired = FakeCognito(), []
    mgr = IdentityManager(None, "me@example.com", SecretFile(secret), clock=FakeClock(), sign_in=fake.sign_in,
                          refresh=fake.refresh, on_secret=lambda _v: None, on_retire=retired.append)
    first = await mgr.get()
    second = await mgr.get(stale=first)
    assert second.access_token != first.access_token and retired == [first.access_token]  # the refresh token is unchanged


async def test_a_password_fixed_while_the_sign_in_is_in_flight_is_tried_at_once(setup):
    """the file's fingerprint was taken when the rejection came back, so a password corrected during the
    0.3-2 s sign-in counted as the known-bad one and waited out the 6 h hold."""
    mgr, _, fake, secret, _ = setup
    secret.write_text("wrong\n")
    mgr.check_secret()
    real_sign_in = fake.sign_in

    async def fixed_meanwhile(session, username, password, endpoint):
        secret.write_text("right\n")  # the owner fixes the file while Cognito is still answering
        return await real_sign_in(session, username, password, endpoint)

    mgr._sign_in = fixed_meanwhile
    with pytest.raises(AuthUnavailable):
        await mgr.get()
    assert mgr.state == HOLD
    mgr._sign_in = real_sign_in
    assert mgr.check_secret()  # the file differs from the password that was rejected
    assert (await mgr.get()).user_id == "42"


async def test_hold_expires_after_six_hours(setup):
    mgr, clock, fake, secret, _ = setup
    fake.good = "other"
    for _ in range(3):
        with pytest.raises(AuthUnavailable):
            await mgr.get()
        clock.t += 21_600
    assert fake.calls == ["srp"] * 3  # one automatic attempt per 6 h


async def test_throttling_backs_off_30s_to_15min(setup):
    mgr, clock, fake, _, _ = setup
    fake.error = AuthError("slow down", "TooManyRequestsException")
    delays = []
    for _ in range(8):
        with pytest.raises(AuthUnavailable) as err:
            await mgr.get()
        delays.append(err.value.retry_in)
        clock.t += err.value.retry_in + 0.001
    assert mgr.state == BACKOFF and mgr.state_value == 1
    assert 24 <= delays[0] <= 36 and 720 <= delays[-1] <= 1080
    assert all(b >= a * 0.66 for a, b in zip(delays, delays[1:]))
    assert len(fake.calls) == 8


async def test_rate_limit_six_successful_sign_ins_per_hour(setup):
    mgr, clock, fake, _, _ = setup
    fake.issue_refresh = False
    identity = await mgr.get()
    for _ in range(5):
        identity = await mgr.get(stale=identity)  # hub refusals
        clock.t += 1
    with pytest.raises(AuthUnavailable) as err:
        await mgr.get(stale=identity)
    assert err.value.reason == "rate limited" and fake.calls.count("srp") == 6
    clock.t += err.value.retry_in
    await mgr.get(stale=identity)
    assert fake.calls.count("srp") == 7


async def test_refusal_uses_refresh_token_first(setup):
    mgr, _, fake, _, _ = setup
    first = await mgr.get()
    renewed = await mgr.get(stale=first)
    assert fake.calls == ["srp", "refresh"] and renewed is not first
    assert mgr.signins[("refresh", "ok")] == 1


async def test_refused_refresh_token_falls_back_to_srp(setup):
    mgr, _, fake, _, _ = setup
    first = await mgr.get()
    fake.refresh_ok = False
    await mgr.get(stale=first)
    assert fake.calls == ["srp", "refresh", "srp"]
    assert mgr.signins[("refresh", "rejected")] == 1


async def test_stale_identity_that_is_no_longer_current_is_not_renewed(setup):
    mgr, _, fake, _, _ = setup
    first = await mgr.get()
    second = await mgr.get(stale=first)
    assert await mgr.get(stale=first) is second  # another session already renewed it
    assert fake.calls == ["srp", "refresh"]


async def test_unreadable_password_file_waits_for_it(setup):
    mgr, _, fake, secret, _ = setup
    secret.unlink()
    with pytest.raises(AuthUnavailable):
        await mgr.get()
    with pytest.raises(AuthUnavailable):
        await mgr.get()
    assert fake.calls == [] and mgr.state == HOLD
    secret.write_text("right")
    assert mgr.check_secret()
    await mgr.get()
    assert fake.calls == ["srp"]


async def test_wait_returns_when_the_state_changes(setup):
    mgr, _, _, secret, _ = setup
    mgr.clock = Clock()  # real time for this one
    secret.write_text("wrong")
    mgr.check_secret()
    with pytest.raises(AuthUnavailable):
        await mgr.get()
    waiter = asyncio.create_task(mgr.wait(3600))
    await asyncio.sleep(0.01)
    secret.write_text("right")
    mgr.check_secret()
    await asyncio.wait_for(waiter, 1)


def test_reprs_hide_secrets(setup):
    mgr, _, _, secret, _ = setup
    assert "right" not in repr(SecretFile(secret))
    assert "api" not in repr(Identity("42", "api-key", "tok", "ref"))
