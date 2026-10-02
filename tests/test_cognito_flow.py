"""Cognito sign-in, refresh and error classification against FakeCognito, plus device discovery."""

import json

import aiohttp
import pytest
from aiohttp import web

from ting_exporter.auth import cognito
from ting_exporter.cloud import api

from . import fakes

PASSWORD = "s3cret-Password!"


@pytest.fixture
async def cloud():
    fake = fakes.FakeCognito(PASSWORD)
    app = web.Application()
    app.router.add_post("/cognito/", fake.handle)
    app.router.add_get("/api/v1/Users/{user_id}", fakes.fake_users)
    app.router.add_get("/api/v1/Notifications/history/{user_id}", fakes.fake_notifications)
    app.router.add_get("/api/v3/Devices/{serial}/voltage/dateRange", fakes.fake_voltage_history)
    runner, base = await fakes.start_app(app)
    async with aiohttp.ClientSession() as session:
        yield fake, session, base
    await runner.cleanup()


async def test_sign_in_and_discover(cloud):
    fake, session, base = cloud
    identity = await cognito.sign_in(session, "me@example.com", PASSWORD, endpoint=f"{base}/cognito/")
    assert (identity.user_id, identity.api_key, identity.refresh_token) == (fakes.USER_ID, fakes.API_KEY, fakes.REFRESH_TOKEN)
    devices = await api.list_devices(session, identity, base_url=base)
    assert [d.serial for d in devices] == [fakes.SERIAL_A, fakes.SERIAL_B]
    assert devices[0].firmware == "SparkFault 2.6.17"
    assert fake.calls == ["srp", "verifier", "getuser"]


async def test_a_device_list_that_is_not_an_object_is_an_api_error():
    """Discovery retries an ApiError; an AttributeError would crash its task, an empty list mean no sensors."""
    async def users(_request):
        return web.json_response(["not", "an", "object"])

    app = web.Application()
    app.router.add_get("/api/v1/Users/{user_id}", users)
    runner, base = await fakes.start_app(app)
    identity = cognito.Identity(user_id=fakes.USER_ID, api_key=fakes.API_KEY, access_token=fakes.ACCESS_TOKEN)
    async with aiohttp.ClientSession() as session:
        with pytest.raises(api.ApiError, match="not an object"):
            await api.list_devices(session, identity, base_url=base)
    await runner.cleanup()
    assert api.parse_devices(["x"]) == [] and api.parse_devices(None) == []


async def test_wrong_password_is_a_hold(cloud):
    _, session, base = cloud
    with pytest.raises(cognito.AuthError) as err:
        await cognito.sign_in(session, "me@example.com", "wrong", endpoint=f"{base}/cognito/")
    assert err.value.code == "NotAuthorizedException" and err.value.hold
    assert "wrong" not in str(err.value)


@pytest.mark.parametrize(
    ("status", "code", "hold"),
    [
        (400, "NotAuthorizedException", True),
        (400, "UserNotFoundException", True),
        (400, "PasswordResetRequiredException", True),
        (400, "UserNotConfirmedException", True),
        (400, "SomethingNewException", True),  # unknown 4xx: do not retry into a lockout
        (400, "TooManyRequestsException", False),
        (400, "LimitExceededException", False),
        (500, "InternalErrorException", False),
        (503, "ServiceUnavailable", False),
    ],
)
async def test_error_classification(cloud, status, code, hold):
    fake, session, base = cloud
    fake.fail = (status, code)
    with pytest.raises(cognito.AuthError) as err:
        await cognito.sign_in(session, "me@example.com", PASSWORD, endpoint=f"{base}/cognito/")
    assert err.value.hold is hold


async def test_network_error_is_transient():
    async with aiohttp.ClientSession() as session:
        with pytest.raises(cognito.AuthError) as err:
            await cognito.sign_in(session, "me@example.com", PASSWORD, endpoint="http://127.0.0.1:9/")
    assert err.value.code == "network" and not err.value.hold


async def test_refresh_token(cloud):
    fake, session, base = cloud
    identity = await cognito.refresh(session, fakes.REFRESH_TOKEN, endpoint=f"{base}/cognito/")
    assert identity.api_key == fakes.API_KEY and identity.refresh_token == fakes.REFRESH_TOKEN
    fake.refresh_valid = False
    with pytest.raises(cognito.AuthError) as err:
        await cognito.refresh(session, fakes.REFRESH_TOKEN, endpoint=f"{base}/cognito/")
    assert err.value.code == "NotAuthorizedException"


def test_identity_repr_hides_secrets():
    identity = cognito.Identity("42", "api-key-secret", "token-secret", "refresh-secret")
    assert "secret" not in repr(identity) and "secret" not in str(identity)


async def test_api_unauthorized_is_classified(cloud):
    _, session, base = cloud
    stale = cognito.Identity(fakes.USER_ID, fakes.API_KEY, "expired")
    with pytest.raises(api.ApiError) as err:
        await api.list_devices(session, stale, base_url=base)
    assert err.value.unauthorized


async def test_notification_history(cloud):
    fake, session, base = cloud
    identity = await cognito.sign_in(session, "me@example.com", PASSWORD, endpoint=f"{base}/cognito/")
    history = await api.list_notifications(session, identity, base_url=base)
    assert [r["eventType"] for r in history] == ["PowerRestored", "CommunityPowerOutage", "Sag"]
    stale = cognito.Identity(identity.user_id, identity.api_key, "expired-token", identity.refresh_token)
    with pytest.raises(api.ApiError) as err:
        await api.list_notifications(session, stale, base_url=base)
    assert err.value.unauthorized and fakes.USER_ID not in str(err.value)  # the user id never appears in errors


async def test_voltage_history_request_and_limits(cloud):
    from datetime import datetime, timedelta, timezone

    fake, session, base = cloud
    identity = await cognito.sign_in(session, "me@example.com", PASSWORD, endpoint=f"{base}/cognito/")
    local = timezone(timedelta(hours=-7))
    data = await api.get_voltage_history(session, identity, fakes.SERIAL_A, datetime(2026, 3, 11, 15, 0, tzinfo=local),
                                         datetime(2026, 3, 11, 21, 0, tzinfo=local), base_url=base)
    assert data["unit"] == "V" and len(data["data"]) == 3
    assert fakes.VOLTAGE_QUERIES[-1] == {"serial": fakes.SERIAL_A, "startUtc": "2026-03-11T22:00:00+00:00",
                                         "endUtc": "2026-03-12T04:00:00+00:00"}
    for start, end in ((datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 3, 3, tzinfo=timezone.utc)),
                       (datetime(2026, 3, 1), datetime(2026, 3, 1, 12))):
        with pytest.raises(ValueError):
            await api.get_voltage_history(session, identity, "S", start, end, base_url=base)


async def test_a_forbidden_resource_is_not_treated_as_a_stale_identity(cloud):
    """403 (the account may not use the endpoint) must not renew the identity: that would spend the sign-in budget."""
    from ting_exporter.auth.identity import IdentityManager, SecretFile
    from ting_exporter.serve import with_renewal

    fake, session, base = cloud
    calls = []

    async def forbidden(identity):
        calls.append(identity)
        raise api.ApiError("GET /api/v3/... failed: HTTP 403", 403)

    async def expired_then_ok(identity):
        calls.append(identity)
        if len(calls) == 1:
            raise api.ApiError("GET ... failed: HTTP 401", 401)
        return "ok"

    secret = __import__("pathlib").Path(__import__("tempfile").mkdtemp()) / "pw"
    secret.write_text(PASSWORD)
    ids = IdentityManager(session, "me@example.com", SecretFile(secret), cognito_url=f"{base}/cognito/")
    with pytest.raises(api.ApiError) as err:
        await with_renewal(ids, forbidden)
    assert err.value.forbidden and not err.value.unauthorized and len(calls) == 1
    assert sum(n for (m, r), n in ids.signins.items() if r == "ok") == 1  # the initial sign-in only
    calls.clear()
    assert await with_renewal(ids, expired_then_ok) == "ok" and len(calls) == 2
    assert calls[0] is not calls[1]  # renewed once on 401


def _challenge(**params):
    return lambda reply: web.json_response({**reply, "ChallengeParameters": {**reply["ChallengeParameters"], **params}})


@pytest.mark.parametrize("target, mangle, code", [
    ("InitiateAuth", _challenge(SRP_B="zz"), "bad_reply"),  # int(x, 16)
    ("InitiateAuth", _challenge(SALT="xyz"), "bad_reply"),  # bytes.fromhex
    ("InitiateAuth", _challenge(SECRET_BLOCK="not base64!"), "bad_reply"),  # b64decode
    ("InitiateAuth", lambda reply: web.json_response({**reply, "ChallengeParameters": ["a", "list"]}), "bad_reply"),
    ("RespondToAuthChallenge", lambda reply: web.json_response({"AuthenticationResult": ["a", "list"]}), "bad_reply"),
    ("GetUser", lambda reply: web.json_response({"UserAttributes": None}), "bad_reply"),
    ("GetUser", lambda reply: web.Response(status=503, body="Dienst nicht verfügbar".encode("latin-1"), content_type="text/plain"), "http_5xx"),
    ("GetUser", lambda reply: web.Response(body=b'{"UserAttributes": "\xff"}', content_type="application/x-amz-json-1.1"), "bad_reply"),
])
async def test_a_reply_cognito_would_never_send_is_a_transient_auth_error(target, mangle, code):
    """Malformed replies back off like a 5xx (no HOLD, no exception the IdentityManager would not handle)."""
    fake = fakes.FakeCognito(PASSWORD)

    async def handle(request):
        resp = await fake.handle(request)
        return mangle(json.loads(resp.body)) if request.headers["X-Amz-Target"].endswith("." + target) else resp

    app = web.Application()
    app.router.add_post("/cognito/", handle)
    runner, base = await fakes.start_app(app)
    try:
        async with aiohttp.ClientSession() as session:
            with pytest.raises(cognito.AuthError) as err:
                await cognito.sign_in(session, "me@example.com", PASSWORD, endpoint=f"{base}/cognito/")
    finally:
        await runner.cleanup()
    assert err.value.code == code and not err.value.hold and PASSWORD not in str(err.value)


async def test_a_malformed_refresh_reply_is_a_transient_auth_error():
    async def handle(_request):
        return web.json_response({"AuthenticationResult": "a string"})

    app = web.Application()
    app.router.add_post("/cognito/", handle)
    runner, base = await fakes.start_app(app)
    try:
        async with aiohttp.ClientSession() as session:
            with pytest.raises(cognito.AuthError) as err:
                await cognito.refresh(session, fakes.REFRESH_TOKEN, endpoint=f"{base}/cognito/")
    finally:
        await runner.cleanup()
    assert err.value.code == "bad_reply" and not err.value.hold
