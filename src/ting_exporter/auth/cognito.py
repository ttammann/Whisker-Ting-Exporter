"""AWS Cognito sign-in for the Ting user pool (USER_SRP_AUTH or REFRESH_TOKEN_AUTH, then GetUser).

The password never leaves this process: SRP-6a proves knowledge of it.
The flow, all JSON POSTs to https://cognito-idp.us-east-1.amazonaws.com/:

1. InitiateAuth          AuthFlow USER_SRP_AUTH, sends USERNAME and SRP_A
2. RespondToAuthChallenge PASSWORD_VERIFIER, sends an HMAC signature over the
                          server's SECRET_BLOCK, keyed with the SRP session key
3. GetUser               AccessToken -> attributes custom:user_id, custom:api_key

Step 2 also returns a RefreshToken. InitiateAuth with AuthFlow
REFRESH_TOKEN_AUTH turns it into a new AccessToken without the password.

The stream and REST calls need only custom:user_id and custom:api_key.
The math matches amazon-cognito-identity-js (AuthenticationHelper.js).

Every failure is an AuthError with a `code` (Cognito's `__type`, or network /
http_5xx / ...) and a `hold` flag: True when retrying could lock the account.
A reply that does not have the expected shape (not UTF-8, not JSON, a field of
the wrong type, a value that does not decode) is "bad_reply", which backs off
like a 5xx: nothing else leaves sign_in() or refresh().
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import aiohttp

REGION = "us-east-1"
USER_POOL_ID = "us-east-1_trW4gH661"
CLIENT_ID = "4akjeqt9gtl8rgg1cksunipk9u"
ENDPOINT = f"https://cognito-idp.{REGION}.amazonaws.com/"
REQUEST_TIMEOUT = 15.0

# RFC 5054 3072-bit group, g = 2 (the group Cognito uses).
N_HEX = (
    "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B2251"
    "4A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C"
    "42E9A637ED6B0BFF5CB6F406B7EDEE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007C"
    "B8A163BF0598DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB9ED52907"
    "7096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3BE39E772C180E86039B2783A2EC"
    "07A28FB5C55DF06F4C52C9DE2BCBF6955817183995497CEA956AE515D2261898FA051015728E5A8AAA"
    "C42DAD33170D04507A33A85521ABDF1CBA64ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4"
    "C7ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6BF12FFA06D98A0864D87602733EC86A64"
    "521F2B18177B200CBBE117577A615D6C770988C0BAD946E208E24FA074E5AB3143DB5BFCE0FD108E4B"
    "82D120A93AD2CAFFFFFFFFFFFFFFFF"
)
N = int(N_HEX, 16)
G = 2

# Retrying these cannot lock the account, so they back off and retry.
TRANSIENT_CODES = frozenset(
    {"TooManyRequestsException", "LimitExceededException", "InternalErrorException", "network", "http_5xx", "bad_reply"}
)


class AuthError(Exception):
    """Sign-in failed. The message never contains credentials.

    `hold` is True for rejected credentials, an unexpected challenge (MFA),
    unknown 4xx codes and anything else where retrying could lock the account.
    """

    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self.code = code

    @property
    def hold(self) -> bool:
        return self.code not in TRANSIENT_CODES


@dataclass(frozen=True)
class Identity:
    user_id: str
    api_key: str = field(repr=False)
    access_token: str = field(repr=False)
    refresh_token: str | None = field(default=None, repr=False)

    def __repr__(self) -> str:
        return f"Identity(user_id={self.user_id!r})"


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pad_hex(value: int | str) -> str:
    """Hex with an even length and a leading 00 when the top bit is set.

    Cognito hashes big integers as two's-complement byte strings, so a value
    whose first byte is >= 0x80 needs a zero byte in front to stay positive.
    """
    text = value if isinstance(value, str) else f"{value:x}"
    if len(text) % 2:
        return "0" + text
    if text[0] in "89abcdefABCDEF":
        return "00" + text
    return text


def _hash_ints(*values: int | str) -> int:
    joined = "".join(pad_hex(v) for v in values)
    return int(_sha256_hex(bytes.fromhex(joined)), 16)


K = int(_sha256_hex(bytes.fromhex("00" + N_HEX + "0" + f"{G:x}")), 16)


def _hkdf16(ikm: bytes, salt: bytes) -> bytes:
    prk = hmac.new(salt, ikm, hashlib.sha256).digest()
    return hmac.new(prk, b"Caldera Derived Key\x01", hashlib.sha256).digest()[:16]


_DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def cognito_timestamp(now: datetime) -> str:
    """Format like 'Sun Sep 7 03:04:05 UTC 2026' (day not zero padded, English names)."""
    return (
        f"{_DAYS[now.weekday()]} {_MONTHS[now.month - 1]} {now.day} "
        f"{now.hour:02d}:{now.minute:02d}:{now.second:02d} UTC {now.year}"
    )


class SrpClient:
    def __init__(self, pool_id: str = USER_POOL_ID, small_a: int | None = None) -> None:
        self.pool_name = pool_id.split("_", 1)[1]
        self.a = small_a if small_a is not None else int.from_bytes(os.urandom(128), "big") % N
        self.A = pow(G, self.a, N)
        if self.A % N == 0:
            raise AuthError("SRP safety check failed", "srp")

    @property
    def srp_a(self) -> str:
        return f"{self.A:x}"

    def session_key(self, user_id_for_srp: str, password: str, salt_hex: str, srp_b_hex: str) -> bytes:
        B = int(srp_b_hex, 16)
        if B % N == 0:
            raise AuthError("SRP safety check failed", "srp")
        u = _hash_ints(self.A, B)
        if u == 0:
            raise AuthError("SRP safety check failed", "srp")
        inner = _sha256_hex(f"{self.pool_name}{user_id_for_srp}:{password}".encode())
        x = int(_sha256_hex(bytes.fromhex(pad_hex(salt_hex) + inner)), 16)
        S = pow(B - K * pow(G, x, N), self.a + u * x, N)
        return _hkdf16(bytes.fromhex(pad_hex(S)), bytes.fromhex(pad_hex(f"{u:x}")))

    def signature(self, key: bytes, user_id_for_srp: str, secret_block_b64: str, timestamp: str) -> str:
        message = (
            self.pool_name.encode()
            + user_id_for_srp.encode()
            + base64.standard_b64decode(secret_block_b64)
            + timestamp.encode()
        )
        return base64.standard_b64encode(hmac.new(key, message, hashlib.sha256).digest()).decode()


async def _call(session: aiohttp.ClientSession, endpoint: str, target: str, payload: dict[str, Any]) -> dict[str, Any]:
    headers = {
        "Content-Type": "application/x-amz-json-1.1",
        "X-Amz-Target": f"AWSCognitoIdentityProviderService.{target}",
    }
    try:
        async with session.post(
            endpoint, data=json.dumps(payload), headers=headers, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as resp:
            status, raw = resp.status, await resp.read()
    except (aiohttp.ClientError, asyncio.TimeoutError) as err:
        raise AuthError(f"{target} failed: {type(err).__name__}", "network") from None
    try:
        body = json.loads(raw.decode("utf-8"))
    except ValueError:  # UnicodeDecodeError or json.JSONDecodeError
        code = "http_5xx" if status >= 500 else "bad_reply"
        raise AuthError(f"{target} failed: HTTP {status}, non-JSON reply", code) from None
    if status != 200:
        code = str(body.get("__type", "")).rsplit("#", 1)[-1] if isinstance(body, dict) else ""
        if not code:
            code = "http_5xx" if status >= 500 else f"http_{status}"
        elif status >= 500 and code not in TRANSIENT_CODES:
            code = "http_5xx"
        raise AuthError(f"{target} failed: HTTP {status} {code}", code)
    if not isinstance(body, dict):
        raise AuthError(f"{target} returned a non-object", "bad_reply")
    return body


async def _identity(session: aiohttp.ClientSession, endpoint: str, access_token: str, refresh_token: str | None) -> Identity:
    user = await _call(session, endpoint, "GetUser", {"AccessToken": access_token})
    if not isinstance(user.get("UserAttributes"), list):
        raise AuthError("GetUser returned no attribute list", "bad_reply")
    attrs = {a.get("Name"): a.get("Value") for a in user["UserAttributes"] if isinstance(a, dict)}
    user_id, api_key = attrs.get("custom:user_id"), attrs.get("custom:api_key")
    if not user_id or not api_key:
        raise AuthError("account has no custom:user_id / custom:api_key attributes", "account")
    return Identity(user_id=str(user_id), api_key=api_key, access_token=access_token, refresh_token=refresh_token)


# What a reply of an unexpected shape raises while it is parsed (binascii.Error and UnicodeDecodeError are ValueErrors).
_MALFORMED = (ValueError, TypeError, AttributeError, LookupError)


async def sign_in(session: aiohttp.ClientSession, username: str, password: str, endpoint: str = ENDPOINT) -> Identity:
    try:
        return await _sign_in(session, username, password, endpoint)
    except _MALFORMED as err:  # never str(err): it could quote the reply or the password
        raise AuthError(f"sign-in reply not understood ({type(err).__name__})", "bad_reply") from None


async def _sign_in(session: aiohttp.ClientSession, username: str, password: str, endpoint: str) -> Identity:
    srp = SrpClient()
    init = await _call(
        session,
        endpoint,
        "InitiateAuth",
        {
            "AuthFlow": "USER_SRP_AUTH",
            "ClientId": CLIENT_ID,
            "AuthParameters": {"USERNAME": username, "SRP_A": srp.srp_a},
        },
    )
    if init.get("ChallengeName") != "PASSWORD_VERIFIER":
        raise AuthError(f"unexpected challenge {init.get('ChallengeName')!r}", "challenge")
    params = init.get("ChallengeParameters") or {}
    try:
        user_id_for_srp = params["USER_ID_FOR_SRP"]
        salt, srp_b, secret_block = params["SALT"], params["SRP_B"], params["SECRET_BLOCK"]
    except KeyError as err:
        raise AuthError(f"challenge missing {err.args[0]}", "bad_reply") from None

    timestamp = cognito_timestamp(datetime.now(timezone.utc))
    key = srp.session_key(user_id_for_srp, password, salt, srp_b)
    answer = await _call(
        session,
        endpoint,
        "RespondToAuthChallenge",
        {
            "ChallengeName": "PASSWORD_VERIFIER",
            "ClientId": CLIENT_ID,
            "ChallengeResponses": {
                "USERNAME": params.get("USERNAME", username),
                "PASSWORD_CLAIM_SECRET_BLOCK": secret_block,
                "PASSWORD_CLAIM_SIGNATURE": srp.signature(key, user_id_for_srp, secret_block, timestamp),
                "TIMESTAMP": timestamp,
            },
        },
    )
    if answer.get("ChallengeName"):
        raise AuthError(f"unsupported follow-up challenge {answer['ChallengeName']!r} (MFA?)", "challenge")
    result = answer.get("AuthenticationResult") or {}
    if not result.get("AccessToken"):
        raise AuthError("no access token in the challenge reply", "bad_reply")
    return await _identity(session, endpoint, result["AccessToken"], result.get("RefreshToken"))


async def refresh(session: aiohttp.ClientSession, refresh_token: str, endpoint: str = ENDPOINT) -> Identity:
    """New access token from a refresh token, no password involved."""
    try:
        return await _refresh(session, refresh_token, endpoint)
    except _MALFORMED as err:
        raise AuthError(f"refresh reply not understood ({type(err).__name__})", "bad_reply") from None


async def _refresh(session: aiohttp.ClientSession, refresh_token: str, endpoint: str) -> Identity:
    answer = await _call(
        session,
        endpoint,
        "InitiateAuth",
        {"AuthFlow": "REFRESH_TOKEN_AUTH", "ClientId": CLIENT_ID, "AuthParameters": {"REFRESH_TOKEN": refresh_token}},
    )
    access_token = (answer.get("AuthenticationResult") or {}).get("AccessToken")
    if not access_token:
        raise AuthError("no access token in the refresh reply", "bad_reply")
    # Cognito does not rotate the refresh token here unless rotation is enabled; keep whichever is newest.
    new_refresh = (answer.get("AuthenticationResult") or {}).get("RefreshToken") or refresh_token
    return await _identity(session, endpoint, access_token, new_refresh)
