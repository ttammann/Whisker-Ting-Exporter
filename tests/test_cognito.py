"""SRP math checks, offline.

1. A minimal SRP-6a *server* built from the same Cognito conventions derives
   the same session key as our client. If any padding or hash rule were off,
   the keys would differ.
2. The client's signature matches pycognito (a widely used implementation)
   for identical inputs.
"""

import base64
import os
from datetime import datetime, timezone

import pytest

from ting_exporter.auth import cognito
from ting_exporter.auth.cognito import G, K, N, SrpClient, _hash_ints, _hkdf16, _sha256_hex, pad_hex

POOL = cognito.USER_POOL_ID
USER_ID_FOR_SRP = "a1b2c3d4-0000-4000-8000-123456789abc"
PASSWORD = "correct horse battery staple"


def server_side(A: int, salt_hex: str):
    pool_name = POOL.split("_", 1)[1]
    inner = _sha256_hex(f"{pool_name}{USER_ID_FOR_SRP}:{PASSWORD}".encode())
    x = int(_sha256_hex(bytes.fromhex(pad_hex(salt_hex) + inner)), 16)
    verifier = pow(G, x, N)
    b = int.from_bytes(os.urandom(64), "big")
    B = (K * verifier + pow(G, b, N)) % N
    u = _hash_ints(A, B)
    S = pow(A * pow(verifier, u, N), b, N)
    key = _hkdf16(bytes.fromhex(pad_hex(S)), bytes.fromhex(pad_hex(f"{u:x}")))
    return f"{B:x}", key


@pytest.mark.parametrize("run", range(20))
def test_client_and_server_agree(run):
    client = SrpClient()
    salt_hex = os.urandom(16).hex()
    srp_b, server_key = server_side(client.A, salt_hex)
    assert client.session_key(USER_ID_FOR_SRP, PASSWORD, salt_hex, srp_b) == server_key


def test_wrong_password_gives_different_key():
    client = SrpClient()
    salt_hex = os.urandom(16).hex()
    srp_b, server_key = server_side(client.A, salt_hex)
    assert client.session_key(USER_ID_FOR_SRP, "wrong", salt_hex, srp_b) != server_key


def test_constants_match_pycognito():
    aws_srp = pytest.importorskip("pycognito.aws_srp")
    assert N == int(aws_srp.N_HEX, 16)
    assert K == aws_srp.hex_to_long(aws_srp.hex_hash("00" + aws_srp.N_HEX + "0" + aws_srp.G_HEX))


def _pycognito_client(aws_srp, small_a):
    theirs = aws_srp.AWSSRP.__new__(aws_srp.AWSSRP)  # skip __init__: it wants boto3
    theirs.username, theirs.password = "someone@example.com", PASSWORD
    theirs.pool_id, theirs.client_id, theirs.client_secret = POOL, cognito.CLIENT_ID, None
    theirs.device_key = None
    theirs.big_n, theirs.val_g = N, G
    theirs.val_k = aws_srp.hex_to_long(aws_srp.hex_hash("00" + aws_srp.N_HEX + "0" + aws_srp.G_HEX))
    theirs.small_a_value, theirs.large_a_value = small_a, pow(G, small_a, N)
    return theirs


@pytest.mark.parametrize(
    "when",
    [datetime(2026, 9, 7, 3, 4, 5, tzinfo=timezone.utc), datetime(2026, 12, 25, 23, 59, 0, tzinfo=timezone.utc)],
)
def test_timestamp_format_matches_pycognito(when):
    aws_srp = pytest.importorskip("pycognito.aws_srp")
    assert cognito.cognito_timestamp(when) == aws_srp.AWSSRP.get_cognito_formatted_timestamp(when)


def test_challenge_response_matches_pycognito():
    aws_srp = pytest.importorskip("pycognito.aws_srp")
    small_a = int.from_bytes(os.urandom(128), "big") % N
    ours = SrpClient(small_a=small_a)
    theirs = _pycognito_client(aws_srp, small_a)
    timestamp = "Mon Sep 7 03:04:05 UTC 2026"
    theirs.get_cognito_formatted_timestamp = lambda _now: timestamp

    salt_hex = os.urandom(16).hex()
    srp_b, _ = server_side(ours.A, salt_hex)
    secret_block = base64.standard_b64encode(os.urandom(64)).decode()
    challenge = {"USER_ID_FOR_SRP": USER_ID_FOR_SRP, "SALT": salt_hex, "SRP_B": srp_b, "SECRET_BLOCK": secret_block}

    expected = theirs.process_challenge(challenge, {"USERNAME": "someone@example.com"})
    key = ours.session_key(USER_ID_FOR_SRP, PASSWORD, salt_hex, srp_b)
    assert ours.signature(key, USER_ID_FOR_SRP, secret_block, timestamp) == expected["PASSWORD_CLAIM_SIGNATURE"]
    assert ours.srp_a == f"{theirs.large_a_value:x}"
