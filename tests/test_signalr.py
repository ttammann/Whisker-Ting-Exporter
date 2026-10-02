import base64

import msgpack
import pytest

from ting_exporter.cloud import signalr


@pytest.mark.parametrize("n", [0, 1, 127, 128, 300, 16383, 16384, 2**31 - 1])
def test_varint_roundtrip(n):
    encoded = signalr.encode_varint(n)
    assert signalr.decode_varint(encoded, 0) == (n, len(encoded))


def test_varint_rejects_long_prefix():
    with pytest.raises(signalr.ProtocolError):
        signalr.decode_varint(b"\x80\x80\x80\x80\x80\x01", 0)


def test_several_messages_in_one_frame():
    data = signalr.frame([6]) + signalr.frame([1, {}, None, "x", [1]])
    assert signalr.decode_messages(data) == [[6], [1, {}, None, "x", [1]]]


@pytest.mark.parametrize("data", [b"\x05\x91", b"\x00", b"\x80"])
def test_truncated_or_empty_frame(data):
    with pytest.raises(signalr.ProtocolError):
        signalr.decode_messages(data)


def test_invocation_has_six_fields():
    body = signalr.encode_invocation("7", "InitializeStreaming", [{"StationId": "S"}, "k", "1"])
    [message] = signalr.decode_messages(body)
    assert message == [1, {}, "7", "InitializeStreaming", [{"StationId": "S"}, "k", "1"], []]


def test_captured_ting_ack_is_success():
    # Frame captured from the live hub: [3, {}, "1", 3, None]
    [message] = signalr.decode_messages(bytes.fromhex("079503 80a131 03c0".replace(" ", "")))
    completion = signalr.as_completion(message)
    assert completion == signalr.Completion("1", None)


def test_error_completion():
    completion = signalr.as_completion([3, {}, "2", 1, "not allowed"])
    assert completion.error == "not allowed"


@pytest.mark.parametrize("reply", ["{}\x1e", b"{}\x1e"])
def test_handshake_ok(reply):
    assert signalr.parse_handshake(reply) is None


def test_handshake_with_coalesced_message():
    tail = signalr.frame([6])
    assert signalr.parse_handshake(b"{}\x1e" + tail) == tail


def test_handshake_error():
    with pytest.raises(signalr.ProtocolError, match="rejected"):
        signalr.parse_handshake('{"error":"nope"}\x1e')


def test_close_message():
    close = signalr.as_close([7, "bye", True])
    assert close.error == "bye" and close.allow_reconnect


def test_ping_encoding():
    assert signalr.encode_ping() == b"\x02" + msgpack.packb([6])


UNCONVERTIBLE = [
    b"\x91\x81\x90\x00",  # [ {[]: 0} ]: a map key Python cannot hash (TypeError)
    b"\x91\xc7\x0c\xff" + bytes(4) + (2**62).to_bytes(8, "big"),  # [ a timestamp past year 9999 ] (OverflowError)
]


@pytest.mark.parametrize("body", UNCONVERTIBLE)
def test_msgpack_that_will_not_become_python_is_a_protocol_error(body):
    """unpack mapped only ValueError and msgpack's own errors; one such frame killed the receiver."""
    with pytest.raises(signalr.ProtocolError):
        signalr.decode_messages(signalr.encode_varint(len(body)) + body)


@pytest.mark.parametrize("body", UNCONVERTIBLE)
def test_a_blob_that_will_not_unpack_is_discarded_and_recorded_raw(body):
    from ting_exporter import recorder as record
    from ting_exporter.pipeline import decode

    with pytest.raises(decode.Discarded):
        decode.combo([body])
    assert record.jsonable([body]) == [{"b64": base64.standard_b64encode(body).decode()}]
