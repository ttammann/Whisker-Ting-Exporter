"""ASP.NET Core SignalR, MessagePack hub protocol, the parts Ting uses.

Spec: https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md

Wire format after the JSON handshake:
    <VarInt length><MessagePack array>  repeated, possibly several per WebSocket frame

Message arrays:
    Invocation  [1, headers, invocation_id, target, [args], [stream_ids]]
    Completion  [3, headers, invocation_id, result_kind, result?]
                result_kind 1 = error (result is the error text)
                            2 = void
                            3 = non-void (Ting sends 3 with a null result as its ack)
    Ping        [6]
    Close       [7, error?, allow_reconnect?]
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import msgpack

INVOCATION = 1
COMPLETION = 3
PING = 6
CLOSE = 7

RESULT_ERROR = 1
RECORD_SEPARATOR = b"\x1e"
HANDSHAKE_REQUEST = '{"protocol":"messagepack","version":1}\x1e'
MAX_VARINT_BYTES = 5


class ProtocolError(ValueError):
    """Malformed SignalR data."""


@dataclass(frozen=True)
class Completion:
    invocation_id: str
    error: str | None


@dataclass(frozen=True)
class Close:
    error: str | None
    allow_reconnect: bool


def encode_varint(value: int) -> bytes:
    if not 0 <= value <= 0x7FFFFFFF:
        raise ProtocolError(f"length out of range: {value}")
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def decode_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Return (value, offset after the VarInt)."""
    value = 0
    for i in range(MAX_VARINT_BYTES):
        pos = offset + i
        if pos >= len(data):
            raise ProtocolError("truncated length prefix")
        byte = data[pos]
        value |= (byte & 0x7F) << (7 * i)
        if not byte & 0x80:
            return value, pos + 1
    raise ProtocolError("length prefix longer than 5 bytes")


def unpack(blob: bytes) -> Any:
    """MessagePack to Python; anything that cannot become a Python value is a ProtocolError (a map key Python
    cannot hash is a TypeError, a timestamp past year 9999 an OverflowError)."""
    try:
        return msgpack.unpackb(blob, raw=False, strict_map_key=False, timestamp=3)
    except (ValueError, TypeError, OverflowError, msgpack.UnpackException) as err:
        raise ProtocolError(f"invalid MessagePack: {err}") from err


def decode_messages(data: bytes) -> list[list[Any]]:
    """Split one WebSocket binary frame into hub message arrays."""
    messages: list[list[Any]] = []
    offset = 0
    while offset < len(data):
        length, start = decode_varint(data, offset)
        end = start + length
        if length == 0 or end > len(data):
            raise ProtocolError(f"frame declares {length} bytes, {len(data) - start} available")
        message = unpack(data[start:end])
        if not isinstance(message, list) or not message:
            raise ProtocolError("hub message is not a non-empty array")
        messages.append(message)
        offset = end
    return messages


def frame(message: list[Any]) -> bytes:
    body = msgpack.packb(message, use_bin_type=True)
    return encode_varint(len(body)) + body


def encode_invocation(invocation_id: str, target: str, args: list[Any]) -> bytes:
    return frame([INVOCATION, {}, invocation_id, target, args, []])


def encode_ping() -> bytes:
    return frame([PING])


def parse_handshake(data: str | bytes) -> bytes | None:
    """Validate the handshake reply; return any hub bytes coalesced after it."""
    raw = data.encode() if isinstance(data, str) else data
    sep = raw.find(RECORD_SEPARATOR)
    if sep < 0:
        raise ProtocolError("handshake reply has no record separator")
    try:
        reply = json.loads(raw[:sep].decode())
    except (UnicodeError, json.JSONDecodeError) as err:
        raise ProtocolError("handshake reply is not JSON") from err
    if not isinstance(reply, dict):
        raise ProtocolError("handshake reply is not an object")
    if reply.get("error"):
        raise ProtocolError(f"handshake rejected: {reply['error']}")
    rest = raw[sep + 1 :]
    return rest or None


def as_completion(message: list[Any]) -> Completion | None:
    if message[0] != COMPLETION or len(message) < 4:
        return None
    error = None
    if message[3] == RESULT_ERROR:
        error = str(message[4]) if len(message) > 4 else "unspecified error"
    return Completion(invocation_id=str(message[2]), error=error)


def as_close(message: list[Any]) -> Close | None:
    if message[0] != CLOSE:
        return None
    error = message[1] if len(message) > 1 and isinstance(message[1], str) else None
    allow = bool(message[2]) if len(message) > 2 else False
    return Close(error=error, allow_reconnect=allow)


def as_invocation(message: list[Any]) -> tuple[str, list[Any]] | None:
    """Return (target, args) for a server-to-client Invocation."""
    if message[0] != INVOCATION or len(message) < 5:
        return None
    target, args = message[3], message[4]
    if not isinstance(target, str) or not isinstance(args, list):
        return None
    return target, args
