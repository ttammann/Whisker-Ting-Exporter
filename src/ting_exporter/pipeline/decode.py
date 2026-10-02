"""Turn Ting hub invocations into readings, driven by the signal registry.

updateComboBinaryData  (DataElement "ComboBinaryData", 4 per second)
    args[0] is a map (in practice always, design 2.4), a MessagePack blob that
    decodes to one, or {"msgpack": map} as a record file stores such a blob:
    {"Voltage": RMS volts, "VoltageHi"/"VoltageLo": the sensor's long-window
     extremes, "AveragePeaksMax": Hi-Fi, "DataTimeUtc": sample time}

updateGraphMultiCategorical  (DataElements "frequency", "thdMin", "thdAvg", "thdMax")
    args[0] is a list of records {"Category": name, "Value": number as a string,
    "ObsTime": time}. THD values are ratios (0.03 = 3 %).

Times arrive as MessagePack timestamps (datetime after unpacking); record files
hold them as ISO strings. Both work. Field names come from the community
integrations and the capture; Whisker Labs documents none of this.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import msgpack

from .. import signals
from ..cloud.signalr import ProtocolError, unpack
from ..signals import Signal

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class Discarded(Exception):
    """A payload arrived but held no usable reading."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class Reading:
    signal: Signal
    value: float
    device_ms: int


def _finite(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError("bool is not a number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("not finite")
    return number


def parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, msgpack.Timestamp):
        return value.to_datetime()
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        if "." in text:  # .NET sends 7 fractional digits; Python takes at most 6
            head, rest = text.split(".", 1)
            digits = len(rest) - len(rest.lstrip("0123456789"))
            text = f"{head}.{rest[:digits][:6].ljust(6, '0')}{rest[digits:]}"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def to_ms(when: datetime) -> int:
    """Milliseconds since the epoch, rounded half up, in integer arithmetic."""
    delta = when - EPOCH
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return (micros + 500) // 1000


def _payload(args: list[Any]) -> dict[str, Any]:
    if not args:
        raise Discarded("no_payload")
    payload = args[0]
    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = unpack(bytes(payload))
        except ProtocolError:
            raise Discarded("no_payload") from None
    elif isinstance(payload, dict) and set(payload) == {"msgpack"}:
        payload = payload["msgpack"]  # a blob as record.jsonable stores it
    if not isinstance(payload, dict):
        raise Discarded("no_payload")
    return payload


def combo(args: list[Any]) -> tuple[list[Reading], list[str]]:
    """Readings from one updateComboBinaryData, plus soft discard reasons.

    Raises Discarded when a required field or the timestamp is unusable.
    Optional fields that are missing are omitted (never defaulted); invalid
    ones are omitted and reported as `bad_<key>` / `implausible_<key>`.
    """
    payload = _payload(args)
    readings: list[Reading] = []
    soft: list[str] = []
    when = parse_time(payload.get(signals.COMBO_TIME_FIELD))
    for signal in signals.COMBO_SIGNALS:
        if signal.field not in payload:
            if signal.required:
                raise Discarded(f"bad_{signal.key}")
            continue
        try:
            value = _finite(payload[signal.field])
        except (TypeError, ValueError):
            if signal.required:
                raise Discarded(f"bad_{signal.key}") from None
            soft.append(f"bad_{signal.key}")
            continue
        if not signal.check(value):
            if signal.required:
                raise Discarded(f"implausible_{signal.key}")
            soft.append(f"implausible_{signal.key}")
            continue
        readings.append(Reading(signal, value, 0))
    if when is None:
        raise Discarded("no_timestamp")
    ms = to_ms(when)
    return [Reading(r.signal, r.value, ms) for r in readings], soft


def categorical(args: list[Any]) -> tuple[list[Reading], list[str]]:
    """Readings from one updateGraphMultiCategorical, plus discard reasons per bad record."""
    if not args:
        raise Discarded("no_payload")
    records = args[0]
    if isinstance(records, (bytes, bytearray)):
        try:
            records = unpack(bytes(records))
        except ProtocolError:
            raise Discarded("no_payload") from None
    elif isinstance(records, dict) and set(records) == {"msgpack"}:
        records = records["msgpack"]
    if isinstance(records, dict):
        records = [records]
    if not isinstance(records, list):
        raise Discarded("no_payload")
    readings: list[Reading] = []
    soft: list[str] = []
    for record in records:
        if not isinstance(record, dict):
            soft.append("bad_record")
            continue
        signal = signals.CATEGORICAL_SIGNALS.get(record.get("Category"))  # type: ignore[arg-type]
        if signal is None:
            soft.append("unknown_category")
            continue
        try:
            value = _finite(record.get("Value"))
        except (TypeError, ValueError):
            soft.append(f"bad_{signal.key}")
            continue
        if not signal.check(value):
            soft.append(f"implausible_{signal.key}")
            continue
        when = parse_time(record.get(signals.CATEGORICAL_TIME_FIELD))
        if when is None:
            soft.append("no_timestamp")
            continue
        readings.append(Reading(signal, value, to_ms(when)))
    return readings, soft
