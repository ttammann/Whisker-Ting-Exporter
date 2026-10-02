"""Decoding hub payloads. The payloads are copied verbatim from the fixture recordings (tests/fixtures)."""

import json
from datetime import datetime, timezone

import msgpack
import pytest

from ting_exporter import signals
from ting_exporter.pipeline import decode

from .fakes import to_wire

# TNG000002's first sample in a-subscribe, as the recorder writes it (times are ISO strings in record files)
COMBO_LINE = '{"t":1773172800.68115,"serial":"TNG000002","kind":"invocation","target":"updateComboBinaryData","args":[{"DataTimeUtc":"2026-03-10T19:59:54.656300+00:00","Voltage":120.77457563869298,"AveragePeaksMax":30.0,"VoltageHi":124.6181,"VoltageLo":118.0436}]}'
THD_LINE = '{"t":1773172800.6859999,"serial":"TNG000002","kind":"invocation","target":"updateGraphMultiCategorical","args":[[{"Category":"thdMin","ObsTime":"2026-03-10T19:59:54.656300+00:00","Value":"0.051098356935186245"},{"Category":"thdAvg","ObsTime":"2026-03-10T19:59:54.656300+00:00","Value":"0.052511714106076204"},{"Category":"thdMax","ObsTime":"2026-03-10T19:59:54.656300+00:00","Value":"0.054207742711144154"}]]}'
FREQ_LINE = '{"t":1773172800.6810198,"serial":"TNG000002","kind":"invocation","target":"updateGraphMultiCategorical","args":[[{"Category":"frequency","ObsTime":"2026-03-10T19:59:54.656300+00:00","Value":"59.99845932201637"}]]}'
TS_MS = 1773172794656  # 19:59:54.656300 rounded to the millisecond


def args(line):
    return json.loads(line)["args"]


def by_key(readings):
    return {r.signal.key: r for r in readings}


@pytest.mark.parametrize("wire", [False, True], ids=["record-file", "live-hub"])
def test_combo_map(wire):
    payload = to_wire(args(COMBO_LINE)) if wire else args(COMBO_LINE)
    if wire:
        assert isinstance(payload[0]["DataTimeUtc"], datetime)  # the live hub's shape
    readings, soft = decode.combo(payload)
    got = by_key(readings)
    assert soft == []
    assert set(got) == {"voltage", "hifi", "rolling_high", "rolling_low"}
    assert got["voltage"].value == 120.77457563869298 and got["voltage"].device_ms == TS_MS
    assert got["hifi"].value == 30.0
    assert got["rolling_low"].value == 118.0436


def test_combo_blob_and_recorded_blob():
    payload = to_wire(args(COMBO_LINE))[0]
    blob = msgpack.packb(payload, datetime=True)
    assert by_key(decode.combo([blob])[0])["voltage"].device_ms == TS_MS
    assert by_key(decode.combo([{"msgpack": args(COMBO_LINE)[0]}])[0])["voltage"].value == 120.77457563869298


def test_missing_hi_lo_are_omitted_not_defaulted():
    payload = {"Voltage": 120.5, "DataTimeUtc": "2026-03-10T20:01:07.943396+00:00"}
    readings, soft = decode.combo([payload])
    assert [r.signal.key for r in readings] == ["voltage"] and soft == []


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({"Voltage": 750000, "DataTimeUtc": "2026-03-10T20:01:07Z"}, "implausible_voltage"),
        ({"Voltage": -120, "DataTimeUtc": "2026-03-10T20:01:07Z"}, "implausible_voltage"),
        ({"Voltage": 0, "DataTimeUtc": "2026-03-10T20:01:07Z"}, "implausible_voltage"),
        ({"Voltage": "x", "DataTimeUtc": "2026-03-10T20:01:07Z"}, "bad_voltage"),
        ({"Voltage": True, "DataTimeUtc": "2026-03-10T20:01:07Z"}, "bad_voltage"),
        ({"Voltage": float("nan"), "DataTimeUtc": "2026-03-10T20:01:07Z"}, "bad_voltage"),
        ({"Other": 1}, "bad_voltage"),
        ({"Voltage": 120.0}, "no_timestamp"),
        ({"Voltage": 120.0, "DataTimeUtc": "yesterday"}, "no_timestamp"),
    ],
)
def test_combo_discards(payload, reason):
    with pytest.raises(decode.Discarded) as err:
        decode.combo([payload])
    assert err.value.reason == reason


@pytest.mark.parametrize("bad", [[], [b"\xc1"], [[1, 2]], ["text"]])
def test_combo_no_payload(bad):
    with pytest.raises(decode.Discarded) as err:
        decode.combo(bad)
    assert err.value.reason == "no_payload"


def test_optional_field_invalid_is_reported_but_voltage_kept():
    payload = {"Voltage": 120.0, "AveragePeaksMax": -3, "VoltageHi": "n/a", "DataTimeUtc": "2026-03-10T20:01:07Z"}
    readings, soft = decode.combo([payload])
    assert [r.signal.key for r in readings] == ["voltage"]
    assert sorted(soft) == ["bad_rolling_high", "implausible_hifi"]


@pytest.mark.parametrize("wire", [False, True], ids=["record-file", "live-hub"])
def test_categorical_thd_and_frequency(wire):
    thd = to_wire(args(THD_LINE)) if wire else args(THD_LINE)
    readings, soft = decode.categorical(thd)
    got = by_key(readings)
    assert soft == [] and set(got) == {"thd", "thd_min", "thd_max"}
    assert got["thd"].value == 0.052511714106076204 and got["thd"].device_ms == TS_MS
    [freq], _ = decode.categorical(args(FREQ_LINE))
    assert freq.signal.metric == "ting_frequency_hertz" and freq.value == 59.99845932201637


def test_categorical_unknown_bad_and_untimed_records():
    records = [
        {"Category": "frequency", "Value": "60.01", "ObsTime": "2026-03-10T20:00:00Z"},
        {"Category": "FutureMetric", "Value": "1", "ObsTime": "2026-03-10T20:00:00Z"},
        {"Category": "thdMax", "Value": "nan", "ObsTime": "2026-03-10T20:00:00Z"},
        {"Category": "thdAvg", "Value": "0.031"},
        "junk",
    ]
    readings, soft = decode.categorical([records])
    assert [(r.signal.key, r.value) for r in readings] == [("frequency", 60.01)]
    assert sorted(soft) == ["bad_record", "bad_thd_max", "no_timestamp", "unknown_category"]


def test_categorical_as_blob():
    blob = msgpack.packb([{"Category": "thdMin", "Value": "0.02", "ObsTime": "2026-03-10T20:00:00Z"}])
    assert decode.categorical([blob])[0][0].value == 0.02


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2026-03-10T20:01:07.943396+00:00", datetime(2026, 3, 10, 20, 1, 7, 943396, tzinfo=timezone.utc)),
        ("2026-03-10T20:01:07.9433960Z", datetime(2026, 3, 10, 20, 1, 7, 943396, tzinfo=timezone.utc)),  # .NET, 7 digits
        ("2026-03-10T20:01:07Z", datetime(2026, 3, 10, 20, 1, 7, tzinfo=timezone.utc)),
        ("2026-03-10T20:01:07", datetime(2026, 3, 10, 20, 1, 7, tzinfo=timezone.utc)),
    ],
)
def test_parse_time(text, expected):
    assert decode.parse_time(text) == expected


def test_msgpack_timestamp_and_ms_rounding():
    ts = msgpack.Timestamp.from_datetime(datetime(2026, 3, 10, 20, 1, 7, 943500, tzinfo=timezone.utc))
    assert decode.to_ms(decode.parse_time(ts)) == 1773172867944  # half a millisecond rounds up


def test_every_categorical_signal_has_a_hub_element():
    required, optional = signals.elements()
    assert required == "ComboBinaryData"
    assert optional == ["frequency", "thdAvg", "thdMin", "thdMax"]
