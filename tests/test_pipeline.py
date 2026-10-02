"""Timing policy, storage rules, rounding and dedup (table driven, no I/O)."""

import pytest

from ting_exporter import signals
from ting_exporter.pipeline import Pipeline, timing
from ting_exporter.pipeline.filters import Dedup, StorageFilter, round_value
from ting_exporter.pipeline.model import format_value

T0 = 1773172794656  # device ms


def combo(ms: int, volts: float = 121.5, **extra) -> list:
    from datetime import datetime, timezone

    payload = {"Voltage": volts, "DataTimeUtc": datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(), **extra}
    return [payload]


def thd(ms: int, avg: float, lo: float | None = None, hi: float | None = None) -> list:
    from datetime import datetime, timezone

    when = datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()
    records = [{"Category": "thdAvg", "Value": str(avg), "ObsTime": when}]
    if lo is not None:
        records.append({"Category": "thdMin", "Value": str(lo), "ObsTime": when})
    if hi is not None:
        records.append({"Category": "thdMax", "Value": str(hi), "ObsTime": when})
    return [records]


@pytest.mark.parametrize(
    ("delay", "expected_ts", "fell_back"),
    [
        (0.55, T0, False),  # live path
        (7.3, T0, False),  # buffered path
        (-1.9, T0, False),  # device slightly ahead of the Pi
        (299.0, T0, False),
        (-2.5, None, True),  # device ts in the future
        (3600.0, None, True),  # device ts an hour old
    ],
)
def test_stamp_guard(delay, expected_ts, fell_back):
    arrival = T0 / 1000 + delay
    ts, got_delay, fb = timing.stamp(T0, arrival)
    assert fb is fell_back and got_delay == pytest.approx(delay)
    assert ts == (expected_ts if expected_ts is not None else round((arrival - 0.55) * 1000))


def test_stamp_arrival_mode():
    ts, _, fb = timing.stamp(T0, T0 / 1000 + 7.0, "arrival")
    assert ts == T0 + 7000 and not fb


def test_late_and_gap_counters():
    st = timing.Timing()
    for ms in (T0, T0 + 250, T0 + 500):
        timing.track_primary(st, ms, ms / 1000 + 7, 7)
    timing.track_primary(st, T0 + 2500, 0, 7)  # 2 s jump: 7 slots missing
    assert st.gap_slots == 7 and st.late == 0
    timing.track_primary(st, T0 + 750, 0, 7)  # interleaved catch-up fills part of the gap
    assert st.late == 1 and st.max_device_ms == T0 + 2500
    assert st.delay.count == 5


def test_pipeline_stamps_counts_and_labels():
    p = Pipeline({"TNG000002": "b"})
    samples, primary = p.process("TNG000002", "updateComboBinaryData", combo(T0, 121.46402695310726, AveragePeaksMax=42.0), T0 / 1000 + 4.0)
    assert primary
    lines = sorted(s.line() for s in samples)
    assert lines == [
        f'ting_hifi{{serial="TNG000002",site="b"}} 42 {T0}\n',
        f'ting_voltage_volts{{serial="TNG000002",site="b"}} 121.464 {T0}\n',
    ]
    st = p.sensors["TNG000002"]
    assert st.timing.last_offset == pytest.approx(4.0) and st.timing.fallbacks == 0


def test_pipeline_fallback_and_unknown_site():
    p = Pipeline({})
    arrival = T0 / 1000 + 3600
    samples, _ = p.process("NEW", "updateComboBinaryData", combo(T0), arrival)
    assert samples[0].ts_ms == round((arrival - 0.55) * 1000)
    assert p.sensors["NEW"].timing.fallbacks == 1 and 'site="unknown"' in samples[0].labels


def test_graph_multi_is_counted_not_discarded():
    p = Pipeline()
    samples, primary = p.process("S", "updateGraphMulti", [["2026-03-10T19:59:54.6563000Z|60.02"]], T0 / 1000)
    assert samples == [] and not primary
    st = p.sensors["S"]
    assert st.hub_messages["updateGraphMulti"] == 1 and not st.discarded


def test_discards_are_counted():
    p = Pipeline()
    p.process("S", "updateComboBinaryData", [{"Voltage": 999}], T0 / 1000)
    p.process("S", "updateGraphMultiCategorical", [[{"Category": "nope", "Value": "1", "ObsTime": "2026-03-10T20:00:00Z"}]], T0 / 1000)
    assert dict(p.sensors["S"].discarded) == {"implausible_voltage": 1, "unknown_category": 1}


def test_thd_on_change_plus_heartbeat():
    p = Pipeline()
    arrival = T0 / 1000 + 1

    def push(ms, value):
        return [s.value for s in p.process("S", "updateGraphMultiCategorical", thd(ms, value), arrival + (ms - T0) / 1000)[0]]

    assert push(T0, 0.0717670) == [0.07177]  # first value
    assert push(T0 + 250, 0.0717670) == []  # the live path repeats it at 4 Hz
    assert push(T0 + 500, 0.0717699) == []  # same after rounding to 5 dp
    assert push(T0 + 30_000, 0.0720001) == [0.072]  # a real change
    assert push(T0 + 89_999, 0.0720001) == []
    assert push(T0 + 90_000, 0.0720001) == [0.072]  # 60 s heartbeat
    assert p.sensors["S"].received["ting_thd_ratio"] == 6 and p.sensors["S"].emitted["ting_thd_ratio"] == 3


def test_rolling_high_low_on_change():
    p = Pipeline()
    got = []
    for i, hi in enumerate([125.721, 125.721, 125.602, 125.602]):
        ms = T0 + i * 250
        samples, _ = p.process("S", "updateComboBinaryData", combo(ms, VoltageHi=hi, VoltageLo=115.672), ms / 1000 + 1)
        got += [(s.metric, s.value) for s in samples if "rolling" in s.metric]
    assert got == [
        ("ting_voltage_rolling_high_volts", 125.721),
        ("ting_voltage_rolling_low_volts", 115.672),
        ("ting_voltage_rolling_high_volts", 125.602),
    ]


def test_duplicate_samples_are_dropped():
    p = Pipeline()
    first, _ = p.process("S", "updateComboBinaryData", combo(T0), T0 / 1000 + 1)
    again, _ = p.process("S", "updateComboBinaryData", combo(T0), T0 / 1000 + 6)  # catch-up after reconnect
    assert len(first) == 1 and again == [] and p.sensors["S"].duplicates == 1


def test_dedup_horizon():
    d = Dedup(horizon_ms=1000)
    assert not d.seen("S", "m", 0)
    assert d.seen("S", "m", 0)
    assert not d.seen("S", "m", 5000)  # evicts ts 0
    assert not d.seen("S", "m", 0) and len(d) == 2


def test_a_late_sample_never_becomes_the_last_stored():
    """T4: an out-of-order THD sample is stored if it differs, but neither its time nor its value
    moves the state, so the heartbeat runs on from the newest stored sample."""
    f = StorageFilter()
    rule = signals.on_change(60)
    assert f.admit("S", "thd", rule, 100_000, 0.05)       # first
    assert not f.admit("S", "thd", rule, 120_000, 0.05)   # same value, 20 s later
    assert f.admit("S", "thd", rule, 90_000, 0.04)        # late, differs: stored
    assert not f.admit("S", "thd", rule, 95_000, 0.05)    # late, same as the newest: not stored
    assert not f.admit("S", "thd", rule, 130_000, 0.05)   # compared with 0.05 at 100 s, not with the late 0.04
    assert f.admit("S", "thd", rule, 160_000, 0.05)       # heartbeat: 60 s after 100 s, not after the late 90 s


@pytest.mark.parametrize(
    ("value", "decimals", "text"),
    [(124.99574456674243, 3, "124.996"), (60.02372702621271, 4, "60.0237"), (42.0, 0, "42"), (0.03836, 5, "0.03836"), (121.5, 3, "121.5"), (120.0, 3, "120")],
)
def test_rounding_and_format(value, decimals, text):
    assert format_value(round_value(value, decimals), decimals) == text


def test_arrival_stamps_never_merge_two_frames():
    """frames can arrive in the same millisecond. Stamped with their arrival time, Dedup took the second for a
    repeat and dropped it (17 of 2870 voltage samples on one sensor); they are distinct readings."""
    from tests import fakes

    pipeline = Pipeline({}, "arrival")
    for row in fakes.capture_rows():
        if row.get("kind") == "invocation":
            pipeline.process(row["serial"], row["target"], row["args"], row["t"])
    for serial, st in pipeline.sensors.items():
        assert st.received["ting_voltage_volts"] > 1000, serial
        assert st.emitted["ting_voltage_volts"] == st.received["ting_voltage_volts"] and st.duplicates == 0, serial


def test_fallback_stamps_are_kept_apart_too():
    pipeline = Pipeline({}, "device")
    args = lambda v: [{"Voltage": v, "DataTimeUtc": "2020-01-01T00:00:00Z"}]  # far off: the guard falls back to arrival
    first, _ = pipeline.process("S", "updateComboBinaryData", args(120.0), 1773172795.0001)
    second, _ = pipeline.process("S", "updateComboBinaryData", args(121.0), 1773172795.0003)  # the same millisecond
    assert len(first) == len(second) == 1 and second[0].ts_ms == first[0].ts_ms + 1


def test_a_silent_sensor_does_not_hold_up_another_sensors_dedup_eviction():
    """eviction was head-of-line: a sensor gone silent kept its keys at the head, so the other sensor's keys
    piled up toward the 200 k cap (~25 MiB during a 4 h site outage)."""
    d = Dedup()
    t0 = 1_790_000_000_000
    for i in range(2400):  # A streams for 10 minutes, then its site goes dark
        d.seen("A", "ting_voltage_volts", t0 + i * 250)
    for i in range(5 * 3600 * 4):  # B streams on for 5 hours
        d.seen("B", "ting_voltage_volts", t0 + 600_000 + i * 250)
    assert len(d) <= 2 * 2401  # A's last 10 minutes and B's, not B's 5 hours
