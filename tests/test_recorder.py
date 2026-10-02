"""Flight recorder / record files: format, rotation, atomic gzip, crash recovery, retention, errors."""

import asyncio
import gzip
import json
import os
from datetime import datetime, timezone

import msgpack

from ting_exporter import recorder as record


def hour_t(h, m=0, s=0, day=10):
    return datetime(2026, 3, day, h, m, s, tzinfo=timezone.utc).timestamp()


def lines(path):
    with gzip.open(path, "rt") as f:
        return [json.loads(line) for line in f]


def test_jsonable_decodes_blobs_and_keeps_the_rest():
    blob = msgpack.packb({"Voltage": 120.5, "DataTimeUtc": "2026-03-10T21:00:00Z"})
    when = datetime(2026, 3, 10, 21, 0, 0, 250000, tzinfo=timezone.utc)
    out = record.jsonable([blob, b"\xc1", when, float("nan"), {1: (2, 3)}])
    assert out == [
        {"msgpack": {"Voltage": 120.5, "DataTimeUtc": "2026-03-10T21:00:00Z"}},
        {"b64": "wQ=="},
        "2026-03-10T21:00:00.250000+00:00",
        "nan",
        {"1": [2, 3]},
    ]
    json.dumps(out)


async def test_hourly_writer_rotates_and_gzips(tmp_path):
    w = record.HourlyWriter(tmp_path)
    w.write({"t": hour_t(20, 59, 59), "n": 1})
    w.write({"t": hour_t(21, 0, 1), "n": 2})
    await w.close()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["ting-20260310T20.jsonl.gz", "ting-20260310T21.jsonl.gz"]
    assert [r["n"] for r in record.read_records(record.expand([tmp_path]))] == [1, 2]


async def test_restart_in_the_same_hour_appends_a_gzip_member(tmp_path):
    for n in (1, 2):
        w = record.HourlyWriter(tmp_path)
        w.write({"t": hour_t(21, n), "n": n})
        await w.close()
    [path] = tmp_path.iterdir()
    assert [r["n"] for r in lines(path)] == [1, 2]


def test_recover_compresses_leftovers_and_removes_temporaries(tmp_path):
    (tmp_path / "ting-20260310T20.jsonl").write_text('{"t": 1, "n": 1}\n')
    (tmp_path / "ting-20260310T20.jsonl.gz.tmp").write_bytes(b"half")
    record.recover(tmp_path)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["ting-20260310T20.jsonl.gz"]


def test_sweep_deletes_files_older_than_retention(tmp_path):
    for name in ("ting-20251201T00.jsonl.gz", "ting-20260310T20.jsonl.gz", "ting-20260310T21.jsonl", "other.txt"):
        (tmp_path / name).write_bytes(b"x")
    deleted, kept = record.sweep(tmp_path, 90, hour_t(22))
    assert deleted == 1 and kept == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == ["other.txt", "ting-20260310T20.jsonl.gz", "ting-20260310T21.jsonl"]


async def test_recorder_disk_error_pauses_and_never_raises(tmp_path, monkeypatch):
    now = [hour_t(21)]
    rec = record.Recorder(tmp_path / "raw", 90, clock=lambda: now[0])
    rec.start()
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 1}], now[0])

    def full(*_a, **_k):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(rec.writer, "write", full)
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 2}], now[0])  # must not raise
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 3}], now[0])  # paused: not even tried
    assert rec.errors == 1
    monkeypatch.undo()
    now[0] += record.ERROR_PAUSE + 1
    rec.event("S", "subscribed", {"elements": ["ComboBinaryData"]})
    await rec.close()
    got = list(record.read_records(record.expand([tmp_path / "raw"])))
    assert [r.get("args", [{}])[0].get("Voltage") for r in got if r["kind"] == "invocation"] == [1]
    assert got[-1]["event"] == "subscribed"


async def test_recorder_survives_a_failed_open_and_resumes(tmp_path, monkeypatch):
    now = [hour_t(21)]
    rec = record.Recorder(tmp_path / "raw", 90, clock=lambda: now[0])
    rec.start()
    real_open, failing = open, [True]

    def flaky_open(*a, **k):
        if failing[0]:
            raise PermissionError(13, "Permission denied")
        return real_open(*a, **k)

    monkeypatch.setattr(record, "open", flaky_open, raising=False)
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 1}], now[0])  # open fails: counted, paused
    now[0] += record.ERROR_PAUSE + 1
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 2}], now[0])  # same hour, still failing: tried again
    failing[0] = False
    now[0] += record.ERROR_PAUSE + 1
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 3}], now[0])  # same hour, the cause has cleared
    now[0] = hour_t(22, 0, 5)
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 4}], now[0])  # the next hour rotates as usual
    await rec.close()
    got = list(record.read_records(record.expand([tmp_path / "raw"])))
    assert [r["args"][0]["Voltage"] for r in got] == [3, 4] and rec.errors == 2


async def test_recorder_never_raises_whatever_fails(tmp_path, monkeypatch):
    now = [hour_t(21)]
    rec = record.Recorder(tmp_path / "raw", 90, clock=lambda: now[0])
    rec.start()

    def broken(*_a, **_k):
        raise TypeError("cannot convert")

    monkeypatch.setattr(record, "jsonable", broken)
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 1}], now[0])  # must not raise
    rec.event("S", "subscribed", {"elements": ["ComboBinaryData"]})  # paused
    rec.meta("end", {})
    assert rec.errors == 1
    monkeypatch.undo()
    now[0] += record.ERROR_PAUSE + 1
    rec.event("S", "subscribed", {"elements": ["ComboBinaryData"]})
    await rec.close()
    assert [r["event"] for r in record.read_records(record.expand([tmp_path / "raw"]))] == ["subscribed"]


async def test_a_failed_compression_is_logged_and_counted(tmp_path, monkeypatch, caplog):
    rec = record.Recorder(tmp_path / "raw", 90, clock=lambda: hour_t(22))
    rec.start()
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 1}], hour_t(21, 59))

    def full(_path):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(record, "gzip_atomic", full)
    rec.invocation("S", "updateComboBinaryData", [{"Voltage": 2}], hour_t(22))  # compresses hour 21 in the background
    await asyncio.gather(*rec.writer.pending, return_exceptions=True)
    await asyncio.sleep(0)
    assert rec.errors == 1 and "compressing ting-20260310T21.jsonl failed" in caplog.text
    monkeypatch.undo()
    await rec.close()


def test_read_records_survives_a_truncated_file_and_filters_by_time(tmp_path):
    path = tmp_path / "ting-20260310T21.jsonl.gz"
    body = gzip.compress(b"".join(json.dumps({"t": hour_t(21, i), "n": i}).encode() + b"\n" for i in range(50)))
    path.write_bytes(body[: len(body) - 20])  # a crash mid-write
    got = [r["n"] for r in record.read_records([path])]
    assert got and got == list(range(len(got)))
    window = (hour_t(21, 10), hour_t(21, 12))
    assert [r["n"] for r in record.read_records([path], window)] == [10, 11]


def test_parse_when():
    assert record.parse_when("2026-03-10T21:00") == hour_t(21)
    assert record.parse_when("2026-03-10T21:00:00Z") == hour_t(21)
    assert record.parse_when("1790550000") == 1790550000


def test_gzip_is_atomic(tmp_path, monkeypatch):
    src = tmp_path / "ting-20260310T20.jsonl"
    src.write_text('{"t": 1}\n')

    def boom(*_a):
        raise OSError("disk full")

    monkeypatch.setattr(os, "fsync", boom)
    try:
        record.gzip_atomic(src)
    except OSError:
        pass
    assert src.exists() and not (tmp_path / "ting-20260310T20.jsonl.gz").exists()


def test_an_hour_written_before_and_after_a_restart_is_read_in_order(tmp_path):
    """by name, the hour's .jsonl (written after the restart) came before its .jsonl.gz (written before it),
    so an import fed the newer half first and the on_change heartbeats of the older half were never admitted."""
    (tmp_path / "ting-20260310T21.jsonl.gz").write_bytes(gzip.compress(b'{"t": 1, "n": 1}\n'))
    (tmp_path / "ting-20260310T21.jsonl").write_text('{"t": 2, "n": 2}\n')
    (tmp_path / "ting-20260310T20.jsonl.gz").write_bytes(gzip.compress(b'{"t": 0, "n": 0}\n'))
    (tmp_path / "ting-20260310T22.jsonl").write_text('{"t": 3, "n": 3}\n')
    assert [p.name for p in record.expand([tmp_path])] == [
        "ting-20260310T20.jsonl.gz", "ting-20260310T21.jsonl.gz", "ting-20260310T21.jsonl", "ting-20260310T22.jsonl"]
    named = sorted(str(p) for p in tmp_path.iterdir())  # as a shell glob passes them: .jsonl before .jsonl.gz
    assert record.expand(named) == record.expand([tmp_path])
    other = [tmp_path / "b-capture.jsonl.gz", tmp_path / "a-capture.jsonl.gz"]
    assert record.expand(other) == other  # not hourly files: the order given


async def test_a_record_from_an_earlier_hour_goes_into_the_current_file(tmp_path):
    """The clock stepped back: 20:00's file is being compressed, so the line goes into 21:00's, not lost."""
    w = record.HourlyWriter(tmp_path)
    w.write({"t": hour_t(20, 59, 59), "n": 1})
    w.write({"t": hour_t(21, 0, 1), "n": 2})
    w.write({"t": hour_t(20, 59, 58), "n": 3})
    await w.close()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["ting-20260310T20.jsonl.gz", "ting-20260310T21.jsonl.gz"]
    assert [r["n"] for r in lines(tmp_path / "ting-20260310T21.jsonl.gz")] == [2, 3]


async def test_update_graph_multi_is_an_hourly_count_not_a_line_per_message(tmp_path):
    """Design 7.3: the duplicate of the categorical data was 37 % of the recorder's volume."""
    now = [hour_t(21) + 10]
    rec = record.Recorder(tmp_path / "raw", 90, clock=lambda: now[0])
    rec.start()
    for i in range(3):
        rec.invocation("TNG000001", "updateGraphMulti", [[{"Category": "frequency", "Value": "60.0"}]], hour_t(21) + i)
    rec.invocation("TNG000002", "updateGraphMulti", [[]], hour_t(21) + 5)
    rec.invocation("TNG000001", "updateComboBinaryData", [{"Voltage": 120.0}], hour_t(21) + 6)
    rec.invocation("TNG000001", "updateGraphMulti", [[]], hour_t(22) + 1)  # the next hour: the 21:00 counts go out first
    now[0] = hour_t(22) + 2
    rec.meta("end", {})
    await rec.close()
    rows = list(record.read_records(record.expand([tmp_path / "raw"])))
    counts = [(r["serial"], r["count"], r["t"] < hour_t(22)) for r in rows if r.get("event") == "graph_multi_count"]
    assert counts == [("TNG000001", 3, True), ("TNG000002", 1, True), ("TNG000001", 1, False)]
    assert rows[-1]["kind"] == "end" and not [r for r in rows if r.get("target") == "updateGraphMulti" and r["kind"] == "invocation"]


async def test_a_lone_surrogate_does_not_fail_a_write(tmp_path):
    rec = record.Recorder(tmp_path / "raw", 90, clock=lambda: hour_t(21))
    rec.start()
    rec.event("TNG000001", "notification", {"record": {"title": "bad \udcff text"}})
    await rec.close()
    assert rec.errors == 0
    [row] = list(record.read_records(record.expand([tmp_path / "raw"])))
    assert row["record"]["title"] == "bad \udcff text"  # written as the JSON escape, read back as the same text
