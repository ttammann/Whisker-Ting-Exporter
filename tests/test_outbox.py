"""The outbox (design 7.2, T12): one durable log, a cursor per target, garbage collection, the cap, the disk failing."""

import gzip
import os

import pytest

from ting_exporter import outbox as outbox_mod
from ting_exporter.outbox import Outbox, orphaned_cursors, valid_line


def line(i: int, serial: str = "TNG000001") -> str:
    return f'ting_voltage_volts{{serial="{serial}",site="a"}} {120 + i % 7} {1790550000000 + i * 250}\n'


class Wall:
    def __init__(self, t: float = 1790550000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t


def make(tmp_path, targets=("local", "peer"), **kw) -> Outbox:
    wall = kw.pop("wall", Wall())
    box = Outbox(tmp_path / "outbox", list(targets), wall=wall, **kw)
    box.open()
    return box


def drain(box: Outbox, target: str, n: int = 10_000) -> list[str]:
    out = []
    while True:
        lines, pos = box.read(target, n)
        if not lines:
            return out
        out += lines
        box.commit(target, pos)


async def test_append_flush_read_commit(tmp_path):
    box = make(tmp_path)
    box.append([line(i) for i in range(10)])
    assert box.read("local", 100)[0] == []  # nothing before the flush
    await box.flush()
    lines, pos = box.read("local", 4)
    assert lines == [line(i) for i in range(4)] and pos[1] == 4
    box.commit("local", pos)
    assert box.lag("local")[0] == 6 and box.lag("peer")[0] == 10
    assert drain(box, "local") == [line(i) for i in range(4, 10)]
    assert (tmp_path / "outbox" / "cursor-local").read_text().split()[1] == "10"
    segs = list((tmp_path / "outbox").glob("seg-*.prom"))
    assert len(segs) == 1 and segs[0].read_text() == "".join(line(i) for i in range(10))  # fsynced on disk


async def test_rotation_compression_and_garbage_collection(tmp_path):
    wall = Wall()
    box = make(tmp_path, wall=wall, rotate_seconds=600)
    for i in range(3):
        box.append([line(10 * i + j) for j in range(10)])
        await box.flush()
        wall.t += 601  # every flush starts a new segment after rotation
    await box.flush()
    sealed = sorted((tmp_path / "outbox").glob("seg-*.prom.gz"))
    assert len(sealed) == 3 and sealed[0].name.endswith("-10.prom.gz")  # name carries the line count
    assert gzip.decompress(sealed[0].read_bytes()).decode() == "".join(line(j) for j in range(10))
    assert drain(box, "local") == [line(i) for i in range(30)]
    await box.flush()
    assert len(list((tmp_path / "outbox").glob("seg-*"))) == 3  # peer still needs them
    drain(box, "peer")
    await box.flush()
    assert not list((tmp_path / "outbox").glob("seg-*"))  # everyone is past: deleted


async def test_a_restart_resumes_every_target_from_its_cursor(tmp_path):
    box = make(tmp_path)
    box.append([line(i) for i in range(20)])
    await box.flush()
    lines, pos = box.read("local", 12)
    box.commit("local", pos)
    await box.close()
    again = make(tmp_path)  # the plain active segment is sealed at the start
    assert list((tmp_path / "outbox").glob("seg-*.prom.gz"))
    assert drain(again, "local") == [line(i) for i in range(12, 20)]
    assert drain(again, "peer") == [line(i) for i in range(20)]
    again.append([line(99)])
    await again.flush()
    assert drain(again, "local") == [line(99)]  # a new segment with a higher seq


async def test_a_crash_between_append_and_cursor_write_resends(tmp_path):
    """The cursor is written after the store took the batch; lose it and the batch goes again (the store dedups)."""
    box = make(tmp_path, targets=("local",))
    box.append([line(i) for i in range(5)])
    await box.flush()
    lines, _pos = box.read("local", 100)  # pushed, but the process dies before commit()
    again = make(tmp_path, targets=("local",))
    assert drain(again, "local") == lines


async def test_a_truncated_last_line_from_a_crash_is_dropped(tmp_path):
    box = make(tmp_path, targets=("local",))
    box.append([line(i) for i in range(3)])
    await box.flush()
    active = next((tmp_path / "outbox").glob("seg-*.prom"))
    with open(active, "a") as f:
        f.write('ting_voltage_volts{serial="TNG000001",site="a"} 12')  # cut short
    again = make(tmp_path, targets=("local",))
    assert drain(again, "local") == [line(i) for i in range(3)]


async def test_the_cap_drops_the_oldest_for_the_lagging_target_only(tmp_path):
    wall = Wall()
    box = make(tmp_path, wall=wall, max_bytes=1, rotate_seconds=600)  # every sealed segment is over the cap
    box.append([line(i) for i in range(10)])
    await box.flush()
    drain(box, "local")  # local keeps up, peer is down
    wall.t += 601
    box.append([line(i) for i in range(10, 20)])
    await box.flush()
    assert box.dropped == {"peer": 10}  # the first segment went; local had sent it already
    assert drain(box, "peer") == [line(i) for i in range(10, 20)]


async def test_targets_come_and_go(tmp_path):
    box = make(tmp_path, targets=("local",))
    box.append([line(i) for i in range(5)])
    await box.flush()
    await box.close()
    new = make(tmp_path, targets=("local", "peer"))  # a new target starts at the end of the log
    assert drain(new, "peer") == [] and new.lag("local")[0] == 5
    await new.close()
    replaying = Outbox(tmp_path / "outbox", ["local", "other"], replay_new=True)
    replaying.open()
    assert drain(replaying, "other") == [line(i) for i in range(5)]  # or at its start
    assert not (tmp_path / "outbox" / "cursor-peer").exists()  # a removed target's cursor is deleted
    assert orphaned_cursors(tmp_path / "outbox", ["local"]) == ["other"]


async def test_an_unreadable_segment_is_skipped_and_counted(tmp_path):
    wall = Wall()
    box = make(tmp_path, wall=wall, rotate_seconds=600)
    box.append([line(i) for i in range(10)])
    await box.flush()
    wall.t += 601
    box.append([line(i) for i in range(10, 15)])
    await box.flush()
    await box.close()
    first = sorted((tmp_path / "outbox").glob("seg-*.prom.gz"))[0]
    first.write_bytes(b"\x1f\x8b not gzip at all")
    again = make(tmp_path)
    assert drain(again, "local") == [line(i) for i in range(10, 15)]
    assert again.dropped["local"] == 10 and again.dropped["peer"] == 10  # peer's cursor was inside it too
    assert (tmp_path / "outbox" / "rejected" / first.name).exists()


async def segments_of_ten(box: Outbox, wall: Wall, n: int) -> None:
    for k in range(n):
        box.append([line(10 * k + i) for i in range(10)])
        await box.flush()
        wall.t += 601
    await box.flush()


async def test_a_later_unreadable_segment_leaves_a_lagging_target_where_it_is(tmp_path):
    wall = Wall()
    box = make(tmp_path, wall=wall, rotate_seconds=600)
    await segments_of_ten(box, wall, 3)
    first, second, _third = box.segments
    box.commit("local", (second.seq, 0))
    box.commit("peer", (first.seq, 5))  # behind, half way through the first segment
    box._cache.clear()
    second.path.write_bytes(b"\x1f\x8b not gzip at all")
    assert drain(box, "local") == [line(i) for i in range(20, 30)]
    assert drain(box, "peer") == [line(i) for i in range(5, 10)] + [line(i) for i in range(20, 30)]
    assert box.dropped == {"local": 10, "peer": 10}  # exactly the unreadable segment, for each


async def test_the_memory_cap_leaves_a_lagging_target_on_its_disk_segment(tmp_path, monkeypatch):
    monkeypatch.setattr(outbox_mod, "MEMORY_SEGMENT_LINES", 10)
    wall = Wall()
    box = make(tmp_path, wall=wall, rotate_seconds=600, memory_max_lines=15)
    await segments_of_ten(box, wall, 1)  # on disk; peer is down and has read none of it
    drain(box, "local")
    box.disk_ok = False
    box._retry_at = 1e18
    await segments_of_ten(box, wall, 3)  # in memory, over the cap: the two oldest memory segments go
    assert box.dropped == {"local": 20, "peer": 20}
    assert drain(box, "peer")[:10] == [line(i) for i in range(10)]  # its disk segment is still read


async def test_a_batch_in_flight_when_the_cap_removes_its_segment(tmp_path):
    """Counted as dropped only if the push then fails (read again without a commit)."""
    wall = Wall()
    box = make(tmp_path, wall=wall, max_bytes=10**9, rotate_seconds=600)
    await segments_of_ten(box, wall, 2)
    drain(box, "local")
    lines, pos = box.read("peer", 4)  # being pushed
    box.max_bytes = 1
    await box.flush()  # the cap removes both sealed segments
    assert box.dropped["peer"] == 6 + 10  # the rest of the first segment and the second, not the 4 in flight
    box.commit("peer", pos)  # the push went through
    assert box.dropped["peer"] == 16 and box.read("peer", 4)[0] == []
    box.max_bytes = 10**9
    box.append([line(i) for i in range(30, 40)])
    await box.flush()
    wall.t += 601
    await box.flush()
    lines, pos = box.read("peer", 4)
    box.max_bytes = 1
    await box.flush()
    assert box.dropped["peer"] == 16 + 6
    box.read("peer", 4)  # the push failed: those 4 are lost too
    assert box.dropped["peer"] == 16 + 10


async def test_an_unusable_outbox_keeps_the_directory_for_the_next_start(tmp_path):
    """open() failing: memory segments get numbers no file in the directory has, the cursor files stay, and the
    next start sends the old backlog and the new samples."""
    box = make(tmp_path, targets=("local",))
    box.append([line(i) for i in range(5)])
    await box.flush()
    await box.close()  # five samples nobody sent
    wall = Wall(1790560000.0)
    unusable = Outbox(tmp_path / "outbox", ["local"], wall=wall)
    unusable.open_in_memory()
    unusable.append([line(i) for i in range(5, 8)])
    await unusable.flush()
    assert drain(unusable, "local") == [line(i) for i in range(5, 8)]  # pushed from memory
    unusable._retry_at = 0.0
    unusable.append([line(8)])
    await unusable.flush()
    await unusable.close()
    assert unusable.disk_ok and (tmp_path / "outbox" / "cursor-local").read_text() == "1 0\n"
    seqs = [int(p.name.split("-")[1]) for p in (tmp_path / "outbox").glob("seg-*")]
    assert len(seqs) == len(set(seqs)) == 2 and max(seqs) >= 1790560000
    again = make(tmp_path, targets=("local",))
    assert drain(again, "local") == [line(i) for i in range(9)]  # the backlog, then everything again (dedup)


async def test_the_disk_failing_keeps_samples_in_memory_and_writes_them_out_later(tmp_path, monkeypatch):
    box = make(tmp_path, targets=("local",))
    box.append([line(0)])
    await box.flush()
    real_fsync = os.fsync
    failing = [True]

    def fsync(fd):
        if failing[0]:
            raise OSError(28, "No space left on device")
        real_fsync(fd)

    monkeypatch.setattr(outbox_mod.os, "fsync", fsync)
    box.append([line(i) for i in range(1, 6)])
    await box.flush()
    assert not box.disk_ok and box.errors == 1
    assert drain(box, "local") == [line(i) for i in range(6)]  # pushed from memory meanwhile
    box.append([line(6)])
    await box.flush()
    failing[0] = False
    box._retry_at = 0.0
    box.append([line(7)])
    await box.flush()
    assert box.disk_ok
    assert drain(box, "local") == [line(6), line(7)]
    await box.close()
    again = make(tmp_path, targets=("local",))
    assert drain(again, "local") == []  # nothing to re-send, nothing lost


async def test_the_memory_cap_while_the_disk_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(outbox_mod, "MEMORY_SEGMENT_LINES", 10)
    box = make(tmp_path, targets=("local",), memory_max_lines=25)
    box.disk_ok = False
    box._retry_at = 1e18
    for k in range(5):
        box.append([line(10 * k + i) for i in range(10)])
        await box.flush()
    assert box.dropped["local"] == 30 and box.lag("local")[0] == 20  # whole memory segments go, oldest first
    assert drain(box, "local")[0] == line(30)


async def test_the_inbox_takes_valid_files_and_rejects_the_rest(tmp_path):
    box = make(tmp_path, targets=("local",))
    inbox = tmp_path / "outbox" / "inbox"
    (inbox / "mark-1.prom").write_text('ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 1 1790550500000\n')
    (inbox / "mark-2.prom").write_text("rm -rf /\n")
    (inbox / "mark-3.tmp").write_text(line(1))  # still being written: left alone
    await box.flush()
    assert drain(box, "local") == ['ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 1 1790550500000\n']
    assert (tmp_path / "outbox" / "rejected" / "mark-2.prom").exists() and (inbox / "mark-3.tmp").exists()
    assert box.inbox_lines == 1 and box.rejected_files == 1


def test_valid_line():
    assert valid_line(line(1).strip())
    assert valid_line('ting_x{a="b\\"c"} -1.5e3 -5')
    for bad in ("ting_x 1 2", 'ting_x{a="b"} 1', 'ting x{a="b"} 1 2', "", '{a="b"} 1 2'):
        assert not valid_line(bad), bad


async def test_lag_seconds_is_the_age_of_the_oldest_unsent_sample(tmp_path):
    wall = Wall()
    box = make(tmp_path, targets=("local",), wall=wall)
    box.append([line(0)])
    await box.flush()
    wall.t += 30
    box.append([line(1)])
    await box.flush()
    wall.t += 10
    assert box.lag("local") == (2, pytest.approx(40.0, abs=0.01))
    lines, pos = box.read("local", 1)
    box.commit("local", pos)
    assert box.lag("local") == (1, pytest.approx(10.0, abs=0.01))
    drain(box, "local")
    assert box.lag("local") == (0, 0.0)
