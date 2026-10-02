"""The outbox: one durable log of formatted samples, and a cursor per push target (design 7.2).

Every sample leaves the exporter through here: the stream, the notification
history, the REST values, inferred cuts, context marks. The writer appends the
lines that came in since the last flush (every push interval) to the active
segment and fsyncs it; each push target's pusher reads from its own cursor and
advances it after VictoriaMetrics took the batch. Re-sending is harmless
(device timestamps, VictoriaMetrics dedup), so a cursor is written without
fsync: a lost one only re-sends.

    /data/outbox/seg-0000000042-1790603992097.prom                 the active segment (appended)
    /data/outbox/seg-0000000041-1790603392097-1790603987001-14388.prom.gz   sealed: seq, first and last
                                                                    append time (ms), line count
    /data/outbox/cursor-<target>          "<seq> <line>": the next line this target needs
    /data/outbox/rejected/                batches a store refused (4xx), unreadable segments
    /data/outbox/inbox/*.prom             lines other processes hand in (`ting-exporter mark`),
                                          appended at the next flush, then deleted

Segments rotate at 8 MiB or 10 minutes and are then gzip-compressed (written
to .tmp, fsynced, renamed). A sealed segment is deleted once every target's
cursor is past it. Above the cap the oldest segment is deleted anyway, and
every cursor still inside it jumps forward: those samples are counted as
dropped for that target. A target without a cursor starts at the end of the
log (or at its start with replay_new); the cursor of a target that is no
longer configured is deleted, so no data is ever stranded for it.

If the disk cannot be written, segments are kept in memory (at most
MEMORY_MAX_LINES lines, the oldest dropped and counted) and pushed from
there; the disk is tried again every DISK_RETRY seconds, and the memory
segments are written out once it works. If the directory cannot even be
opened at the start, what it holds is left for the next start (open_in_memory).
Nothing here ever raises into the code that appends.
"""

from __future__ import annotations

import asyncio
import bisect
import gzip
import logging
import os
import re
import time
import zlib
from collections import Counter, OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar

log = logging.getLogger(__name__)
T = TypeVar("T")

ROTATE_BYTES = 8 * 1_048_576
ROTATE_SECONDS = 600.0
MEMORY_SEGMENT_LINES = 5_000
MEMORY_MAX_LINES = 50_000
DISK_RETRY = 60.0
CACHE_SEGMENTS = 3  # decompressed sealed segments kept for pushers that catch up
_ACTIVE = re.compile(r"^seg-(\d{10})-(\d+)\.prom$")
_SEALED = re.compile(r"^seg-(\d{10})-(\d+)-(\d+)-(\d+)\.prom\.gz$")
_LINE = re.compile(r'^[a-zA-Z_:][a-zA-Z0-9_:]*\{(?:[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\\n]|\\.)*",?)*\} \S+ -?\d+$')


def valid_line(line: str) -> bool:
    """One sample in the Prometheus text format with labels and a timestamp (what the inbox accepts)."""
    return bool(_LINE.match(line))


@dataclass
class Segment:
    seq: int
    first_ms: int
    last_ms: int
    count: int = 0
    path: Path | None = None  # None: in memory only (the disk failed)
    sealed: bool = False
    lines: list[str] | None = None  # in memory for the active and memory-only segments
    bytes: int = 0  # on disk
    marks: list[tuple[int, int]] = field(default_factory=list)  # (line index, append ms) per flush, this run
    stale_file: Path | None = None  # a partly written file of this segment, deleted once it is written out

    def name(self) -> str:
        if self.sealed:
            return f"seg-{self.seq:010d}-{self.first_ms}-{self.last_ms}-{self.count}.prom.gz"
        return f"seg-{self.seq:010d}-{self.first_ms}.prom"

    def appended_ms(self, line: int) -> int:
        """When line `line` was appended (exact for this run's flushes, interpolated for older segments)."""
        if self.marks:
            i = bisect.bisect_right([m[0] for m in self.marks], line) - 1
            return self.marks[max(i, 0)][1]
        if self.count <= 1:
            return self.first_ms
        return self.first_ms + (self.last_ms - self.first_ms) * line // (self.count - 1)


class Outbox:
    def __init__(
        self,
        directory: Path,
        targets: list[str],
        max_bytes: int = 268_435_456,
        *,
        replay_new: bool = False,
        rotate_bytes: int = ROTATE_BYTES,
        rotate_seconds: float = ROTATE_SECONDS,
        memory_max_lines: int = MEMORY_MAX_LINES,
        wall: Callable[[], float] = time.time,
    ) -> None:
        self.dir = directory
        self.targets = list(targets)
        self.max_bytes = max_bytes
        self.replay_new = replay_new
        self.rotate_bytes = rotate_bytes
        self.rotate_seconds = rotate_seconds
        self.memory_max_lines = memory_max_lines
        self.wall = wall
        self.segments: list[Segment] = []  # in seq order; the last may be the active one
        self.cursors: dict[str, tuple[int, int]] = {}
        self.pending: list[str] = []
        self.dropped: Counter[str] = Counter()  # samples by target: cap, unreadable segment, memory cap
        self.errors = 0  # disk write errors
        self.rejected_files = 0
        self.inbox_lines = 0
        self.disk_ok = True
        self.write_cursors = True  # False after open() failed: the cursor files on disk stay as the last run left them
        self.last_flush = time.monotonic()
        self.wake = asyncio.Event()  # set after every flush that added lines
        self._file = None
        self._next_seq = 1
        self._retry_at = 0.0
        self._cache: OrderedDict[int, list[str]] = OrderedDict()
        self._inflight: dict[str, tuple[int, int, int]] = {}  # target -> (seq, first, end line) read, not committed yet
        self._unconfirmed: Counter[str] = Counter()  # in-flight lines whose segment went: lost unless committed

    # ---- start ---------------------------------------------------------------

    def open(self) -> None:
        """Scan the directory: recover a previous run's segments and cursors. Raises OSError if unusable."""
        self.dir.mkdir(parents=True, exist_ok=True)
        (self.dir / "rejected").mkdir(exist_ok=True)
        (self.dir / "inbox").mkdir(exist_ok=True)
        for tmp in self.dir.glob("*.tmp"):
            tmp.unlink(missing_ok=True)
        sealed: dict[int, Segment] = {}
        for path in self.dir.iterdir():
            if m := _SEALED.match(path.name):
                seq = int(m[1])
                sealed[seq] = Segment(seq, int(m[2]), int(m[3]), int(m[4]), path, True, bytes=path.stat().st_size)
        for path in sorted(self.dir.iterdir()):
            if not (m := _ACTIVE.match(path.name)):
                continue
            seq = int(m[1])
            if seq in sealed:  # written out already; this is the partial copy from before
                path.unlink(missing_ok=True)
                continue
            sealed[seq] = self._recover(path, seq, int(m[2]))
        self.segments = [sealed[s] for s in sorted(sealed) if sealed[s] is not None]
        seqs = [s.seq for s in self.segments]
        for path in self.dir.glob("cursor-*"):
            target = path.name.removeprefix("cursor-")
            pos = _read_cursor(path)
            if pos is not None:
                seqs.append(pos[0])
            if target not in self.targets:
                log.warning("outbox: %s is no longer a push target; deleting its cursor (its backlog is not kept for it)",
                            target)
                path.unlink(missing_ok=True)
            elif pos is not None:
                self.cursors[target] = pos
        self._next_seq = max(seqs, default=0) + 1
        for target in self.targets:
            if target not in self.cursors:
                start = (self.segments[0].seq, 0) if self.replay_new and self.segments else self.end()
                self.cursors[target] = start
                self._write_cursor(target)
                log.info("outbox: new push target %s starts at %s", target,
                         "the start of the log" if self.replay_new and self.segments else "the end of the log")
        for target in self.targets:
            self._normalize(target)
        if self.segments:
            log.info("outbox: %d segments, %d bytes; behind: %s", len(self.segments), self.disk_bytes(),
                     ", ".join(f"{t} {self.lag(t)[0]}" for t in self.targets))

    def open_in_memory(self) -> None:
        """open() failed: keep the samples in memory and try the disk every DISK_RETRY. What the directory holds is
        left alone: segment numbers start at the wall clock's seconds, above any it can hold, and the cursor files
        are not written, so the next start resumes every target where the last run left it (re-sending is
        harmless) and no segment number is used twice."""
        self.errors += 1
        self.disk_ok = False
        self.write_cursors = False
        self._retry_at = time.monotonic() + DISK_RETRY
        self._next_seq = int(self.wall())
        self.cursors = {target: (self._next_seq, 0) for target in self.targets}

    def _recover(self, path: Path, seq: int, first_ms: int) -> Segment | None:
        """A previous run's active segment: keep its complete lines and seal it."""
        try:
            data = path.read_bytes()
        except OSError as err:
            log.error("outbox: cannot read %s (%s); moving it to rejected/", path.name, err)
            self._reject_file(path)
            return None
        if not data:
            path.unlink(missing_ok=True)
            return None
        if not data.endswith(b"\n"):  # a write cut short by a crash
            data = data[: data.rfind(b"\n") + 1]
        try:
            lines = data.decode("utf-8").splitlines(keepends=True)
        except UnicodeDecodeError:
            log.error("outbox: %s is not UTF-8; moving it to rejected/", path.name)
            self._reject_file(path)
            return None
        seg = Segment(seq, first_ms, int(path.stat().st_mtime * 1000), len(lines), path, False, lines)
        self._seal_now(seg)
        return seg

    # ---- appending (never raises) ------------------------------------------------

    def append(self, lines: list[str]) -> None:
        self.pending.extend(lines)

    async def flush(self) -> None:
        """Write what came in since the last flush, rotate, retry the disk, collect garbage. Never raises."""
        self.last_flush = time.monotonic()
        try:
            self._ingest_inbox()
            added = bool(self.pending)
            self._rotate_if_due()  # an old active segment is sealed before new lines go in
            if self.pending:
                await self._write_pending()
            self._rotate_if_due()
            if not self.disk_ok and time.monotonic() >= self._retry_at:
                await self._retry_disk()
            for seg in [s for s in self.segments if s.sealed and s.path is not None and not s.path.name.endswith(".gz")]:
                await self._seal(seg)
            self._enforce_memory_cap()
            self._collect()
            if added:
                self.wake.set()
                self.wake = asyncio.Event()
        except Exception:  # a bug: say so, keep the lines, never stop the writer
            self.errors += 1
            log.exception("outbox: flush failed")

    def _rotate_if_due(self) -> None:
        active = self._active()
        if active is not None and self._due(active):
            self._finish_active()

    def _active(self) -> Segment | None:
        return self.segments[-1] if self.segments and not self.segments[-1].sealed else None

    def _due(self, seg: Segment) -> bool:
        if seg.path is None:
            return seg.count >= MEMORY_SEGMENT_LINES
        return seg.bytes >= self.rotate_bytes or self.wall() * 1000 - seg.first_ms >= self.rotate_seconds * 1000

    async def _write_pending(self) -> None:
        now_ms = int(self.wall() * 1000)
        lines, self.pending = self.pending, []
        while lines:
            seg = self._active()
            if seg is None:
                seg = self._new_segment(now_ms)
            room = MEMORY_SEGMENT_LINES - seg.count if seg.path is None else len(lines)
            chunk, lines = lines[:room], lines[room:]
            if seg.path is not None:
                data = "".join(chunk).encode()
                try:
                    await _finish_in_thread(self._append, seg.path, data)  # an fsync can take long: not on the loop
                    seg.bytes += len(data)
                except OSError as err:
                    self._disk_failed(seg, err)
            seg.marks.append((seg.count, now_ms))
            seg.lines.extend(chunk)  # type: ignore[union-attr]
            seg.count += len(chunk)
            seg.last_ms = now_ms
            if lines:  # a memory segment is full
                self._finish_active()

    def _append(self, path: Path, data: bytes) -> None:
        """Append to the active segment's file and fsync it (in a thread; only the writer calls it)."""
        if self._file is None:
            self._file = open(path, "ab")
        self._file.write(data)
        self._file.flush()
        os.fsync(self._file.fileno())

    def _new_segment(self, now_ms: int) -> Segment:
        seg = Segment(self._next_seq, now_ms, now_ms, lines=[])
        self._next_seq += 1
        if self.disk_ok:
            seg.path = self.dir / seg.name()
        self.segments.append(seg)
        return seg

    def _disk_failed(self, seg: Segment, err: OSError) -> None:
        self.errors += 1
        self.disk_ok = False
        self._retry_at = time.monotonic() + DISK_RETRY
        log.error("outbox: cannot write %s (%s); keeping up to %d samples in memory, retrying the disk every %.0f s",
                  seg.path.name if seg.path else "?", err, self.memory_max_lines, DISK_RETRY)
        self._close_file()
        seg.stale_file, seg.path, seg.bytes = seg.path, None, 0

    def _close_file(self) -> None:
        if self._file is not None:
            try:
                self._file.close()
            except OSError:
                pass
            self._file = None

    def _finish_active(self) -> None:
        """Seal the active segment; a disk segment is compressed later in the same flush."""
        seg = self._active()
        if seg is None:
            return
        self._close_file()
        seg.sealed = True

    async def _seal(self, seg: Segment) -> bool:
        """Write a sealed segment as .prom.gz (the file work in a thread, the bookkeeping here)."""
        lines = seg.lines if seg.lines is not None else self._lines(seg)
        if lines is None:
            return False
        try:
            size = await _finish_in_thread(_write_gz, self.dir, self._sealed_name(seg), lines)
        except OSError as err:
            self.errors += 1
            log.error("outbox: cannot compress segment %d (%s); it is tried again at the next flush", seg.seq, err)
            return False
        self._sealed(seg, size, lines)
        return True

    def _seal_now(self, seg: Segment) -> None:
        """The same, synchronously (open(), before anything else runs)."""
        lines = seg.lines or []
        try:
            size = _write_gz(self.dir, self._sealed_name(seg), lines)
        except OSError as err:
            self.errors += 1
            log.error("outbox: cannot compress segment %d (%s); it is tried again at the next flush", seg.seq, err)
            return
        self._sealed(seg, size, lines)

    def _sealed_name(self, seg: Segment) -> str:
        seg.sealed = True
        return seg.name()

    def _sealed(self, seg: Segment, size: int, lines: list[str]) -> None:
        old, stale = seg.path, seg.stale_file
        seg.path, seg.bytes, seg.stale_file = self.dir / seg.name(), size, None
        for path in (old, stale):
            if path is not None and path != seg.path:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
        self._cache[seg.seq] = lines
        self._trim_cache()
        seg.lines = None

    async def _retry_disk(self) -> None:
        """Write the memory segments out; back to disk mode once they all are."""
        try:
            await _finish_in_thread(_probe, self.dir)
        except OSError:
            self._retry_at = time.monotonic() + DISK_RETRY
            return
        active = self._active()
        if active is not None:
            active.sealed = True  # memory segments are written out sealed; new lines start a disk segment
        for seg in [s for s in self.segments if s.path is None]:
            if not await self._seal(seg):
                self._retry_at = time.monotonic() + DISK_RETRY
                return
        self.disk_ok = True
        log.info("outbox: the disk works again; the memory segments are written out")

    def _enforce_memory_cap(self) -> None:
        memory = [s for s in self.segments if s.path is None]
        total = sum(s.count for s in memory)
        for seg in memory:
            if total <= self.memory_max_lines or not seg.sealed:
                break
            log.error("outbox: memory cap reached while the disk fails; dropping %d samples (segment %d)", seg.count, seg.seq)
            total -= seg.count
            self._remove(seg, "memory cap")

    def _ingest_inbox(self) -> None:
        inbox = self.dir / "inbox"
        try:
            files = sorted(p for p in inbox.iterdir() if p.suffix == ".prom")
        except OSError:
            return
        for path in files:
            try:
                text = path.read_text(encoding="utf-8")
                lines = [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.startswith("#")]
                if not all(valid_line(ln) for ln in lines):
                    raise ValueError("not one sample with labels and a timestamp per line")
            except (OSError, ValueError) as err:
                log.error("outbox: inbox file %s refused (%s); moved to rejected/", path.name, err)
                self._reject_file(path)
                continue
            self.pending.extend(ln + "\n" for ln in lines)
            self.inbox_lines += len(lines)
            log.info("outbox: took %d samples from inbox/%s", len(lines), path.name)
            path.unlink(missing_ok=True)

    # ---- reading (the pushers) -----------------------------------------------

    def end(self) -> tuple[int, int]:
        active = self._active()
        return (active.seq, active.count) if active is not None else (self._next_seq, 0)

    def _segment(self, seq: int) -> Segment | None:
        i = bisect.bisect_left([s.seq for s in self.segments], seq)
        return self.segments[i] if i < len(self.segments) and self.segments[i].seq == seq else None

    def _normalize(self, target: str) -> tuple[int, int]:
        """Move a cursor past finished or missing segments, onto the segment it needs next."""
        seq, line = self.cursors[target]
        pos = (self._next_seq, 0)  # past everything: the next segment to be created
        for seg in self.segments:
            if seg.seq < seq or (seg.seq == seq and seg.sealed and line >= seg.count):
                continue
            pos = (seq, line) if seg.seq == seq else (seg.seq, 0)
            break
        self.cursors[target] = pos
        return pos

    async def prefetch(self, target: str) -> None:
        """Load the segment the target reads next in a thread, so read() does not gunzip on the event loop. A file
        that cannot be read is left to read(), which reports it."""
        seq, _line = self._normalize(target)
        seg = self._segment(seq)
        if seg is None or seg.lines is not None or seg.seq in self._cache or seg.path is None:
            return
        try:
            lines = await _finish_in_thread(_read_gz, seg.path)
        except (OSError, EOFError, zlib.error, UnicodeDecodeError):
            return
        if seg in self.segments and seg.lines is None:
            self._keep(seg, lines)

    def read(self, target: str, max_lines: int) -> tuple[list[str], tuple[int, int]]:
        """Up to `max_lines` lines from the target's cursor (all from one segment), and the position after them.
        Reading again without a commit means the last batch did not go: its lines are counted if their segment went."""
        self._settle(target)
        seq, line = self._normalize(target)
        seg = self._segment(seq)
        if seg is None:
            return [], (seq, line)
        lines = self._lines(seg)
        if lines is None:  # unreadable: removed, its samples counted as dropped
            return self.read(target, max_lines)
        chunk = lines[line: line + max_lines]
        if chunk:
            self._inflight[target] = (seq, line, line + len(chunk))
        return chunk, (seq, line + len(chunk))

    def _settle(self, target: str) -> None:
        self._inflight.pop(target, None)
        if lost := self._unconfirmed.pop(target, 0):
            self.dropped[target] += lost
            log.error("outbox: %d samples for %s lost (cap, while they were being pushed)", lost, target)

    def _lines(self, seg: Segment) -> list[str] | None:
        """The lines of a segment; [] if it cannot be read right now; None if it is unreadable (then removed)."""
        if seg.lines is not None:
            return seg.lines
        if seg.seq in self._cache:
            self._cache.move_to_end(seg.seq)
            return self._cache[seg.seq]
        try:
            with open(seg.path, "rb") as f:  # type: ignore[arg-type]
                raw = f.read()
        except FileNotFoundError:
            raw = None
        except OSError as err:  # EMFILE, EIO: the next read tries again
            log.warning("outbox: cannot read segment %d right now (%s)", seg.seq, err)
            return []
        try:
            if raw is None:
                raise FileNotFoundError("the file is gone")
            lines = _decompress(raw)
        except (OSError, EOFError, zlib.error, UnicodeDecodeError) as err:  # gzip.BadGzipFile is an OSError
            log.error("outbox: segment %d is unreadable (%s); moved to rejected/, its samples are lost", seg.seq, err)
            if seg.path is not None and seg.path.exists():
                self._reject_file(seg.path)
            self._remove(seg, "unreadable")
            return None
        self._keep(seg, lines)
        return lines

    def _keep(self, seg: Segment, lines: list[str]) -> None:
        """Cache a sealed segment's lines; its name's line count yields to what the file holds."""
        if len(lines) != seg.count:
            log.warning("outbox: segment %d holds %d lines, its name says %d", seg.seq, len(lines), seg.count)
            seg.count = len(lines)
        self._cache[seg.seq] = lines
        self._trim_cache()

    def _trim_cache(self) -> None:
        while len(self._cache) > CACHE_SEGMENTS:
            self._cache.popitem(last=False)

    def commit(self, target: str, pos: tuple[int, int]) -> None:
        """The store took everything before `pos` (or refused it: then it is in rejected/)."""
        self._inflight.pop(target, None)
        self._unconfirmed.pop(target, None)
        self.cursors[target] = pos
        self._write_cursor(target)

    def reject(self, target: str, lines: list[str]) -> None:
        """A batch the store refused (4xx): kept in rejected/ for a human, then skipped."""
        name = f"{target}-{int(self.wall() * 1000)}.prom.gz"
        try:
            (self.dir / "rejected" / name).write_bytes(gzip.compress("".join(lines).encode(), mtime=0))
        except OSError as err:
            log.error("outbox: cannot keep the refused batch (%s)", err)

    def lag(self, target: str) -> tuple[int, float]:
        """(samples behind, age in seconds of the oldest of them) for a target."""
        if target not in self.cursors:
            return 0, 0.0
        seq, line = self._normalize(target)
        behind, oldest = 0, None
        for seg in self.segments:
            if seg.seq < seq:
                continue
            start = line if seg.seq == seq else 0
            if seg.count > start:
                behind += seg.count - start
                if oldest is None:
                    oldest = seg.appended_ms(start)
        age = 0.0 if oldest is None else max(0.0, self.wall() - oldest / 1000)
        return behind, age

    # ---- garbage collection ----------------------------------------------------

    def _collect(self) -> None:
        """Delete segments every target is past; above the cap, the oldest anyway."""
        while self.segments and self.segments[0].sealed:
            seg = self.segments[0]
            if all(self._past(t, seg) for t in self.targets):
                self._remove(seg, None)
            else:
                break
        while self.disk_bytes() > self.max_bytes:
            seg = next((s for s in self.segments if s.sealed and s.path is not None), None)
            if seg is None:
                break
            log.error("outbox: over %d bytes; dropping segment %d (%d samples) for the targets behind", self.max_bytes,
                      seg.seq, seg.count)
            self._remove(seg, "cap")

    def _past(self, target: str, seg: Segment) -> bool:
        seq, line = self.cursors.get(target, self.end())
        return seq > seg.seq or (seq == seg.seq and line >= seg.count)

    def _remove(self, seg: Segment, why: str | None) -> None:
        """Forget a segment; with a reason, the targets still needing it lose its rest (counted). Only a cursor
        inside it moves on: one on an earlier segment still reads that, and then skips the gap (_normalize). A
        batch being pushed from it counts only if the push fails (read() again without commit())."""
        if why is not None:
            for target in self.targets:
                seq, line = self.cursors.get(target, self.end())
                if seq > seg.seq:
                    continue
                lost = seg.count if seq < seg.seq else max(0, seg.count - line)
                if seq == seg.seq:
                    held = self._inflight.get(target)
                    if held is not None and held[:2] == (seq, line):
                        sending = min(held[2], seg.count) - line
                        self._unconfirmed[target] += sending
                        lost -= sending
                    self.cursors[target] = (seg.seq + 1, 0)
                    self._write_cursor(target)
                if lost > 0:
                    self.dropped[target] += lost
                    log.error("outbox: %d samples for %s lost (%s)", lost, target, why)
        if seg.path is not None and seg.path.exists():
            try:
                seg.path.unlink()
            except OSError as err:
                log.warning("outbox: cannot delete %s (%s)", seg.path.name, err)
        self.segments.remove(seg)
        self._cache.pop(seg.seq, None)

    def disk_bytes(self) -> int:
        return sum(s.bytes for s in self.segments if s.path is not None)

    # ---- files ---------------------------------------------------------------

    def _write_cursor(self, target: str) -> None:
        if not self.write_cursors:
            return
        seq, line = self.cursors[target]
        path = self.dir / f"cursor-{target}"
        try:
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text(f"{seq} {line}\n")
            os.replace(tmp, path)
        except OSError as err:  # a lost cursor only re-sends
            log.debug("outbox: cannot write the cursor of %s (%s)", target, err)

    def _reject_file(self, path: Path) -> None:
        self.rejected_files += 1
        try:
            os.replace(path, self.dir / "rejected" / path.name)
        except OSError:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass

    async def close(self) -> None:
        """Shutdown: append and fsync what is pending (the active segment is sealed at the next start)."""
        await self.flush()
        self._close_file()


def _read_cursor(path: Path) -> tuple[int, int] | None:
    try:
        seq, line = path.read_text().split()
        return int(seq), int(line)
    except (OSError, ValueError):
        return None


def _write_gz(directory: Path, name: str, lines: list[str]) -> int:
    """Compress `lines` into directory/name: .tmp, fsync, rename, fsync the directory. Returns the size."""
    data = gzip.compress("".join(lines).encode(), compresslevel=5, mtime=0)
    target = directory / name
    tmp = target.with_name(name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, target)
    _fsync_dir(directory)
    return len(data)


def _decompress(raw: bytes) -> list[str]:
    return gzip.decompress(raw).decode("utf-8").splitlines(keepends=True)


def _read_gz(path: Path) -> list[str]:
    return _decompress(path.read_bytes())


async def _finish_in_thread(func: Callable[..., T], *args: object) -> T:
    """`func(*args)` in a thread. Cancelled meanwhile, it still waits for the thread to finish before passing the
    cancellation on, so a write is never still running when the next one starts (shutdown flushes once more)."""
    task = asyncio.ensure_future(asyncio.to_thread(func, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await asyncio.wait({task})
        raise


def _probe(directory: Path) -> None:
    probe = directory / ".probe.tmp"
    with open(probe, "wb") as f:
        f.write(b"ok")
        os.fsync(f.fileno())
    probe.unlink()


def _fsync_dir(directory: Path) -> None:
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def orphaned_cursors(directory: Path, targets: list[str]) -> list[str]:
    """Targets with a cursor in `directory` that are not configured (check-config reports them)."""
    try:
        return sorted(p.name.removeprefix("cursor-") for p in directory.glob("cursor-*")
                      if not p.name.endswith(".tmp") and p.name.removeprefix("cursor-") not in targets)
    except OSError:
        return []
