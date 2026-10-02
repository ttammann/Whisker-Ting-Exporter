"""Raw hub capture: the flight recorder in `serve`, the `record` command, and the reader for `replay`.

Every hub invocation and every connection event goes to JSON Lines, one file
per UTC hour:

    ting-20260927T21.jsonl      the hour being written (flushed every 10 s)
    ting-20260927T20.jsonl.gz   finished hours, compressed on rotation

Each line is one object:

    {"t": 1790541296.123456, "serial": "...", "kind": "invocation",
     "target": "updateComboBinaryData", "args": [...]}
    {"t": ..., "serial": "...", "kind": "event", "event": "subscribed", "elements": [...]}
    {"t": ..., "kind": "start" | "end", ...}

"t" is the time the WebSocket frame was received (wall clock). MessagePack
blobs inside args are decoded in place as {"msgpack": ...}; blobs that do not
decode are kept as {"b64": ...}; timestamps become ISO strings. Nothing
written here contains the password, tokens or API key.

updateGraphMulti, an exact duplicate of the categorical data (37 % of the
volume), is not written per message: an hourly event counts it
({"event": "graph_multi_count", "count": N}), so a protocol change there would
still show (design 7.3).

No recorder error ever reaches the stream: a failed write (the disk, a file
that will not open, a payload that will not convert) is counted, recording
pauses for five minutes and then tries again, reopening the hour's file. Text
that is not valid UTF-8 (a lone surrogate in a notification) is written as a
JSON escape, never a failed write. A finished hour that fails to compress is
counted and logged, and compressed on the next start. Compression writes
`.gz.tmp` and renames it, and the plain file is deleted only after the rename
is durable, so a crash can leave an unfinished `.jsonl` (compressed on the next
start) but never a truncated or duplicated `.gz`.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import logging
import math
import os
import re
import shutil
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import msgpack

from .cloud import signalr
from .signals import DUPLICATE_TARGET

log = logging.getLogger(__name__)

FLUSH_INTERVAL = 10.0
ERROR_PAUSE = 300.0
SWEEP_INTERVAL = 3600.0
PREFIX = "ting"
_HOURLY = re.compile(r"^(?P<prefix>.+)-(?P<hour>\d{8}T\d{2})\.jsonl(\.gz)?$")


def jsonable(obj: Any, depth: int = 0) -> Any:
    """Make a decoded hub payload JSON-safe without losing information. Never raises: a value that will not
    convert is kept as its repr (the recorder runs on the receive path)."""
    try:
        return _jsonable(obj, depth)
    except Exception:
        return repr(obj)


def _jsonable(obj: Any, depth: int) -> Any:
    if depth > 8:
        return repr(obj)
    if isinstance(obj, (bytes, bytearray)):
        try:
            return {"msgpack": jsonable(signalr.unpack(bytes(obj)), depth + 1)}
        except signalr.ProtocolError:
            return {"b64": base64.standard_b64encode(bytes(obj)).decode()}
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, msgpack.Timestamp):
        return obj.to_datetime().isoformat()
    if isinstance(obj, msgpack.ExtType):
        return {"ext": obj.code, "b64": base64.standard_b64encode(obj.data).decode()}
    if isinstance(obj, float) and not math.isfinite(obj):
        return str(obj)
    if isinstance(obj, dict):
        return {str(k): jsonable(v, depth + 1) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [jsonable(v, depth + 1) for v in obj]
    return obj


def _hour(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y%m%dT%H")


def gzip_atomic(path: Path) -> Path:
    """Compress `x.jsonl` to `x.jsonl.gz` via a temporary file; append a member if the .gz exists."""
    target = path.with_name(path.name + ".gz")
    tmp = target.with_name(target.name + ".tmp")
    with open(tmp, "wb") as out:
        if target.exists():  # the same hour was written before a restart
            with open(target, "rb") as old:
                shutil.copyfileobj(old, out)
        with open(path, "rb") as src, gzip.GzipFile(fileobj=out, mode="wb", filename="", mtime=0) as dst:
            shutil.copyfileobj(src, dst)
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, target)
    _fsync_dir(path.parent)
    path.unlink()
    return target


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


class HourlyWriter:
    """Appends JSON lines to an hourly file; gzips each hour once it is finished."""

    def __init__(self, out_dir: Path, prefix: str = PREFIX, on_error: Callable[[Exception], None] | None = None) -> None:
        self.out_dir, self.prefix = out_dir, prefix
        self.hour = ""
        self.newest = ""  # the latest hour opened: never gone back to, its file may be compressing
        self.file = None
        self.lines = 0
        self.pending: set[asyncio.Task] = set()
        self.on_error = on_error or (lambda _err: None)  # a finished hour that could not be compressed

    def write(self, record: dict[str, Any]) -> None:
        """A record stamped in an earlier hour (the clock stepped back) goes into the current file: that hour's
        may be compressing, and a line appended to it then would be lost. Replay reads it as a late record."""
        hour = max(_hour(record["t"]), self.newest)
        if hour != self.hour:
            self._rotate(hour)
        self.file.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
        self.lines += 1

    def flush(self) -> None:
        if self.file is not None:
            self.file.flush()

    def _rotate(self, hour: str) -> None:
        finished = self._close()
        if finished is not None:
            self._compress_later(finished)
        # the hour is only taken once its file is open: if open() fails, the next write tries again
        # backslashreplace: a lone surrogate becomes the JSON escape \udcxx, which reads back as the same text
        self.file = open(self.out_dir / f"{self.prefix}-{hour}.jsonl", "a", encoding="utf-8", errors="backslashreplace")
        self.hour = self.newest = hour

    def _compress_later(self, path: Path) -> None:
        task = asyncio.get_running_loop().create_task(asyncio.to_thread(gzip_atomic, path))
        self.pending.add(task)
        task.add_done_callback(lambda done: self._compressed(done, path))

    def _compressed(self, task: asyncio.Task, path: Path) -> None:
        self.pending.discard(task)
        if task.cancelled() or task.exception() is None:
            return
        log.error("flight recorder: compressing %s failed: %s; it is compressed at the next start", path.name, task.exception())
        self.on_error(task.exception())

    def _close(self) -> Path | None:
        if self.file is None:
            return None
        try:
            self.file.close()
        finally:
            path, self.file, self.hour = Path(self.file.name), None, ""
        return path

    def abandon(self) -> None:
        """Drop the file handle after an error; the next write reopens (appends)."""
        try:
            self._close()
        except Exception:  # the handle is dropped either way
            pass

    async def close(self) -> None:
        finished = self._close()
        await asyncio.gather(*self.pending, return_exceptions=True)
        if finished is not None:
            await asyncio.to_thread(gzip_atomic, finished)


def recover(out_dir: Path, prefix: str = PREFIX) -> None:
    """Compress hours left uncompressed by a crash; remove half-written temporaries."""
    for tmp in out_dir.glob(f"{prefix}-*.jsonl.gz.tmp"):
        tmp.unlink(missing_ok=True)
    for path in sorted(out_dir.glob(f"{prefix}-*.jsonl")):
        log.info("compressing %s left over from an earlier run", path.name)
        gzip_atomic(path)


def sweep(out_dir: Path, retention_days: float, now: float, prefix: str = PREFIX) -> tuple[int, int]:
    """Delete hourly files older than the retention; return (files deleted, bytes kept)."""
    cutoff = _hour(now - retention_days * 86400)
    deleted, kept = 0, 0
    for path in out_dir.iterdir():
        m = _HOURLY.match(path.name)
        if not m or m["prefix"] != prefix:
            continue
        if m["hour"] < cutoff:
            path.unlink(missing_ok=True)
            deleted += 1
        else:
            try:
                kept += path.stat().st_size
            except FileNotFoundError:
                pass
    return deleted, kept


class Recorder:
    """The flight recorder: tees everything the hub sends into hourly files.

    Every method is safe to call from the receive path: it never raises.
    """

    def __init__(self, out_dir: Path, retention_days: float, clock: Callable[[], float] = time.time, prefix: str = PREFIX,
                 keep_duplicates: bool = False) -> None:
        self.out_dir = out_dir
        self.retention_days = retention_days
        self.clock = clock
        self.errors = 0
        self.writer = HourlyWriter(out_dir, prefix, on_error=self._count_error)
        self.prefix = prefix
        self.bytes = 0
        self.deleted = 0
        self.paused_until = 0.0
        self.keep_duplicates = keep_duplicates  # `record`: every updateGraphMulti too, for analysis
        self.multi: Counter[str] = Counter()  # updateGraphMulti per serial in the current hour
        self.multi_hour = ""

    def start(self) -> None:
        try:
            self.out_dir.mkdir(parents=True, exist_ok=True)
            recover(self.out_dir, self.prefix)
        except Exception as err:
            self._failed(err)

    def invocation(self, serial: str, target: str, args: list[Any], t: float) -> None:
        if target == DUPLICATE_TARGET and not self.keep_duplicates:
            if self.multi and _hour(t) != self.multi_hour:
                self._flush_multi()
            self.multi_hour = _hour(t)
            self.multi[serial] += 1
            return
        self._write(lambda: {"t": t, "serial": serial, "kind": "invocation", "target": target, "args": jsonable(args)})

    def _flush_multi(self) -> None:
        """The hour's updateGraphMulti counts, stamped in that hour's last second."""
        counts, self.multi = self.multi, Counter()
        if not counts or not self.multi_hour:
            return
        end = datetime.strptime(self.multi_hour, "%Y%m%dT%H").replace(tzinfo=timezone.utc).timestamp() + 3599.999
        t = min(end, self.clock())
        for serial, n in sorted(counts.items()):
            self._write(lambda serial=serial, n=n: {"t": t, "serial": serial, "kind": "event", "event": "graph_multi_count",
                                                    "target": DUPLICATE_TARGET, "count": n}, flush_multi=False)

    def event(self, serial: str, kind: str, fields: dict[str, Any], t: float | None = None) -> None:
        self._write(lambda: {"t": t if t is not None else self.clock(), "serial": serial, "kind": "event", "event": kind,
                             **jsonable(fields)})

    def meta(self, kind: str, fields: dict[str, Any]) -> None:
        if kind == "end":
            self._flush_multi()  # the counts before the closing line
        self._write(lambda: {"t": self.clock(), "kind": kind, **jsonable(fields)})

    def _write(self, make: Callable[[], dict[str, Any]], flush_multi: bool = True) -> None:
        """Build and write one record. Recording is best-effort: whatever fails (the disk, a closed
        file, a payload that will not convert) is counted and pauses recording, and never reaches the
        stream that called it."""
        if self.paused_until and self.clock() < self.paused_until:
            return
        try:
            record = make()
            if flush_multi and self.multi and _hour(record["t"]) != self.multi_hour:
                self._flush_multi()  # into the hour they belong to, before this record rotates the file
            self.writer.write(record)
            self.paused_until = 0.0
        except Exception as err:
            self._failed(err)

    def _count_error(self, _err: Exception) -> None:
        self.errors += 1

    def _failed(self, err: Exception) -> None:
        self.errors += 1
        self.writer.abandon()
        self.paused_until = self.clock() + ERROR_PAUSE
        log.error("flight recorder: %s (%s); pausing recording for %.0f s", err, type(err).__name__, ERROR_PAUSE)

    @property
    def lines(self) -> int:
        return self.writer.lines

    def flush(self) -> None:
        try:
            self.writer.flush()
        except Exception as err:
            self._failed(err)

    async def sweep(self) -> None:
        try:
            deleted, self.bytes = await asyncio.to_thread(sweep, self.out_dir, self.retention_days, self.clock(), self.prefix)
        except OSError as err:
            log.warning("flight recorder retention sweep failed: %s", err)
            return
        if deleted:
            self.deleted += deleted
            log.info("flight recorder: deleted %d files older than %g days", deleted, self.retention_days)

    async def run(self, stop: asyncio.Event) -> None:
        """Flush every 10 s, apply the retention every hour."""
        next_sweep = 0.0
        while not stop.is_set():
            self.flush()
            if time.monotonic() >= next_sweep:
                next_sweep = time.monotonic() + SWEEP_INTERVAL
                await self.sweep()
            try:
                await asyncio.wait_for(stop.wait(), FLUSH_INTERVAL)
            except asyncio.TimeoutError:
                pass

    async def close(self) -> None:
        self._flush_multi()
        try:
            await self.writer.close()
        except Exception as err:  # shutdown goes on: the hour is compressed at the next start
            log.error("flight recorder: closing failed: %s (%s)", err, type(err).__name__)


# ---- reading ------------------------------------------------------------------


def _in_time_order(path: Path) -> tuple[str, int]:
    """By name, i.e. by hour; within one hour the .jsonl.gz (written before a restart) before the .jsonl (after it)."""
    name = path.name
    return (name + ".gz", 1) if name.endswith(".jsonl") else (name, 0)


def expand(paths: Iterable[str | Path]) -> list[Path]:
    """Files and directories (every *.jsonl / *.jsonl.gz inside). Hourly files are put in time order, also when
    they are named one by one (a shell glob sorts an hour's .jsonl before its .jsonl.gz); other files keep the
    order given."""
    out: list[Path] = []
    for p in map(Path, paths):
        if p.is_dir():
            out += sorted((q for q in p.iterdir() if q.name.endswith((".jsonl", ".jsonl.gz"))), key=_in_time_order)
        else:
            out.append(p)
    if out and all(_HOURLY.match(p.name) for p in out):
        out.sort(key=_in_time_order)
    return out


def read_records(paths: Iterable[Path], window: tuple[float | None, float | None] = (None, None)) -> Iterator[dict[str, Any]]:
    """Every record of every file in order. A truncated tail (a crash) ends that file with a warning."""
    start, end = window
    for path in paths:
        opener = gzip.open if path.suffix == ".gz" else open
        bad = 0
        try:
            with opener(path, "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        bad += 1
                        continue
                    t = record.get("t")
                    if not isinstance(t, (int, float)):
                        bad += 1
                        continue
                    if (start is not None and t < start) or (end is not None and t >= end):
                        continue
                    yield record
        except (EOFError, gzip.BadGzipFile, zlib.error) as err:
            log.warning("%s: truncated (%s); using what was readable", path, err)
        if bad:
            log.warning("%s: skipped %d unreadable lines", path, bad)


def parse_when(text: str) -> float:
    """ISO time (UTC if no zone) or unix seconds."""
    try:
        return float(text)
    except ValueError:
        pass
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()

