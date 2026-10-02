"""The Prometheus text exposition format (version 0.0.4), written by hand.

Replaces prometheus_client (design 4.2): the exporter's own metrics are read
from live objects at scrape time, so a writer is all that is needed, and it
gives full control of label escaping. A family is written only if it has a
sample; counters are named with their `_total` suffix.
"""

from __future__ import annotations

import math
import os
import resource
import sys
import time
from collections.abc import Iterable, Sequence

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_START = time.time()


def escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _escape_help(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\n", "\\n")


def format_number(value: float) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if value == int(value) and abs(value) < 1e15:
        return str(int(value))
    return repr(value)


class Family:
    """One metric family: name, type, help, and its samples."""

    def __init__(self, name: str, kind: str, help: str, labels: Sequence[str] = ()) -> None:
        if kind not in ("gauge", "counter", "histogram"):
            raise ValueError(kind)
        if kind == "counter" and not name.endswith("_total"):
            raise ValueError(f"counter {name} must end with _total")
        self.name, self.kind, self.help, self.labels = name, kind, help, tuple(labels)
        self.samples: list[tuple[str, tuple[str, ...], float]] = []  # (suffix, label values, value)

    def add(self, values: Sequence[str], value: float) -> Family:
        if len(values) != len(self.labels):
            raise ValueError(f"{self.name}: {len(values)} label values for {self.labels}")
        self.samples.append(("", tuple(str(v) for v in values), value))
        return self

    def histogram(self, values: Sequence[str], cumulative: Iterable[tuple[str, int]], total: float) -> Family:
        """`cumulative`: [(le, count up to le)], the last le "+Inf"."""
        count = 0
        for le, n in cumulative:
            self.samples.append(("_bucket", (*values, le), n))
            count = n
        self.samples.append(("_sum", tuple(values), total))
        self.samples.append(("_count", tuple(values), count))
        return self

    def render(self) -> str:
        if not self.samples:
            return ""
        out = [f"# HELP {self.name} {_escape_help(self.help)}", f"# TYPE {self.name} {self.kind}"]
        for suffix, values, value in self.samples:
            names = (*self.labels, "le") if suffix == "_bucket" else self.labels
            labels = ",".join(f'{n}="{escape_label(v)}"' for n, v in zip(names, values))
            out.append(f"{self.name}{suffix}{{{labels}}} {format_number(value)}" if labels
                       else f"{self.name}{suffix} {format_number(value)}")
        return "\n".join(out) + "\n"


def gauge(name: str, help: str, labels: Sequence[str] = ()) -> Family:
    return Family(name, "gauge", help, labels)


def counter(name: str, help: str, labels: Sequence[str] = ()) -> Family:
    return Family(name, "counter", help, labels)


def histogram(name: str, help: str, labels: Sequence[str] = ()) -> Family:
    return Family(name, "histogram", help, labels)


def render(families: Iterable[Family]) -> bytes:
    return "".join(f.render() for f in families).encode()


def process_families() -> list[Family]:
    """The process_* basics: memory, CPU, open files, start time (Linux /proc, else getrusage)."""
    out = []
    usage = resource.getrusage(resource.RUSAGE_SELF)
    out.append(counter("process_cpu_seconds_total", "User and system CPU time.").add((), usage.ru_utime + usage.ru_stime))
    rss = None
    try:
        with open("/proc/self/statm") as f:
            rss = int(f.read().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        rss = usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)  # a peak, where /proc is missing
    out.append(gauge("process_resident_memory_bytes", "Resident memory.").add((), rss))
    try:
        out.append(gauge("process_open_fds", "Open file descriptors.").add((), len(os.listdir("/proc/self/fd"))))
    except OSError:
        pass
    out.append(gauge("process_start_time_seconds", "Start time, seconds since the epoch.").add((), _START))
    info = sys.version_info
    out.append(gauge("python_info", "Python version.", ("implementation", "version"))
               .add((sys.implementation.name, f"{info.major}.{info.minor}.{info.micro}"), 1))
    return out
