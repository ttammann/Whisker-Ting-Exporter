"""Context marks: what a site's sensor was measuring, e.g. its power source (design 0.4).

    ting-exporter mark b source=inverter          from now on, site b's sensor is on the inverter
    ting-exporter mark b source=mains --at 2026-04-01T18:30
    ting-exporter mark b source=                  the context ends (no value)
    ting-exporter mark --list                     what is set now, per site

Series: ting_context{serial, site, context, value} = 1 from the mark on, and 0
at the moment a later mark replaces or ends it. Panels select by context with

    last_over_time(ting_context{context="source", value="inverter"}[5y]) == 1

(the window only has to reach back to the mark; the series has a handful of
points, so the query is cheap). The current value is looked up in the local
store (including the last seconds, which VictoriaMetrics hides by default),
so a mark can be run on either host. The samples go into the outbox's inbox,
and the running exporter takes them at its next push (every 5 s) and delivers
them to every store; with --vm-url they are written to that one store directly.
A mark still in the inbox counts as current too. One the exporter has taken
but not yet delivered to the local store (it is down, or behind) does not: a
second mark of that site and context then does not end it.
"""

from __future__ import annotations

import itertools
import os
import re
import time
from pathlib import Path

import aiohttp

from .pipeline.model import label_text
from .vmquery import VmQuery

METRIC = "ting_context"
KEY = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
VALUE = re.compile(r"^[A-Za-z0-9_.+-]{1,64}$")
LOOKBACK = "5y"
_LINE = re.compile(rf"^{METRIC}\{{([^}}]*)\}} (\S+) (-?\d+)$")
_LABEL = re.compile(r'(\w+)="([^"\\]*)"')
_SEQ = itertools.count()  # two marks of one process within a millisecond get two files


class MarkError(ValueError):
    pass


def parse_assignment(text: str) -> tuple[str, str | None]:
    """'source=inverter' -> ('source', 'inverter'); 'source=' -> ('source', None)."""
    key, sep, value = text.partition("=")
    key, value = key.strip(), value.strip()
    if not sep or not KEY.match(key):
        raise MarkError(f"{text!r}: write context=value, the context in lower case (e.g. source=inverter)")
    if value and not VALUE.match(value):
        raise MarkError(f"{value!r}: 1-64 letters, digits, '_', '.', '+' or '-'")
    return key, value or None


def lines(serial: str, site: str, key: str, value: str | None, ending: list[str], ts_ms: int) -> list[str]:
    """0 for every value that ends, 1 for the new one, all at ts_ms."""
    out = []
    for old in sorted(set(ending) - {value}):
        out.append(f"{METRIC}{{{label_text({'serial': serial, 'site': site, 'context': key, 'value': old})}}} 0 {ts_ms}\n")
    if value is not None:
        out.append(f"{METRIC}{{{label_text({'serial': serial, 'site': site, 'context': key, 'value': value})}}} 1 {ts_ms}\n")
    return out


async def active(vm: VmQuery, *, at: float, site: str | None = None, key: str | None = None) -> list[dict[str, str]]:
    """The context values in force at `at`: labels of the series whose last point is 1."""
    matchers = ",".join(f'{k}="{v}"' for k, v in (("site", site), ("context", key)) if v)
    result = await vm.query(f"last_over_time({METRIC}{{{matchers}}}[{LOOKBACK}]) == 1", at, fresh=True)
    return sorted((labels for labels, _ in result), key=lambda m: (m.get("site", ""), m.get("context", ""), m.get("value", "")))


def in_inbox(outbox_dir: Path, at_ms: int) -> list[tuple[dict[str, str], float]]:
    """Marks still in the inbox (the exporter has not taken them yet): (labels, value) of each series' last point at
    or before at_ms, on equal timestamps the larger, as VictoriaMetrics keeps."""
    last: dict[tuple[str, str, str], tuple[int, float, dict[str, str]]] = {}
    try:
        paths = sorted((outbox_dir / "inbox").glob("*.prom"))
    except OSError:
        return []
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:  # taken by the exporter meanwhile
            continue
        for line in text.splitlines():
            m = _LINE.match(line.strip())
            if not m or int(m[3]) > at_ms:
                continue
            labels = dict(_LABEL.findall(m[1]))
            if not all(labels.get(k) for k in ("site", "context", "value")):
                continue
            series = (labels["site"], labels["context"], labels["value"])
            point = (int(m[3]), float(m[2]), labels)
            if series not in last or point[:2] > last[series][:2]:
                last[series] = point
    return [(labels, v) for _ts, v, labels in last.values()]


async def current(vm: VmQuery, *, at: float, site: str | None = None, key: str | None = None,
                  outbox_dir: Path | None = None) -> list[dict[str, str]]:
    """active(), and with `outbox_dir` the marks still in its inbox: not in the store yet, so newer than what it has."""
    rows = {(m.get("site", ""), m.get("context", ""), m.get("value", "")): m
            for m in await active(vm, at=at, site=site, key=key)}
    if outbox_dir is not None:
        for labels, v in in_inbox(outbox_dir, int(at * 1000)):
            if (site and labels["site"] != site) or (key and labels["context"] != key):
                continue
            series = (labels["site"], labels["context"], labels["value"])
            if v == 1:
                rows[series] = labels
            else:
                rows.pop(series, None)
    return [rows[series] for series in sorted(rows)]


def write_inbox(outbox_dir: Path, text_lines: list[str]) -> Path:
    """Hand the lines to the running exporter: inbox/mark-<ms>-<pid>-<n>.prom, written as .tmp and renamed."""
    inbox = outbox_dir / "inbox"
    if not inbox.is_dir():
        raise MarkError(f"{inbox} does not exist: run this inside the exporter's container, or use --vm-url")
    path = inbox / f"mark-{int(time.time() * 1000)}-{os.getpid()}-{next(_SEQ)}.prom"
    tmp = path.with_suffix(".tmp")
    tmp.write_text("".join(text_lines), encoding="utf-8")
    os.replace(tmp, path)
    return path


async def mark(session: aiohttp.ClientSession, *, store: str, serial: str, site: str, key: str, value: str | None,
               at: float, outbox_dir: Path | None) -> tuple[list[str], str]:
    """Write a mark. Returns (the lines, where they went). Raises MarkError or vmquery.VmError."""
    vm = VmQuery(session, store)
    now = sorted({m["value"] for m in await current(vm, at=at, site=site, key=key, outbox_dir=outbox_dir)
                  if m.get("value")})
    if value is not None and now == [value]:
        raise MarkError(f"site {site}: {key}={value} is already set")
    if value is None and not now:
        raise MarkError(f"site {site}: no {key} is set")
    text_lines = lines(serial, site, key, value, now, int(at * 1000))
    if outbox_dir is not None:
        return text_lines, f"the exporter's outbox ({write_inbox(outbox_dir, text_lines).name}), for every store"
    await vm.import_lines(text_lines)
    return text_lines, store
