"""Ting notifications: what the phone app alerts on, as VictoriaMetrics series.

The Ting cloud keeps a per-account notification history
(GET /api/v1/Notifications/history/{user_id}, see cloud/api.py), about three
months deep. Each record has an id, an eventType, an eventCategory, a title, a
message and a time. Seen eventTypes:

    PowerOutage              the power went out at the sensor's site
    CommunityPowerOutage     the same, and other Ting homes nearby lost power too
    PowerRestored            power is back
    PowerOrInternetRestored  power and the internet connection are back
    PowerOutageAndRestored   a short outage, reported once both ended
    Sag, Swell               brownout, surge
    (FireHazard, WeatherAlert, FrozenPipe: named by the community integrations)

A Ting is powered from the outlet it measures, so it cannot record its own
outage; these notifications are the authoritative source for outages, and
they cover times when this exporter was down too. Not every disturbance is
reported: a brownout the sensor rode through (it kept measuring) is not.
Brownouts and cuts are separate events: a brownout may come before a cut or
after it (the power back, but low), or on its own.

Series (labels serial and site, like every other series):

    ting_notification{type, category, title}  1 at the event time, one sample per notification
    ting_power_outage                          while an outage lasts, every minute: 1 = at the site,
                                               2 = community-wide (other Ting homes too); 0 when it ends

The kind is the value, not a label: a PowerOutage later upgraded by a
CommunityPowerOutage rewrites the same samples with 2, and VictoriaMetrics
keeps the larger value on equal timestamps, so no orphaned series is left.
An open outage is written only up to OPEN_LAG before now: the restore
notification arrives within about a poll interval, so no minute is written
after the real restore. Everything is deterministic for a given history and
time; pushing it again changes nothing. Message texts can name the place, so
they never become labels; the flight recorder keeps the raw records.

How an outage ends (outages()): at the earliest of its restore (however
late), a notification only a powered sensor sends (POWERED: the power is
back, if low), or the next cut (its restore was missed). One with no news at
all for OUTAGE_MAX_MS is drawn that long and no further, without a 0, so a
restore that still comes closes it where it belongs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import Any

from .pipeline.decode import parse_time, to_ms
from .pipeline.model import Sample, label_text

NOTIFICATION_METRIC = "ting_notification"
OUTAGE_METRIC = "ting_power_outage"

OUTAGE_START = {"PowerOutage": 1, "CommunityPowerOutage": 2}  # eventType -> value (kind)
KIND_NAME = {1: "site", 2: "community"}
OUTAGE_POINT = {"PowerOutageAndRestored"}  # start and end in one record; drawn as one minute
POWERED = {"Sag", "Swell"}  # only a powered, measuring sensor sends these: an open outage is over
MINUTE_MS = 60_000
OPEN_LAG_MS = 3 * MINUTE_MS  # an open outage is written up to this long before now
OUTAGE_MAX_MS = 24 * 3_600_000  # an outage with no news at all is drawn this long and no further
COMMUNITY_LAG_MS = 5 * MINUTE_MS  # Ting may call a cut community-wide this long after it (seen: 56 s)
EARLIEST_MS = 1_577_836_800_000  # 2020-01-01: older times are placeholders (the API sends year 1 in eventTimestampUtc)
FUTURE_SLACK_MS = 2 * 86_400_000
TIME_FIELDS = ("eventTimestampUtc", "eventTimestampLocal", "sentUtc")


def is_restore(event_type: str) -> bool:
    """PowerRestored, PowerOrInternetRestored and any future *Restored ends an open outage."""
    return event_type.endswith("Restored") and event_type not in OUTAGE_POINT


@dataclass(frozen=True)
class Notification:
    id: str
    serial: str
    type: str
    category: str
    title: str
    ts_ms: int


@dataclass(frozen=True)
class Outage:
    serial: str
    kind: int  # 1 = at the site, 2 = community
    start_ms: int
    end_ms: int | None  # None while still out
    capped: bool = False  # no news for OUTAGE_MAX_MS: drawn to start + OUTAGE_MAX_MS, no closing 0


def event_ms(record: dict[str, Any], now_ms: int | None = None) -> int | None:
    """The event time: the first of TIME_FIELDS that parses to a plausible time (2020 .. now + 2 days).

    eventTimestampLocal carries its UTC offset, so it is as good as the UTC field when that one is a placeholder.
    """
    real_ms = int(time.time() * 1000)  # the later of the given (possibly simulated) and the real clock
    latest = max(now_ms if now_ms is not None else real_ms, real_ms) + FUTURE_SLACK_MS
    for key in TIME_FIELDS:
        when = parse_time(record.get(key))
        if when is None:
            continue
        try:
            ms = to_ms(when)
        except (OverflowError, ValueError):
            continue
        if EARLIEST_MS <= ms <= latest:
            return ms
    return None


def parse(record: dict[str, Any], now_ms: int | None = None) -> Notification | None:
    """One history record, or None if it has no id, serial or plausible time."""
    ident, serial = record.get("id"), record.get("serialNumber")
    ts_ms = event_ms(record, now_ms)
    if ident in (None, "") or not serial or ts_ms is None:
        return None
    return Notification(
        id=str(ident),
        serial=str(serial),
        type=str(record.get("eventType") or "unknown"),
        category=str(record.get("eventCategory") or ""),
        title=str(record.get("title") or ""),
        ts_ms=ts_ms,
    )


def ends_an_outage(event_type: str) -> bool:
    """A restore, a sign of power (a brownout, a surge) or the next cut: whatever was open is over."""
    return is_restore(event_type) or event_type in POWERED or event_type in OUTAGE_POINT or event_type == "PowerOutage"


def outages(notes: list[Notification], now_ms: int) -> list[Outage]:
    """Pair outage starts with their end, per sensor (see the module docstring for the rules).

    A CommunityPowerOutage upgrades the open outage, or the one that ended at most COMMUNITY_LAG_MS before
    it (Ting classifies a short cut late). On equal timestamps a start comes before its end.
    """
    out: list[Outage] = []
    by_serial: dict[str, list[Notification]] = {}
    for n in sorted(notes, key=lambda n: (n.ts_ms, n.type not in OUTAGE_START, n.id)):
        by_serial.setdefault(n.serial, []).append(n)
    for serial, events in sorted(by_serial.items()):
        start: int | None = None
        kind = 1
        last: int | None = None  # this sensor's last closed outage, in `out`
        for n in events:
            if start is not None and ends_an_outage(n.type):
                out.append(Outage(serial, kind, start, n.ts_ms))
                start, last = None, len(out) - 1
            if n.type == "PowerOutage":
                start, kind = n.ts_ms, 1
            elif n.type == "CommunityPowerOutage":
                if start is not None:
                    kind = 2
                elif last is not None and n.ts_ms - out[last].end_ms <= COMMUNITY_LAG_MS:
                    out[last] = replace(out[last], kind=2)
                else:
                    start, kind = n.ts_ms, 2
            elif n.type in OUTAGE_POINT:
                out.append(Outage(serial, 1, n.ts_ms, n.ts_ms + MINUTE_MS))
                last = len(out) - 1
        if start is not None:
            if now_ms - start > OUTAGE_MAX_MS:
                out.append(Outage(serial, kind, start, start + OUTAGE_MAX_MS, capped=True))
            else:
                out.append(Outage(serial, kind, start, None))
    return out


def samples(notes: list[Notification], sites: dict[str, str], now_ms: int) -> list[Sample]:
    """Every sample the history implies, deterministic for a given history and now."""
    def labels(serial: str, **extra: str) -> str:
        return label_text({"serial": serial, "site": sites.get(serial, "unknown"), **extra})

    out = [
        Sample(NOTIFICATION_METRIC, n.serial, labels(n.serial, type=n.type, category=n.category, title=n.title), n.ts_ms, 1.0, "1")
        for n in sorted(notes, key=lambda n: (n.ts_ms, n.id))
    ]
    for o in outages(notes, now_ms):
        lab, value = labels(o.serial), float(o.kind)
        text = str(o.kind)
        until = o.end_ms if o.end_ms is not None else now_ms - OPEN_LAG_MS
        out.append(Sample(OUTAGE_METRIC, o.serial, lab, o.start_ms, value, text))
        t = (o.start_ms // MINUTE_MS + 1) * MINUTE_MS  # then every whole minute while it lasts
        while t < until:
            out.append(Sample(OUTAGE_METRIC, o.serial, lab, t, value, text))
            t += MINUTE_MS
        if o.end_ms is not None and not o.capped:
            out.append(Sample(OUTAGE_METRIC, o.serial, lab, o.end_ms, 0.0, "0"))
    return out


class Tracker:
    """Keeps the history seen so far and hands out only samples not pushed before."""

    def __init__(self, sites: dict[str, str]) -> None:
        self.sites = sites
        self.notes: dict[str, Notification] = {}
        self.history: set[str] = set()  # ids in the last poll: what the account's history holds now
        self.pushed: set[tuple[str, str, int, str]] = set()
        self.unparsable = 0  # records of the last poll without an id, serial or plausible time

    def update(self, records: list[dict[str, Any]], now_ms: int) -> tuple[list[Sample], list[tuple[Notification, dict[str, Any]]]]:
        """New samples to push, and the notifications seen for the first time with their raw records."""
        fresh: list[tuple[Notification, dict[str, Any]]] = []
        self.unparsable = 0
        history: set[str] = set()
        for record in records:
            n = parse(record, now_ms)
            if n is None:
                self.unparsable += 1
                continue
            history.add(n.id)
            if n.id not in self.notes:
                self.notes[n.id] = n
                fresh.append((n, record))
        self.history = history
        self._forget(history)
        new: list[Sample] = []
        for s in samples(list(self.notes.values()), self.sites, now_ms):
            key = (s.metric, s.labels, s.ts_ms, s.text)  # the value too: an upgrade to community rewrites 1 as 2
            if key not in self.pushed:
                self.pushed.add(key)
                new.append(s)
        return new, fresh

    def _forget(self, history: set[str]) -> None:
        """Drop records that left the server's history and are older than anything they could still pair with.
        Their samples were pushed long ago; nothing derived from what is kept can fall before the cutoff."""
        if not history:  # an empty answer may be a hiccup: keep everything
            return
        cutoff = min(self.notes[i].ts_ms for i in history) - OUTAGE_MAX_MS
        self.notes = {i: n for i, n in self.notes.items() if i in history or n.ts_ms >= cutoff}
        self.pushed = {key for key in self.pushed if key[2] >= cutoff}

    def counts(self) -> dict[tuple[str, str], int]:
        """Notifications in the account's history as of the last poll, by (serial, type)."""
        out: dict[tuple[str, str], int] = {}
        for i in self.history:
            n = self.notes[i]
            out[(n.serial, n.type)] = out.get((n.serial, n.type), 0) + 1
        return out

    def open_outages(self, now_ms: int) -> dict[str, str]:
        """serial -> "site" | "community" for outages without an end yet (not capped)."""
        return {o.serial: KIND_NAME[o.kind] for o in outages(list(self.notes.values()), now_ms) if o.end_ms is None}
