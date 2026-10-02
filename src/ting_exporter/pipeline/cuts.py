"""Inferred power cuts and data gaps (design 5.4).

A Ting is powered from the outlet it measures, so it cannot report its own
power cut: it goes dark, and when the power returns it restarts. The restart
resets its own long-window high and low (VoltageHi/VoltageLo, normally 4-10 V
apart) onto the current voltage, so the first sample after the return has them
within COLLAPSE_VOLTS of each other, and the window has shrunk (a lower high
or a higher low than before the silence). Ting's notification history misses
some cuts, so they are inferred here from the stream:

    a silence of at least SILENCE_MS in the sensor's device time, then
      restarted (narrow and shrunk)  ting_power_cut  (the site lost power)
      otherwise                      ting_stream_gap (the sensor's network or the cloud)

"Shrunk" matters right after a restart: a sensor that came back with a fresh,
narrow window, went dark again a minute later and then returned shows the same
window, slightly wider: narrow, but not a second restart.

Each writes 1 at the first missing 0.25 s slot, 1 at every whole minute of
the silence, and 0 at the first returned sample. Both exporters see the same
device timestamps, so their samples coincide.

Only a silence this exporter actually watched is attributed to the sensor:

- watched: no sign since the last sample that this exporter was not listening
  (a failed connect or socket error, a refused subscription, waiting for a
  sign-in). Required for both kinds; otherwise the silence may be ours.
- confirmed: a subscription during the silence stayed empty for CONFIRM_S,
  so the hub really had nothing. Required for a gap, which has no evidence of
  its own; a cut has the restart.
- restored: the last sample is from before a restart of this exporter (the
  state file, or a `start` line in a replay). A cut is still inferred if the
  exporter was down at most RESTORE_MAX_S; a gap never is.

Everything is computed from device times and arrival times passed in, so a
replay of the flight recorder infers exactly what the live exporter did.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

CUT_METRIC = "ting_power_cut"
GAP_METRIC = "ting_stream_gap"
SILENCE_MS = 60_000  # 1.6 x the longest natural silence seen (37 s)
SLOT_MS = 250
MINUTE_MS = 60_000
COLLAPSE_VOLTS = 2.5
CONFIRM_S = 10.0  # an empty subscription for this long: the hub had nothing (its catch-up is ~6 s)
PENDING_MAX = 20  # returned samples without Hi/Lo before the silence is decided without them
MAX_DRAW_MS = 31 * 86_400_000  # a longer silence is logged, not drawn
RESTORE_MAX_S = 600.0  # a restart longer than this observed nothing: the restored state is not used
BLIND_ENDS = frozenset({"ws_error", "error"})  # session ends that mean we may not have been listening
BLIND_EVENTS = frozenset({"refused", "auth_wait"})


@dataclass
class Silence:
    serial: str
    kind: str  # "cut" | "gap"
    start_ms: int
    end_ms: int

    @property
    def metric(self) -> str:
        return CUT_METRIC if self.kind == "cut" else GAP_METRIC

    def points(self) -> list[tuple[int, int]]:
        """(ts_ms, value): 1 at the start, 1 at every whole minute inside, 0 at the end."""
        out = [(self.start_ms, 1)]
        t = (self.start_ms // MINUTE_MS + 1) * MINUTE_MS
        while t < self.end_ms:
            out.append((t, 1))
            t += MINUTE_MS
        out.append((self.end_ms, 0))
        return out


@dataclass
class _Pending:
    start_ms: int
    end_ms: int
    gap_ok: bool
    high: float | None  # the window before the silence
    low: float | None
    seen: int = 0


def restarted(high: float | None, low: float | None, before_high: float | None, before_low: float | None) -> bool | None:
    """Did the sensor restart during the silence? None: this sample does not tell (no Hi/Lo)."""
    if high is None or low is None:
        return None
    if high - low >= COLLAPSE_VOLTS:
        return False
    if before_high is None or before_low is None:
        return True
    return high < before_high or low > before_low  # a window that only stayed or widened was not reset


@dataclass
class _Sensor:
    last_ms: int | None = None
    last_high: float | None = None  # the sensor's own window as of the newest sample
    last_low: float | None = None
    watched: bool = False
    restored: bool = False
    confirmed: bool = False
    subscribed_at: float | None = None  # arrival time of the newest subscription not yet followed by data
    pending: _Pending | None = None
    found: dict[str, int] = field(default_factory=lambda: {"cut": 0, "gap": 0, "unattributed": 0})


class SilenceTracker:
    def __init__(self) -> None:
        self.sensors: dict[str, _Sensor] = {}

    def _sensor(self, serial: str) -> _Sensor:
        return self.sensors.setdefault(serial, _Sensor())

    # ---- inputs ----------------------------------------------------------------

    def event(self, serial: str, kind: str, fields: dict[str, Any], t: float) -> None:
        """A hub session event (live, or a recorded one in a replay)."""
        st = self._sensor(serial)
        if kind in BLIND_EVENTS or (kind == "ended" and fields.get("reason") in BLIND_ENDS):
            st.watched = False
        elif kind == "subscribed":
            if st.subscribed_at is not None and t - st.subscribed_at >= CONFIRM_S:
                st.confirmed = True  # the previous subscription stayed empty
            st.subscribed_at = t

    def restart(self, down_seconds: float | None) -> None:
        """This exporter restarted after `down_seconds` (None: unknown) without observing anything."""
        for st in self.sensors.values():
            st.restored = True
            st.subscribed_at, st.confirmed, st.pending = None, False, None
            if down_seconds is None or down_seconds > RESTORE_MAX_S:
                st.watched = False

    def primary(self, serial: str, ts_ms: int, arrival: float, high: float | None, low: float | None) -> list[Silence]:
        """A voltage sample (device time, arrival time, the sensor's own high and low if sent)."""
        st = self._sensor(serial)
        out: list[Silence] = []
        if st.pending is not None:
            p = st.pending
            p.seen += 1
            verdict = restarted(high, low, p.high, p.low)
            if verdict is not None or p.seen >= PENDING_MAX:
                out += self._decide(serial, st, p.start_ms, p.end_ms, bool(verdict), p.gap_ok)
                st.pending = None
        if st.last_ms is not None and ts_ms <= st.last_ms:
            return out  # late: the silence logic runs on the newest device time only
        if st.subscribed_at is not None and arrival - st.subscribed_at >= CONFIRM_S:
            st.confirmed = True
        if st.last_ms is not None and ts_ms - st.last_ms >= SILENCE_MS:
            start = st.last_ms + SLOT_MS
            gap_ok = st.confirmed and not st.restored
            verdict = restarted(high, low, st.last_high, st.last_low)
            if not st.watched:
                st.found["unattributed"] += 1
                log.info("%s: %.0f s without samples, not all of them watched by this exporter; not inferred",
                         serial, (ts_ms - st.last_ms) / 1000)
            elif verdict is None:
                st.pending = _Pending(start, ts_ms, gap_ok, st.last_high, st.last_low)
            else:
                out += self._decide(serial, st, start, ts_ms, verdict, gap_ok)
        st.last_ms = ts_ms
        if high is not None and low is not None:
            st.last_high, st.last_low = high, low
        st.watched, st.restored, st.confirmed, st.subscribed_at = True, False, False, None
        return out

    def _decide(self, serial: str, st: _Sensor, start: int, end: int, collapsed: bool, gap_ok: bool) -> list[Silence]:
        kind = "cut" if collapsed else ("gap" if gap_ok else None)
        if kind is None:
            st.found["unattributed"] += 1
            return []
        if end - start > MAX_DRAW_MS:
            log.warning("%s: a %s of %.1f days is not drawn (longer than %d days)", serial, kind,
                        (end - start) / 86_400_000, MAX_DRAW_MS // 86_400_000)
            return []
        st.found[kind] += 1
        log.info("%s: inferred %s of %.0f s (Hi/Lo %s)", serial, "power cut" if kind == "cut" else "data gap",
                 (end - start) / 1000, "collapsed: the sensor restarted" if collapsed else "not collapsed")
        return [Silence(serial, kind, start, end)]

    # ---- state across restarts ------------------------------------------------

    def state(self) -> dict[str, dict[str, Any]]:
        return {s: {"last_ms": st.last_ms, "watched": st.watched, "high": st.last_high, "low": st.last_low}
                for s, st in self.sensors.items() if st.last_ms is not None}

    def load(self, state: dict[str, Any], down_seconds: float | None) -> None:
        for serial, entry in state.items():
            if not isinstance(entry, dict) or not isinstance(entry.get("last_ms"), int):
                continue
            st = self._sensor(str(serial))
            st.last_ms, st.watched = entry["last_ms"], bool(entry.get("watched"))
            high, low = entry.get("high"), entry.get("low")
            if isinstance(high, (int, float)) and isinstance(low, (int, float)):
                st.last_high, st.last_low = float(high), float(low)
        self.restart(down_seconds)
