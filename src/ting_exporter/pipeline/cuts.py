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

Not a power cut when two sites "restart" together: on 2026-09-29 both sensors,
at sites on different grids, went silent for 67 s and came back with their
windows reset, seconds apart. Two grids do not lose power in the same minute;
the cause was upstream (the Ting cloud). So a cut is held for HOLD_MS of the
sensor's own device time after its return, and dropped (unattributed) if a
sensor at another site also returned restarted within CORRELATE_MS of it. A
held cut is decided at the latest at shutdown or at the end of a replay
(finish), and kept in the state file across a crash.

Everything is computed from device times and arrival times passed in, so a
replay of the flight recorder infers exactly what the live exporter did.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
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
HOLD_MS = 120_000  # a cut waits this long (the sensor's device time) for another site's restart
CORRELATE_MS = 60_000  # two sites' restarts this close together: upstream, not a power cut
RESTARTS_KEPT_MS = 3_600_000  # restart observations older than this (behind the newest) are forgotten
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
class _Held:
    silence: Silence
    release_ms: int  # decided once the sensor's device time reaches this


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
    def __init__(self, site_of: Callable[[str], str] | None = None) -> None:
        self.sensors: dict[str, _Sensor] = {}
        self.site_of = site_of or (lambda serial: serial)
        self.restarts: list[tuple[str, int]] = []  # (serial, return ms) of every silence that ended in a restart
        self.held: list[_Held] = []

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
                if verdict:
                    self._saw_restart(serial, p.end_ms)
                out += self._decide(serial, st, p.start_ms, p.end_ms, bool(verdict), p.gap_ok)
                st.pending = None
        if st.last_ms is not None and ts_ms <= st.last_ms:
            return out + self._release(serial, st.last_ms)  # late: the silence logic runs on the newest device time only
        if st.subscribed_at is not None and arrival - st.subscribed_at >= CONFIRM_S:
            st.confirmed = True
        if st.last_ms is not None and ts_ms - st.last_ms >= SILENCE_MS:
            start = st.last_ms + SLOT_MS
            gap_ok = st.confirmed and not st.restored
            verdict = restarted(high, low, st.last_high, st.last_low)
            if verdict:
                self._saw_restart(serial, ts_ms)  # evidence for the cross-site check, watched or not
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
        return out + self._release(serial, ts_ms)

    def _saw_restart(self, serial: str, end_ms: int) -> None:
        self.restarts.append((serial, end_ms))
        newest = max(t for _, t in self.restarts)
        self.restarts = [(s, t) for s, t in self.restarts if t >= newest - RESTARTS_KEPT_MS]

    def _release(self, serial: str | None, now_ms: int | None) -> list[Silence]:
        """Decide the held cuts of `serial` that are due at its device time `now_ms` (None, None: every one)."""
        out: list[Silence] = []
        keep: list[_Held] = []
        for h in self.held:
            if serial is not None and (h.silence.serial != serial or now_ms is None or now_ms < h.release_ms):
                keep.append(h)
                continue
            st = self._sensor(h.silence.serial)
            site = self.site_of(h.silence.serial)
            together = [s for s, t in self.restarts if s != h.silence.serial and self.site_of(s) != site
                        and abs(t - h.silence.end_ms) <= CORRELATE_MS]
            if together:
                st.found["unattributed"] += 1
                log.info("%s: restarted after %.0f s without samples, as did %s at another site within a minute: "
                         "upstream (the Ting cloud), not a power cut", h.silence.serial,
                         (h.silence.end_ms - h.silence.start_ms) / 1000, ", ".join(sorted(set(together))))
                continue
            st.found["cut"] += 1
            log.info("%s: inferred power cut of %.0f s (Hi/Lo collapsed: the sensor restarted)", h.silence.serial,
                     (h.silence.end_ms - h.silence.start_ms) / 1000)
            out.append(h.silence)
        self.held = keep
        return out

    def finish(self) -> list[Silence]:
        """Shutdown or the end of a replay: decide every held cut with what is known now."""
        return self._release(None, None)

    def _decide(self, serial: str, st: _Sensor, start: int, end: int, collapsed: bool, gap_ok: bool) -> list[Silence]:
        kind = "cut" if collapsed else ("gap" if gap_ok else None)
        if kind is None:
            st.found["unattributed"] += 1
            return []
        if end - start > MAX_DRAW_MS:
            log.warning("%s: a %s of %.1f days is not drawn (longer than %d days)", serial, kind,
                        (end - start) / 86_400_000, MAX_DRAW_MS // 86_400_000)
            return []
        if kind == "cut":  # decided once another site's restart could have shown up (HOLD_MS)
            self.held.append(_Held(Silence(serial, kind, start, end), end + HOLD_MS))
            return []
        st.found[kind] += 1
        log.info("%s: inferred data gap of %.0f s (Hi/Lo not collapsed)", serial, (end - start) / 1000)
        return [Silence(serial, kind, start, end)]

    # ---- state across restarts ------------------------------------------------

    def state(self) -> dict[str, Any]:
        out: dict[str, Any] = {s: {"last_ms": st.last_ms, "watched": st.watched, "high": st.last_high, "low": st.last_low}
                               for s, st in self.sensors.items() if st.last_ms is not None}
        if self.held:
            out["_held"] = [[h.silence.serial, h.silence.start_ms, h.silence.end_ms, h.release_ms] for h in self.held]
        if self.restarts:
            out["_restarts"] = [[s, t] for s, t in self.restarts]
        return out

    def load(self, state: dict[str, Any], down_seconds: float | None) -> None:
        for entry in state.get("_held", []) if isinstance(state.get("_held"), list) else []:
            try:
                serial, start, end, release = entry
                self.held.append(_Held(Silence(str(serial), "cut", int(start), int(end)), int(release)))
            except (TypeError, ValueError):
                continue
        for entry in state.get("_restarts", []) if isinstance(state.get("_restarts"), list) else []:
            try:
                serial, end = entry
                self.restarts.append((str(serial), int(end)))
            except (TypeError, ValueError):
                continue
        for serial, entry in state.items():
            if serial.startswith("_") or not isinstance(entry, dict) or not isinstance(entry.get("last_ms"), int):
                continue
            st = self._sensor(str(serial))
            st.last_ms, st.watched = entry["last_ms"], bool(entry.get("watched"))
            high, low = entry.get("high"), entry.get("low")
            if isinstance(high, (int, float)) and isinstance(low, (int, float)):
                st.last_high, st.last_low = float(high), float(low)
        self.restart(down_seconds)
