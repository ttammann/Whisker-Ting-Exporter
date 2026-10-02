"""Write the checked-in test fixtures: synthetic flight-recorder files of two Ting sensors.

    python tools/make_fixtures.py tests/fixtures/capture

Nothing here comes from a real installation. Two simulated sensors (serials
TNG000001 and TNG000002, sites a and b) produce 4 Hz voltage, Hi-Fi and
frequency from a seeded random walk, and THD triplets that change about every
30 s. Their delivery follows what the hub is known to do (design 2.4-2.6):

- every sample is three invocations: updateGraphMulti (an exact duplicate of
  the frequency, as a .NET time string), updateGraphMultiCategorical
  (frequency) and updateComboBinaryData (a named map);
- the live path delivers about 0.55 s after the device time, with the last THD
  triplet repeated at 4 Hz; the buffered path 4-8 s late, paced at 4 Hz, with
  THD only when it changes; a switch from buffered to live reorders samples;
- a subscription first gets about 6 s of catch-up;
- the session events (connecting, subscribed, ended, backoff) of a recorder
  run with TING_STALE_SECONDS=30.

The slices are what the tests need (SLICES). The same seed writes the same
bytes; the golden files are derived from these files (README, Development).
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

SEED = 3_0_0
A, B = "TNG000001", "TNG000002"
ELEMENTS = ["ComboBinaryData", "frequency", "thdMin", "thdAvg", "thdMax"]
STEP = 0.25
LIVE = 0.55
STALE = 30.0
STALE_RETRY = 5.0
CATCH_UP = 6.0


def _t(text: str) -> float:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp()


def iso(t: float) -> str:
    """DataTimeUtc / ObsTime as the record file holds them (microseconds, +00:00)."""
    return datetime.fromtimestamp(round(t, 6), timezone.utc).isoformat(timespec="microseconds")


def dotnet(t: float) -> str:
    """updateGraphMulti's .NET time string: 7 fractional digits and Z."""
    return datetime.fromtimestamp(round(t, 6), timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "0Z"


@dataclass
class Sensor:
    serial: str
    phase: float  # seconds after the whole second of the first sample
    base: float  # volts
    hifi: float
    hi: float  # the sensor's own long-window high and low (VoltageHi/VoltageLo)
    lo: float
    thd: float  # thdAvg ratio
    rng: random.Random
    track: bool = False  # the window follows every sample (after a restart, around events)
    noise: float = 0.0
    freq: float = 0.0
    thd_next: float = 0.0
    thd_values: tuple[float, float, float] = (0.0, 0.0, 0.0)
    extra: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.new_thd(0.0)

    def new_thd(self, t: float) -> None:
        self.thd = max(0.005, self.thd + self.rng.gauss(0, 0.0012))
        spread = abs(self.rng.gauss(0.0009, 0.0003))
        self.thd_values = (self.thd - spread, self.thd, self.thd + spread * 1.2)
        self.thd_next = t + 30.0 + self.rng.uniform(-0.05, 0.05)

    def reading(self, t: float, volts: float | None = None) -> dict:
        self.noise = 0.9 * self.noise + self.rng.gauss(0, 0.05)
        v = volts if volts is not None else self.base + 0.6 * math.sin(2 * math.pi * t / 900.0) + self.noise \
            + self.rng.gauss(0, 0.03)
        self.freq = 0.98 * self.freq + self.rng.gauss(0, 0.0018)
        if self.track:
            self.hi, self.lo = max(self.hi, v), min(self.lo, v)
        return {"DataTimeUtc": iso(t), "Voltage": v, "AveragePeaksMax": float(max(1, round(self.rng.gauss(self.hifi, self.hifi / 4)))),
                "VoltageHi": self.hi, "VoltageLo": self.lo, "_freq": 60.0 + self.freq + self.rng.uniform(-2e-6, 2e-6)}


class Recording:
    """Rows of one slice; written sorted by receive time ("t")."""

    def __init__(self) -> None:
        self.rows: list[tuple[float, int, dict]] = []

    def add(self, t: float, row: dict) -> None:
        self.rows.append((t, len(self.rows), {"t": round(t, 7), **row}))

    def event(self, t: float, serial: str, event: str, **fields) -> None:
        self.add(t, {"serial": serial, "kind": "event", "event": event, **fields})

    def subscribe(self, t: float, serial: str) -> float:
        self.event(t, serial, "connecting")
        self.event(t + 0.52, serial, "subscribed", elements=ELEMENTS)
        return t + 0.52

    def sample(self, s: Sensor, dev: float, arrival: float, *, live: bool, multi: bool = True, volts: float | None = None,
               freq: float | None = None, thd: float | None = None) -> None:
        r = s.reading(dev, volts)
        f = freq if freq is not None else r.pop("_freq")
        r.pop("_freq", None)
        if multi:
            self.add(arrival, {"serial": s.serial, "kind": "invocation", "target": "updateGraphMulti",
                               "args": [[f"{dotnet(dev)}|{f!r}"]]})
        self.add(arrival + 0.00012, {"serial": s.serial, "kind": "invocation", "target": "updateGraphMultiCategorical",
                                     "args": [[{"Category": "frequency", "ObsTime": iso(dev), "Value": repr(f)}]]})
        self.add(arrival + 0.00025, {"serial": s.serial, "kind": "invocation", "target": "updateComboBinaryData",
                                     "args": [r]})
        changed = dev >= s.thd_next
        if changed:
            s.new_thd(dev)
        if thd is not None:
            s.thd_values = (thd - 0.004, thd, thd + 0.006)
        if live or changed or thd is not None:
            values = s.thd_values
            if multi:
                self.add(arrival + 0.005, {"serial": s.serial, "kind": "invocation", "target": "updateGraphMulti",
                                           "args": [[f"{dotnet(dev)}|{v!r}" for v in values]]})
            self.add(arrival + 0.0051, {"serial": s.serial, "kind": "invocation", "target": "updateGraphMultiCategorical",
                                        "args": [[{"Category": c, "ObsTime": iso(dev), "Value": repr(v)}
                                                  for c, v in zip(("thdMin", "thdAvg", "thdMax"), values)]]})

    def write(self, path: Path) -> int:
        self.rows.sort(key=lambda r: (r[0], r[1]))
        with open(path, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=9, mtime=0, filename="") as dst:
            for _t_, _i, row in self.rows:
                dst.write((json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n").encode())
        return len(self.rows)


def device_times(s: Sensor, first: float, last: float) -> list[float]:
    """4 Hz device times in [first, last): the sensor's own grid, a few microseconds of jitter."""
    k0 = math.ceil((first - s.phase) / STEP)
    out = []
    k = k0
    while (t := s.phase + k * STEP) < last:
        out.append(t + s.rng.uniform(-0.00004, 0.00004))
        k += 1
    return out


def stream(rec: Recording, s: Sensor, first: float, last: float, delay, *, subscribed: float | None = None,
           multi: bool = True, window: tuple[float, float] = (-math.inf, math.inf), **values) -> None:
    """Samples with device time in [first, last), received at device + delay(device) (seconds, live if < 1).
    After a subscription at `subscribed`, the catch-up (device time within CATCH_UP before it) arrives at once.
    Only samples received inside `window` are recorded (a slice is cut by receive time)."""
    burst = 0
    for dev in device_times(s, first, last):
        lag = delay(dev)
        arrival = dev + lag + s.rng.gauss(0, 0.012 if lag < 1 else 0.0015)
        if subscribed is not None and dev < subscribed:
            if dev < subscribed - CATCH_UP:
                continue
            arrival = subscribed + 0.08 + 0.0021 * burst
            burst += 1
        if window[0] <= arrival <= window[1]:
            rec.sample(s, dev, arrival, live=lag < 1, multi=multi, **values)
        else:
            s.reading(dev)  # the sensor goes on; the slice did not record it


def sensors(rng: random.Random) -> tuple[Sensor, Sensor]:
    return (Sensor(A, 0.0827, 122.4, 7.0, 125.8312, 117.2074, 0.041, random.Random(rng.random())),
            Sensor(B, 0.1563, 120.9, 24.0, 124.6181, 118.0436, 0.052, random.Random(rng.random())))


def switching(*segments: tuple[float, float]):
    """delay(device time) from (from_time, delay) segments, in order."""
    def delay(t: float) -> float:
        lag = segments[0][1]
        for start, d in segments:
            if t >= start:
                lag = d
        return lag
    return delay


# ---- the slices ------------------------------------------------------------------------------------------------


def a_subscribe(rec: Recording, a: Sensor, b: Sensor) -> None:
    """The recorder starts, both sensors subscribe and get ~6 s of catch-up; A switches from live to the buffered
    path and back (the switch back reorders samples), B stays live with THD repeated at 4 Hz."""
    t0, end = _t("2026-03-10T20:00:00"), _t("2026-03-10T20:04:30")
    rec.add(t0, {"kind": "start", "version": "3.0.0", "host": "example-host", "command": "record", "seconds": 86400.0,
                 "stale": STALE})
    sa, sb = rec.subscribe(t0 + 0.0011, A), rec.subscribe(t0 + 0.0009, B) + 0.08
    stream(rec, a, t0 - CATCH_UP, end, switching((0, LIVE), (t0 + 62.3, 5.8), (t0 + 212.4, LIVE)), subscribed=sa,
           window=(t0, end))
    stream(rec, b, t0 - CATCH_UP, end, switching((0, LIVE), (t0 + 140.1, 4.6), (t0 + 151.0, LIVE)), subscribed=sb,
           window=(t0, end))


def spot(rec: Recording, a: Sensor, b: Sensor, first: str, last: str) -> None:
    """Two complete minutes for both sensors (the spot values, design T7): A buffered, B live."""
    t0, end = _t(first), _t(last)
    stream(rec, a, t0 - 10, end, switching((0, 6.2)), window=(t0, end))
    stream(rec, b, t0 - 10, end, switching((0, LIVE)), window=(t0, end))


def c_reconnect(rec: Recording, a: Sensor, b: Sensor) -> None:
    """B is silent for 36 s: the recorder's session ends stale after 30 s and subscribes again; A carries on."""
    t0, end = _t("2026-03-10T20:27:40"), _t("2026-03-10T20:29:30")
    stream(rec, a, t0 - 10, end, switching((0, 6.2)), window=(t0, end))
    dark, back = t0 + 31.2, t0 + 67.2
    stream(rec, b, t0 - 10, dark, switching((0, LIVE)), window=(t0, end))
    ended = dark + LIVE + STALE
    rec.event(ended, B, "ended", reason="stale", error=f"no voltage for {STALE:.0f} s")
    rec.event(ended + 0.001, B, "backoff", seconds=5.1, session_seconds=1803.4)
    sub = rec.subscribe(ended + 5.1, B)
    stream(rec, b, back, end, switching((0, LIVE)), subscribed=sub, window=(t0, end))


def e_overlap(rec: Recording, a: Sensor, b: Sensor) -> None:
    """A's long-window high changes at hh:00:08, and A carries a second, phase-shifted 4 Hz series for 5 s."""
    t0, end = _t("2026-03-11T08:00:00"), _t("2026-03-11T08:01:30")
    change = _t("2026-03-11T08:00:08")
    stream(rec, a, t0 - 10, change, switching((0, LIVE)), window=(t0, end))
    a.hi = 126.0723
    stream(rec, a, change, end, switching((0, LIVE)), window=(t0, end))
    stream(rec, b, t0 - 10, end, switching((0, 6.0)), window=(t0, end))
    shadow = Sensor(A, a.phase + 0.125, a.base + 0.4, a.hifi, a.hi, a.lo, a.thd, random.Random(7))
    for dev in device_times(shadow, _t("2026-03-11T08:00:43"), _t("2026-03-11T08:00:48")):
        rec.sample(shadow, dev, dev + LIVE + 0.03, live=False)


def f_cut(rec: Recording, b: Sensor) -> None:
    """Sensor B alone (no updateGraphMulti):
    - 3 min 20 s dark, watched, and a subscription stayed empty: a data gap (no restart);
    - a brownout (55-58 V for 3 min, THD 0.15) and its recovery spike (128.6 V, 60.4 Hz), which widen the window;
    - 52 min dark, then a restart: Hi and Lo within 0.3 V and the window shrank: a power cut;
    - 5 min dark after a socket error (not watched): not inferred."""
    t0, end = _t("2026-03-11T15:30:00"), _t("2026-03-11T16:50:00")
    b.track = True
    live = switching((0, LIVE))
    windows = (t0, end)

    def dark(last: float, back: float, *, socket_error: bool = False) -> float:
        """The recorder's sessions while the sensor is dark from `last` (its last sample's device time) to `back`;
        returns when the subscription that receives `back` started."""
        t = last + LIVE + (8.0 if socket_error else STALE)
        if socket_error:
            rec.event(t, B, "ended", reason="ws_error", error="ClientConnectionResetError: Connection reset by peer")
        else:
            rec.event(t, B, "ended", reason="stale", error=f"no voltage for {STALE:.0f} s")
        while True:
            rec.event(t + 0.001, B, "backoff", seconds=STALE_RETRY, session_seconds=STALE)
            sub = rec.subscribe(t + STALE_RETRY, B)
            if sub + STALE + LIVE > back:
                return sub
            t = sub + STALE + LIVE
            rec.event(t, B, "ended", reason="stale", error=f"no voltage for {STALE:.0f} s")

    # normal, then the gap
    gap_from, gap_to = t0 + 101.3, t0 + 301.3
    stream(rec, b, t0 - 10, gap_from, live, multi=False, window=windows)
    sub = dark(gap_from, gap_to)
    # back, normal, the brownout, the spike, normal
    brown, spike, calm = gap_to + 95.0, gap_to + 275.0, gap_to + 278.0
    stream(rec, b, gap_to, brown, live, subscribed=sub, multi=False, window=windows)
    for dev in device_times(b, brown, spike):
        sag = 55.0 + 3.0 * b.rng.random()
        rec.sample(b, dev, dev + live(dev) + b.rng.gauss(0, 0.012), live=True, multi=False, volts=sag, thd=0.15)
    for dev in device_times(b, spike, calm):
        rec.sample(b, dev, dev + live(dev) + b.rng.gauss(0, 0.012), live=True, multi=False,
                   volts=128.6 - (dev - spike) * 2.4, freq=60.4 - (dev - spike) * 0.13)
    b.new_thd(calm)
    cut_from = calm + 120.0
    stream(rec, b, calm, cut_from, live, multi=False, window=windows)
    # the cut: dark, then a restart with a fresh, narrow window
    cut_to = cut_from + 52 * 60 + 11.1
    sub = dark(cut_from, cut_to)
    restart_v = b.base + 0.2
    b.hi, b.lo = restart_v + 0.15, restart_v - 0.15
    after = cut_to + 30.0
    stream(rec, b, cut_to, after, live, subscribed=sub, multi=False, window=windows)
    # dark after a socket error, back with the window only widened
    back = after + 5 * 60 + 2.4
    sub = dark(after, back, socket_error=True)
    stream(rec, b, back, end, live, subscribed=sub, multi=False, window=windows)


SLICES = [  # name: what it covers
    ("a-subscribe", "start record, both subscribes with ~6 s catch-up, A live -> buffered -> live (reordered), "
                    "B live with THD at 4 Hz, a short buffered stretch on B"),
    ("b-spot-1", "complete windows (20:17, 20:18] for both sensors (spot values, design T7)"),
    ("c-reconnect", "B 36 s silent, stale reconnect (ended/backoff/connecting/subscribed events), catch-up"),
    ("d-spot-2", "complete windows (06:07, 06:08] for both sensors (spot values, design T7)"),
    ("e-overlap", "A VoltageHi change at 08:00:08 and a 5 s overlap episode (a second 4 Hz series)"),
    ("f-cut", "B only, no updateGraphMulti: a data gap, a brownout, a power cut with a restart, an unwatched silence"),
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("out", type=Path)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    a, b = sensors(rng)
    makers = {
        "a-subscribe": lambda rec: a_subscribe(rec, a, b),
        "b-spot-1": lambda rec: spot(rec, a, b, "2026-03-10T20:16:30", "2026-03-10T20:18:40"),
        "c-reconnect": lambda rec: c_reconnect(rec, a, b),
        "d-spot-2": lambda rec: spot(rec, a, b, "2026-03-11T06:06:40", "2026-03-11T06:08:40"),
        "e-overlap": lambda rec: e_overlap(rec, a, b),
        "f-cut": lambda rec: f_cut(rec, b),
    }
    for name, _ in SLICES:
        rec = Recording()
        makers[name](rec)
        print(f"{name}: {rec.write(args.out / f'{name}.jsonl.gz')} lines")


if __name__ == "__main__":
    main()
