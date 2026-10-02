"""From hub invocation to samples: decode, stamp, storage rule, round, dedup, inferred cuts (design 7.1).

`serve` and `replay` feed the same Pipeline, so a replayed record file produces
exactly the samples the live exporter pushed (design 4.1, principle 7). The
pipeline is synchronous and does no I/O; it keeps per-sensor counters, which
the self-metrics export and `replay` prints. Session events (connects, ends,
refusals) go in through `event`, live or from the flight recorder, because the
inferred cuts depend on whether this exporter was listening (pipeline/cuts.py).
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from .. import signals
from . import decode, timing
from .cuts import SilenceTracker
from .filters import Dedup, StorageFilter, round_value
from .model import Sample, format_value, label_text

log = logging.getLogger(__name__)

UNKNOWN_SITE = "unknown"
FALLBACK_LOG_INTERVAL = 60.0

_COMBO = signals.COMBO_TARGET.casefold()
_CATEGORICAL = signals.CATEGORICAL_TARGET.casefold()
_HIGH = next(s for s in signals.COMBO_SIGNALS if s.field == signals.HIGH_FIELD)
_LOW = next(s for s in signals.COMBO_SIGNALS if s.field == signals.LOW_FIELD)


@dataclass
class SensorStats:
    serial: str
    site: str
    labels: str
    timing: timing.Timing = field(default_factory=timing.Timing)
    hub_messages: Counter[str] = field(default_factory=Counter)  # by target
    received: Counter[str] = field(default_factory=Counter)  # by metric, after decode
    emitted: Counter[str] = field(default_factory=Counter)  # by metric, after storage rule and dedup
    discarded: Counter[str] = field(default_factory=Counter)  # by reason
    duplicates: int = 0
    last_fallback_log: float = float("-inf")


class Pipeline:
    def __init__(self, sites: dict[str, str] | None = None, timestamp_source: str = "device") -> None:
        if timestamp_source not in ("device", "arrival"):
            raise ValueError("timestamp_source must be device or arrival")
        self.sites = dict(sites or {})
        self.timestamp_source = timestamp_source
        self.sensors: dict[str, SensorStats] = {}
        self.storage = StorageFilter()
        self.dedup = Dedup()
        self.silences = SilenceTracker()
        self._arrival_ms: dict[tuple[str, str], int] = {}  # newest arrival stamp per (serial, metric)

    def site(self, serial: str) -> str:
        return self.sites.get(serial, UNKNOWN_SITE)

    def sensor(self, serial: str) -> SensorStats:
        stats = self.sensors.get(serial)
        if stats is None:
            site = self.site(serial)
            if site == UNKNOWN_SITE:
                log.warning("%s has no site in TING_SITES; labelling it site=%s", serial, UNKNOWN_SITE)
            stats = self.sensors[serial] = SensorStats(serial, site, label_text({"serial": serial, "site": site}))
        return stats

    def event(self, serial: str, kind: str, fields: dict[str, Any], t: float) -> None:
        """A hub session event: connecting, subscribed, ended (with a reason), refused, auth_wait, ..."""
        self.silences.event(serial, kind, fields, t)

    def process(self, serial: str, target: str, args: list[Any], arrival: float) -> tuple[list[Sample], bool]:
        """Samples from one invocation, and whether it carried the primary signal (voltage)."""
        st = self.sensor(serial)
        st.hub_messages[target] += 1
        name = target.casefold()
        try:
            if name == _COMBO:
                readings, soft = decode.combo(args)
            elif name == _CATEGORICAL:
                readings, soft = decode.categorical(args)
            else:
                return [], False  # updateGraphMulti (a duplicate) and unknown targets: counted above only
        except decode.Discarded as err:
            st.discarded[err.reason] += 1
            return [], False
        for reason in soft:
            st.discarded[reason] += 1

        samples: list[Sample] = []
        primary_ms: int | None = None
        for reading in readings:
            sig = reading.signal
            st.received[sig.metric] += 1
            ts_ms, delay, fell_back = timing.stamp(reading.device_ms, arrival, self.timestamp_source)
            if fell_back or self.timestamp_source == "arrival":
                ts_ms = self._after_last_arrival(serial, sig.metric, ts_ms)
            if fell_back:
                st.timing.fallbacks += 1
                if arrival - st.last_fallback_log >= FALLBACK_LOG_INTERVAL:
                    st.last_fallback_log = arrival
                    log.warning("%s: device timestamp off by %.1f s from arrival; stamping arrival - %.2f s",
                                serial, delay, timing.FALLBACK_OFFSET)
            if sig.primary:
                primary_ms = ts_ms
                timing.track_primary(st.timing, ts_ms, arrival, delay)
            value = round_value(reading.value, sig.decimals)
            if not self.storage.admit(serial, sig.metric, sig.storage, ts_ms, value):
                continue
            if self.dedup.seen(serial, sig.metric, ts_ms):
                st.duplicates += 1
                continue
            st.emitted[sig.metric] += 1
            samples.append(Sample(sig.metric, serial, st.labels, ts_ms, value, format_value(value, sig.decimals)))
        if primary_ms is not None:
            values = {r.signal.key: r.value for r in readings}
            for silence in self.silences.primary(serial, primary_ms, arrival, values.get(_HIGH.key), values.get(_LOW.key)):
                for ts, value in silence.points():
                    st.emitted[silence.metric] += 1
                    samples.append(Sample(silence.metric, serial, st.labels, ts, float(value), str(value)))
        return samples, primary_ms is not None

    def _after_last_arrival(self, serial: str, metric: str, ts_ms: int) -> int:
        """An arrival stamp is this host's clock, not the sensor's: two frames can arrive in the same millisecond,
        and they are two readings, not a repeat for Dedup to drop. Keep each series' arrival stamps increasing."""
        key = (serial, metric)
        last = self._arrival_ms.get(key)
        if last is not None and ts_ms <= last:
            ts_ms = last + 1
        self._arrival_ms[key] = ts_ms
        return ts_ms
