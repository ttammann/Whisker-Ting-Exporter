"""Timestamp policy: device time, guarded by arrival time.

The hub delivers each sample 0.5-13 s after its device timestamp, over a live
(~0.55 s) or a buffered (4-8 s, paced at 4 Hz) path, switching hundreds of
times a day (design 2.5). The device timeline stays continuous across
those switches, so the device timestamp (DataTimeUtc / ObsTime) is the sample
time, and arrival time only guards it:

    delay = arrival - device
    -2 s <= delay <= 300 s   timestamp = device time (ms)
    otherwise                timestamp = arrival - 0.55 s, counted as a fallback

TING_TIMESTAMP_SOURCE=arrival stamps every sample with its arrival time.

For the primary signal (voltage) this also tracks, per sensor, the newest
device time, late samples (older than the newest, e.g. interleaved catch-up),
an upper bound on missing 0.25 s slots, and the delay distribution (exported
as ting_clock_offset_seconds and ting_delivery_delay_seconds).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..stats import Hist

GUARD_MIN, GUARD_MAX = -2.0, 300.0
FALLBACK_OFFSET = 0.55  # the live path's delivery delay (design 2.5)
SLOT_MS = 250
GAP_THRESHOLD_MS = 375
DELAY_BUCKETS = (0.25, 0.5, 1, 2, 4, 6, 8, 10, 15, 30, 60, 300)


@dataclass
class Timing:
    """Per-sensor timing state and counters (primary signal only, except fallbacks)."""

    max_device_ms: int | None = None
    last_receive: float | None = None  # wall clock of the newest primary sample
    last_offset: float | None = None  # arrival - device of the newest primary sample
    late: int = 0
    gap_slots: int = 0
    fallbacks: int = 0
    delay: Hist = field(default_factory=lambda: Hist(DELAY_BUCKETS))


def stamp(device_ms: int, arrival: float, source: str = "device") -> tuple[int, float, bool]:
    """(timestamp ms, delay s, fell back?) for one reading."""
    delay = arrival - device_ms / 1000
    if source == "arrival":
        return round(arrival * 1000), delay, False
    if GUARD_MIN <= delay <= GUARD_MAX:
        return device_ms, delay, False
    return round((arrival - FALLBACK_OFFSET) * 1000), delay, True


def track_primary(state: Timing, ts_ms: int, arrival: float, delay: float) -> None:
    """Late / gap bookkeeping and delay statistics for a primary sample."""
    state.last_receive = arrival
    state.last_offset = delay
    state.delay.observe(delay)
    if state.max_device_ms is None:
        state.max_device_ms = ts_ms
        return
    gap = ts_ms - state.max_device_ms
    if gap < 0:
        state.late += 1
    elif gap > GAP_THRESHOLD_MS:
        state.gap_slots += round(gap / SLOT_MS) - 1
    if gap > 0:
        state.max_device_ms = ts_ms
