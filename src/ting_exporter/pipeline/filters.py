"""Storage rules, rounding and dedup: the steps between a reading and a stored sample."""

from __future__ import annotations

from collections import deque

from ..signals import Storage

DEDUP_HORIZON_MS = 600_000
DEDUP_MAX_KEYS = 200_000


def round_value(value: float, decimals: int) -> float:
    if decimals <= 0:
        return float(round(value))
    return round(value, decimals)


class StorageFilter:
    """Applies each signal's storage rule per (serial, metric), in device time.

    on_change: store when the rounded value differs from the last stored one, or
    at least `seconds` have passed since it (heartbeat). A late sample (older
    than the last stored one, interleaved delivery) is stored when its value
    differs, but it never becomes "the last stored": neither its time nor its
    value moves the state, so the heartbeat runs on and the next in-order sample
    is compared with the newest value (design 5.2).
    """

    def __init__(self) -> None:
        self._last: dict[tuple[str, str], tuple[int, float]] = {}

    def admit(self, serial: str, metric: str, storage: Storage, ts_ms: int, value: float) -> bool:
        if storage.kind == "every":
            return True
        key = (serial, metric)
        last = self._last.get(key)
        if last is None:
            self._last[key] = (ts_ms, value)
            return True
        last_ms, last_value = last
        if ts_ms < last_ms:
            return value != last_value
        if value != last_value or ts_ms - last_ms >= storage.seconds * 1000:
            self._last[key] = (ts_ms, value)
            return True
        return False


class Dedup:
    """Drops exact repeats of (serial, metric, ts_ms) within a 10 min device-time horizon.

    A reconnect's catch-up or a replayed file can repeat samples. VictoriaMetrics
    would collapse them too (-dedup.minScrapeInterval=1ms); dropping them here
    keeps the push volume and the counters honest. Keys are evicted per sensor,
    by that sensor's own newest device time, so a sensor that has gone silent
    keeps only its last 10 minutes and never holds up another's eviction.
    """

    def __init__(self, horizon_ms: int = DEDUP_HORIZON_MS, max_keys: int = DEDUP_MAX_KEYS) -> None:
        self.horizon_ms = horizon_ms
        self.max_keys = max_keys
        self._seen: set[tuple[str, str, int]] = set()
        self._order: dict[str, deque[tuple[str, str, int]]] = {}  # per sensor, in arrival order
        self._newest: dict[str, int] = {}
        self.dropped = 0

    def seen(self, serial: str, metric: str, ts_ms: int) -> bool:
        key = (serial, metric, ts_ms)
        if key in self._seen:
            self.dropped += 1
            return True
        self._seen.add(key)
        self._order.setdefault(serial, deque()).append(key)
        self._newest[serial] = max(self._newest.get(serial, ts_ms), ts_ms)
        self._evict(serial)
        return False

    def _evict(self, serial: str) -> None:
        order, oldest_kept = self._order[serial], self._newest[serial] - self.horizon_ms
        while order and order[0][2] < oldest_kept:
            self._seen.discard(order.popleft())
        while len(self._seen) > self.max_keys:  # the cap across sensors: the longest gives way
            self._seen.discard(max(self._order.values(), key=len).popleft())

    def keys_of(self, serial: str) -> int:
        return len(self._order.get(serial, ()))

    def __len__(self) -> int:
        return len(self._seen)
