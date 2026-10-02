"""Independent reference for the 1-minute rollups: raw capture in, rollup values out.

    python tools/reference_rollups.py [--sites TNG000001=a,...] FILE.jsonl.gz ...

This script deliberately does not import ting_exporter. It restates the storage
rules of the design (device timestamps, rounding, change-or-60 s filter with
late samples that never become "the last stored", dedup) and the rollup rules
in the simplest possible code, so that `ting-exporter replay
--dry-run` can be checked against it (tests/test_golden.py).

Output: CSV `window_end,serial,site,record,value`, one row per rollup series and
minute, windows (T-60 s, T] stamped with their end T. Pushed-sample counts go to
stderr.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MINUTE_MS = 60_000
HEARTBEAT_MS = 60_000

# source -> (metric, decimals, rule); rule "every" or "change"
COMBO = {
    "Voltage": ("ting_voltage_volts", 3, "every"),
    "AveragePeaksMax": ("ting_hifi", 0, "every"),
    "VoltageHi": ("ting_voltage_rolling_high_volts", 3, "change"),
    "VoltageLo": ("ting_voltage_rolling_low_volts", 3, "change"),
}
CATEGORIES = {
    "frequency": ("ting_frequency_hertz", 4, "every"),
    "thdAvg": ("ting_thd_ratio", 5, "change"),
    "thdMin": ("ting_thd_min_ratio", 5, "change"),
    "thdMax": ("ting_thd_max_ratio", 5, "change"),
}
# metric -> [(aggregation, decimals of the avg or None)]
ROLLUPS = {
    "ting_voltage_volts": [("min", None), ("max", None), ("avg", 4), ("count", None)],
    "ting_frequency_hertz": [("min", None), ("max", None), ("avg", 5)],
    "ting_hifi": [("min", None), ("max", None), ("avg", 2)],
    "ting_thd_ratio": [("avg", 5)],
    "ting_thd_min_ratio": [("min", None)],
    "ting_thd_max_ratio": [("max", None)],
}
DECIMALS = {metric: dp for metric, dp, _ in [*COMBO.values(), *CATEGORIES.values()]}


def device_ms(text: str) -> int:
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    delta = parsed - EPOCH
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return (micros + 500) // 1000


def stamp(arrival: float, text: str) -> int:
    ms = device_ms(text)
    delay = arrival - ms / 1000
    if -2 <= delay <= 300:
        return ms
    return round((arrival - 0.55) * 1000)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sites", default="")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()
    sites = dict(pair.split("=", 1) for pair in args.sites.split(",") if pair)

    stored: dict[tuple[str, str], dict[int, float]] = defaultdict(dict)  # (serial, metric) -> ts -> value
    last: dict[tuple[str, str], tuple[int, float]] = {}
    pushed: Counter[tuple[str, str]] = Counter()

    def offer(serial: str, metric: str, dp: int, rule: str, ts: int, raw: object) -> None:
        value = round(float(raw), dp)
        if dp == 0:
            value = float(int(value))
        key = (serial, metric)
        remember = rule == "change"
        if rule == "change" and key in last:
            last_ts, last_value = last[key]
            if ts < last_ts:  # late: stored if it differs, but the newest stays "the last stored"
                if value == last_value:
                    return
                remember = False
            elif value == last_value and ts - last_ts < HEARTBEAT_MS:
                return
        if ts in stored[key]:  # duplicate device timestamp
            return
        if remember:
            last[key] = (ts, value)
        stored[key][ts] = value
        pushed[key] += 1

    for path in args.files:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                row = json.loads(line)
                if row.get("kind") != "invocation":
                    continue
                serial, target, payload = row["serial"], row["target"], row["args"]
                if target == "updateComboBinaryData":
                    data = payload[0]
                    if "msgpack" in data:
                        data = data["msgpack"]
                    ts = stamp(row["t"], data["DataTimeUtc"])
                    for field, (metric, dp, rule) in COMBO.items():
                        if field in data:
                            offer(serial, metric, dp, rule, ts, data[field])
                elif target == "updateGraphMultiCategorical":
                    for record in payload[0]:
                        spec = CATEGORIES.get(record["Category"])
                        if spec:
                            offer(serial, spec[0], spec[1], spec[2], stamp(row["t"], record["ObsTime"]), record["Value"])

    out = []
    for (serial, metric), samples in stored.items():
        windows: dict[int, list[float]] = defaultdict(list)
        for ts in sorted(samples):
            windows[-(-ts // MINUTE_MS) * MINUTE_MS].append(samples[ts])
        for end, values in windows.items():
            when = datetime.fromtimestamp(end / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            for agg, avg_dp in ROLLUPS.get(metric, []):
                if agg == "count":
                    text = str(len(values))
                elif agg == "avg":  # float64 like VictoriaMetrics: plain sum in time order; ties may differ by 1 ulp of the step
                    total = 0.0
                    for v in values:
                        total += v
                    step = float(f"1e-{avg_dp}")
                    mean = total / len(values) + step / 2
                    text = f"{mean - math.fmod(mean, step):.{avg_dp}f}"
                else:
                    text = f"{(min if agg == 'min' else max)(values):.{DECIMALS[metric]}f}"
                record = f"ting:{metric.removeprefix('ting_')}:{agg}_1m"
                out.append((when, serial, sites.get(serial, "unknown"), record, text))
    for row in sorted(out):
        print(",".join(row))
    for (serial, metric), n in sorted(pushed.items()):
        print(f"pushed {serial} {metric} {n}", file=sys.stderr)


if __name__ == "__main__":
    main()
