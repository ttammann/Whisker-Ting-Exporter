"""`compare-stores`: the Ting data in two VictoriaMetrics, samples per series name, as of one moment (design 7.7).

    ting-exporter compare-stores 192.0.2.10 198.51.100.10
    ting-exporter compare-stores http://a:8428 http://b:8428 --lookback 30d --at 2026-04-01T12:00:00Z

ALERTS and the exporter's self-metrics are not compared: each store's vmalert and scrapes concern its own host.

Counts every pushed series (raw samples, notifications, outages: no `job` label) and every rollup
(`ting:*`) over the lookback window ending at --at (default: 10 minutes ago, so samples still in
flight do not count; an --at without an offset is UTC). It counts a week per query: one count over
months of a 4 Hz series passes VictoriaMetrics' -search.maxSamplesPerSeries. Prints one row per name
with both counts and marks differences. Exit status 1 if a raw series differs (rollups may differ by a
few points where one vmalert started or stopped), 2 if a store could not answer.
Standard library only (no exporter state), so it also runs as `python -m ting_exporter.compare`.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

PUSHED = '{__name__=~"ting_.*",job=""}'
ROLLUPS = '{__name__=~"ting:.*"}'
CHUNK_S = 7 * 86400  # a week of a 4 Hz series is 2.4 M samples, far below -search.maxSamplesPerSeries (30 M)
UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 7 * 86400}


def base_url(host: str) -> str:
    if "://" not in host:
        host = f"http://{host}" + ("" if ":" in host else ":8428")
    return host.rstrip("/")


def seconds(duration: str) -> int:
    """'120d', '6h', '2w' ... as seconds."""
    number, unit = duration[:-1], duration[-1:]
    if unit not in UNITS or not number.isdigit():
        raise argparse.ArgumentTypeError(f"{duration!r}: a whole number with s, m, h, d or w")
    return int(number) * UNITS[unit]


def parse_at(text: str) -> int:
    """An ISO time; without an offset it is UTC, as every time option of ting-exporter."""
    when = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return int((when if when.tzinfo else when.replace(tzinfo=timezone.utc)).timestamp())


def counts(base: str, selector: str, lookback_s: int, at: int) -> dict[str, float]:
    """Samples per name in (at - lookback_s, at], counted a week (CHUNK_S) per query and added up."""
    out: dict[str, float] = {}
    end, remaining = at, lookback_s
    while remaining > 0:
        window = min(CHUNK_S, remaining)
        query = f"sum by (__name__) (count_over_time({selector}[{window}s]) keep_metric_names)"
        url = f"{base}/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": end})
        with urllib.request.urlopen(url, timeout=120) as resp:
            body = json.load(resp)
        for r in body["data"]["result"]:
            name = r["metric"].get("__name__", "")
            out[name] = out.get(name, 0.0) + float(r["value"][1])
        end, remaining = end - window, remaining - window
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    add_arguments(parser)
    return run(parser.parse_args(argv))


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("a", nargs="?", help="first VictoriaMetrics (host, host:port or URL; default: the first push target)")
    parser.add_argument("b", nargs="?", help="second VictoriaMetrics (default: the second push target)")
    parser.add_argument("--lookback", default="120d", type=seconds_arg, help="window to count over (default 120d)")
    parser.add_argument("--at", help="end of the window, ISO time, UTC without an offset (default: 10 minutes ago)")


def seconds_arg(text: str) -> str:
    seconds(text)
    return text


def run(args: argparse.Namespace) -> int:
    lookback_s = seconds(args.lookback)
    at = parse_at(args.at) if args.at else int(time.time()) - 600
    a, b = base_url(args.a), base_url(args.b)
    print(f"samples per name over {args.lookback} up to {datetime.fromtimestamp(at, timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
    print(f"{'':44} {args.a:>14} {args.b:>14}")
    raw_differs = False
    for title, selector in (("pushed", PUSHED), ("rollups", ROLLUPS)):
        try:
            left, right = counts(a, selector, lookback_s, at), counts(b, selector, lookback_s, at)
        except urllib.error.HTTPError as err:
            print(f"{err.filename}: HTTP {err.code}: {err.read().decode('utf-8', 'replace')[:500]}", file=sys.stderr)
            return 2
        except (urllib.error.URLError, OSError, ValueError, KeyError) as err:
            print(f"cannot query the stores: {err}", file=sys.stderr)
            return 2
        print(f"-- {title}")
        for name in sorted(left.keys() | right.keys()):
            x, y = left.get(name, 0.0), right.get(name, 0.0)
            mark = "" if x == y else f"  <- differs by {y - x:+.0f}"
            raw_differs |= title == "pushed" and x != y
            print(f"{name:44} {x:>14.0f} {y:>14.0f}{mark}")
    print("pushed series: " + ("DIFFER" if raw_differs else "identical"))
    return 1 if raw_differs else 0


if __name__ == "__main__":
    sys.exit(main())
