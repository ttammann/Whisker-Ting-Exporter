"""Golden replay: the fixture recordings through the serve pipeline must give the reference rollups.

tests/fixtures/golden-rollups.csv comes from tools/reference_rollups.py, which
does not import ting_exporter (regenerate: see tools/make_fixtures.py and the
README). The spot values are the complete minutes of the two spot slices.
"""

import io
import subprocess
import sys
from pathlib import Path

import pytest

from ting_exporter.pipeline import Pipeline
from ting_exporter.replay import replay

from .fakes import CAPTURE, FIXTURES, SERIAL_A, SERIAL_B

SITES = {SERIAL_A: "a", SERIAL_B: "b"}
ROOT = Path(__file__).parent.parent


@pytest.fixture(scope="module")
def replayed():
    import asyncio

    pipeline = Pipeline(SITES)
    out = io.StringIO()
    totals = asyncio.run(replay(CAPTURE, pipeline, dry_run=True, out=out))
    return pipeline, out.getvalue(), totals


def test_rollups_equal_the_independent_reference(replayed):
    _, csv, _ = replayed
    assert csv == (FIXTURES / "golden-rollups.csv").read_text()


def test_emitted_counts_equal_the_reference(replayed):
    pipeline, _, _ = replayed
    expected = {}
    for line in (FIXTURES / "golden-pushed.txt").read_text().splitlines():
        serial, metric, n = line.split()
        expected[(serial, metric)] = int(n)
    got = {(serial, metric): n for serial, st in pipeline.sensors.items() for metric, n in st.emitted.items()}
    assert got == expected


def test_reference_script_still_produces_the_golden_file():
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "reference_rollups.py"), "--sites", "TNG000001=a,TNG000002=b", *map(str, CAPTURE)],
        capture_output=True, text=True, check=True,
    )
    assert result.stdout == (FIXTURES / "golden-rollups.csv").read_text()


@pytest.mark.parametrize(
    ("end", "serial", "count", "vmin", "vmax", "avg"),
    [
        ("2026-03-10T20:18:00Z", SERIAL_A, "240", "122.574", "123.264", "122.9046"),
        ("2026-03-10T20:18:00Z", SERIAL_B, "240", "121.013", "121.764", "121.4199"),
        ("2026-03-11T06:08:00Z", SERIAL_A, "240", "122.110", "122.672", "122.3971"),
        ("2026-03-11T06:08:00Z", SERIAL_B, "240", "120.592", "121.180", "120.9188"),
    ],
)
def test_spot_values(replayed, end, serial, count, vmin, vmax, avg):
    _, csv, _ = replayed
    rows = {tuple(r.split(",")[:4]): r.split(",")[4] for r in csv.splitlines()}
    site = SITES[serial]
    assert rows[(end, serial, site, "ting:voltage_volts:count_1m")] == count
    assert rows[(end, serial, site, "ting:voltage_volts:min_1m")] == vmin
    assert rows[(end, serial, site, "ting:voltage_volts:max_1m")] == vmax
    assert rows[(end, serial, site, "ting:voltage_volts:avg_1m")] == avg


def test_capture_properties(replayed):
    """What the slices were made for: catch-up and reordering, THD repeated on the live path, reconnect, overlap."""
    pipeline, csv, totals = replayed
    one, f8 = pipeline.sensors[SERIAL_A], pipeline.sensors[SERIAL_B]
    assert one.timing.late > 0 and f8.timing.late > 0  # out-of-order samples by device time
    assert f8.received["ting_thd_ratio"] > 10 * f8.emitted["ting_thd_ratio"]  # 4 Hz THD repeats, stored on change
    assert f8.timing.gap_slots >= 140  # the 36 s reconnect gap (upper bound)
    counts = [int(r.split(",")[4]) for r in csv.splitlines() if r.split(",")[3] == "ting:voltage_volts:count_1m"]
    assert max(counts) > 240  # the overlap episode: a second 4 Hz series
    assert one.timing.fallbacks == f8.timing.fallbacks == 0
    assert not one.discarded and not f8.discarded
    assert one.duplicates == f8.duplicates == 0
    assert one.hub_messages["updateGraphMulti"] > 0  # counted, not discarded
    assert totals["invocations"] > 20_000
