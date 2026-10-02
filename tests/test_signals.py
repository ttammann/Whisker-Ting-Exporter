"""The signal registry is the single source: rules, reference script and docs must agree with it."""

import re
from pathlib import Path

import pytest

from ting_exporter import rules, signals
from ting_exporter.replay import _decimals
from ting_exporter.signals import Rollup, Signal

ROOT = Path(__file__).parent.parent
ROLLUP_FILE = ROOT / "deploy" / "vmalert" / "ting-rollups.yml"


def test_registry_is_consistent():
    signals.validate()


def test_checked_in_rollup_rules_match_the_registry():
    assert ROLLUP_FILE.read_text() == rules.render(), (
        "deploy/vmalert/ting-rollups.yml is out of date: run `ting-exporter rules > deploy/vmalert/ting-rollups.yml`"
    )


def test_rollup_rules_parse_and_cover_every_rollup():
    yaml = pytest.importorskip("yaml")
    doc = yaml.safe_load(ROLLUP_FILE.read_text())
    [group] = doc["groups"]
    assert group["interval"] == "1m" and group["eval_delay"] == "60s"
    records = {r["record"]: r["expr"] for r in group["rules"]}
    expected = {s.record_name(r) for s in signals.REGISTRY for r in s.rollups}
    assert set(records) == expected
    for s in signals.REGISTRY:
        for r in s.rollups:
            assert f"{r.agg}_over_time({s.metric}[1m])" in records[s.record_name(r)]


def test_reference_script_restates_the_registry():
    """tools/reference_rollups.py must describe the same signals (it may not import the package)."""
    text = (ROOT / "tools" / "reference_rollups.py").read_text()
    for s in signals.REGISTRY:
        rule = "every" if s.storage.kind == "every" else "change"
        assert re.search(rf'"{s.field}": \("{s.metric}", {s.decimals}, "{rule}"\)', text), s.key
        if s.rollups:
            aggs = ", ".join(f'("{r.agg}", {_decimals(r.round_to) if r.round_to else None})' for r in s.rollups)
            assert f'"{s.metric}": [{aggs}]' in text, s.key


def test_readme_lists_every_metric():
    readme = (ROOT / "README.md").read_text()
    for s in signals.REGISTRY:
        assert f"`{s.metric}`" in readme, s.metric


@pytest.mark.parametrize(
    "bad",
    [
        Signal("x", signals.CATEGORICAL, "x", "ting_x", "1", "", 0, required=True),
        Signal("x", signals.COMBO, "x", "x_metric", "1", "", 0),
        Signal("x", signals.COMBO, "x", "ting_x", "1", "", 0, rollups=(Rollup("avg"),)),
        Signal("x", signals.COMBO, "x", "ting_x", "1", "", 0, storage=signals.on_change(60), rollups=(Rollup("count"),)),
    ],
)
def test_validate_rejects_mistakes(bad):
    with pytest.raises(ValueError):
        signals.validate((*signals.REGISTRY, bad))


def test_rollup_names_follow_the_convention():
    s = signals.PRIMARY
    assert s.record_name(Rollup("avg", 0.0001)) == "ting:voltage_volts:avg_1m"
    assert rules.expression(s, Rollup("avg", 0.00001)) == "round(avg_over_time(ting_voltage_volts[1m]), 0.00001)"
    assert rules.expression(s, Rollup("min"), 'serial="X"') == 'min_over_time(ting_voltage_volts{serial="X"}[1m])'


def test_derived_series_are_not_stream_signals():
    for s in signals.REGISTRY:
        assert s.metric not in signals.DERIVED_METRICS
