"""deploy/grafana/ting-dashboard.json: generated from tools/make_dashboard.py, and it only queries series that exist."""

import importlib.util
import json
import re
from pathlib import Path

from ting_exporter import signals

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD = ROOT / "deploy" / "grafana" / "ting-dashboard.json"


def _generator():
    spec = importlib.util.spec_from_file_location("make_dashboard", ROOT / "tools" / "make_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.configure(module.EXAMPLE_SITES)
    return module


def _exprs(dashboard):
    out = []

    def walk(panels):
        for p in panels:
            out.extend(t["expr"] for t in p.get("targets", []))
            walk(p.get("panels", []))

    walk(dashboard["panels"])
    out.extend(a["expr"] for a in dashboard["annotations"]["list"] if "expr" in a)
    return out


def test_checked_in_dashboard_matches_the_generator():
    assert json.loads(DASHBOARD.read_text()) == _generator().build()


def test_dashboard_queries_only_existing_rollups_and_metrics():
    rollups = {s.record_name(r) for s in signals.REGISTRY for r in s.rollups}
    raw = {s.metric for s in signals.REGISTRY}
    exporter = set(re.findall(r'"(ting_[a-z_]+)"', (ROOT / "src" / "ting_exporter" / "selfmetrics.py").read_text()))
    exporter |= {f"{m}_total" for m in exporter}  # counters gain _total on exposition
    from ting_exporter import rest
    exporter |= set(signals.DERIVED_METRICS) | rest.REST_METRICS
    text = "\n".join(_exprs(json.loads(DASHBOARD.read_text())))
    for name in set(re.findall(r"\bting:[a-z_]+:[a-z]+_1m\b", text)):
        assert name in rollups, f"{name} is not a rollup rule"
    for name in set(re.findall(r"\bting_[a-z_]+\b", text)):
        assert name in raw | exporter, f"{name} is neither a signal nor an exporter metric"


def test_markers_are_bands_in_each_sites_own_panels_and_follow_the_dropdown():
    """Grafana draws a dashboard annotation on every panel (repeated panels share one id), so a site's markers
    are queries of its own repeated panels on $site, drawn as bars on a hidden axis, and gated by the dropdown."""
    gen = _generator()
    dashboard = json.loads(DASHBOARD.read_text())

    def walk(panels):
        for p in panels:
            yield p
            yield from walk(p.get("panels", []))

    per_site = [p for p in walk(dashboard["panels"]) if p.get("repeat") == "site"]
    assert {p["title"] for p in per_site} == {"Voltage · site $site", "Frequency · site $site", "Raw voltage · site $site"}
    for panel in per_site:
        marks = [t for t in panel["targets"] if t["refId"].startswith("M")]
        assert len(marks) == len(gen.MARKERS) - 1  # every kind but "none"
        for t in marks:
            assert 'site="$site"' in t["expr"] and "${markers:regex}" in t["expr"]
            assert "offset -$__interval" not in t["expr"]  # VictoriaMetrics carries sparse series forward with it
        for o in (o for o in panel["fieldConfig"]["overrides"] if o["matcher"]["id"] == "byFrameRefID"):
            props = {x["id"]: x["value"] for x in o["properties"]}
            assert props["custom.axisPlacement"] == "hidden" and props["min"] == 0
            if props["custom.drawStyle"] == "bars":  # a band: exactly its window, full height
                assert props["custom.barAlignment"] == -1 and props["max"] == 1
            else:  # a dot just below the top edge
                assert props["custom.drawStyle"] == "points" and props["max"] > 1
    [key] = [p for p in dashboard["panels"] if p["type"] == "text"]
    for kind, meaning in gen.MARKER_MEANING.items():  # the colour key names every marker in its colour
        assert gen.MARKERS[kind][1] in key["options"]["content"] and meaning in key["options"]["content"]
    assert set(gen.MARKER_MEANING) == set(gen.MARKERS) - {"none"}
    variables = {v["name"]: v for v in dashboard["templating"]["list"]}
    assert variables["site"]["includeAll"] and variables["site"]["multi"]
    markers = variables["markers"]
    assert markers["options"][0]["value"] == "none"  # Grafana falls back to the first option when none is selected
    assert markers["current"]["value"] == list(gen.DEFAULT_MARKERS) and "gaps" not in gen.DEFAULT_MARKERS
    notes, *rest = dashboard["annotations"]["list"]
    assert notes["builtIn"] == 1 and notes["hide"] is False and notes["name"] == "Notes"
    assert all('site=""' in a["expr"] for a in rest)  # only alerts that belong to no site stay dashboard-wide


def test_the_site_list_keeps_a_site_that_has_gone_dark():
    """Rollups stop with the data, and VictoriaMetrics answers label_values from its per-day index, so a site
    24-48 h into an outage dropped out of the list. ting_stream_up is scraped every 30 s for every streamed
    sensor, dark or not; the rollups still cover imported history from before the exporter ran."""
    [site] = [v for v in json.loads(DASHBOARD.read_text())["templating"]["list"] if v["name"] == "site"]
    assert site["definition"] == site["query"]["query"] == 'label_values({__name__=~"ting_stream_up|ting:voltage_volts:avg_1m"}, site)'


def test_alert_markers_and_descriptions_fit_two_stores():
    dashboard = json.loads(DASHBOARD.read_text())
    [note] = [a for a in dashboard["annotations"]["list"] if a.get("name") == "exporter alerts (no site)"]
    assert "Vmalert.*" in note["expr"]  # VmalertDown has no site either
    assert "same data" not in dashboard["description"]  # self-metrics and ALERTS are each store's own
    health = next(p for p in dashboard["panels"] if p.get("title") == "Exporter and storage")["panels"]
    for panel in health:
        if any("ting_voltage_rolling" in t["expr"] for t in panel["targets"]):
            continue  # pushed data: the same in both stores
        assert "own exporter" in panel["description"], panel["title"]


def _panels(panels):
    for panel in panels:
        yield panel
        yield from _panels(panel.get("panels", []))


def test_marker_queries_never_run_below_one_minute():
    """the marker expressions read 1-minute series; at the raw panel's 250 ms step they found a sample on one
    step in N and drew hairline stripes instead of bands."""
    markers = [t for p in _panels(json.loads(DASHBOARD.read_text())["panels"]) for t in p.get("targets", [])
               if t["refId"].startswith("M")]
    assert markers and all(t.get("interval") == "1m" for t in markers)


def test_the_gaps_marker_sees_minutes_that_are_missing_entirely():
    """M12: a missing minute has no count_1m point, so only checking for short minutes and 5 silent minutes at
    each step missed short gaps and gaps between two steps. The marker now counts the minutes in each window."""
    [expr] = {t["expr"] for p in _panels(json.loads(DASHBOARD.read_text())["panels"]) for t in p.get("targets", [])
              if t.get("legendFormat") == "Data gap"}
    assert "($__interval_ms / 60000) - max by (site) (count_over_time(ting:voltage_volts:count_1m" in expr
    assert "vector(time()) < (${__to} / 1000 - 150)" in expr  # the newest minutes, not rolled up yet, are no gap


def test_coverage_ends_where_the_rollups_do():
    """M13: a minute's rollup is written ~82 s after it ends (eval_delay plus vmalert's phase), so ranges up to now
    read degraded with perfect data (98.3 % on "Last 1 hour", 93.3 % on "Last 15 minutes")."""
    coverage = [p for p in _panels(json.loads(DASHBOARD.read_text())["panels"]) if p.get("title", "").startswith("Coverage")]
    assert len(coverage) == 2
    for panel in coverage:
        assert panel["timeShift"] == "2m" and "2 minutes ago" in panel["description"], panel["title"]


def test_no_data_is_readable_on_every_stat_tile():
    """a transparent "No data" is right where the tile colours its background, invisible where it colours
    the text (Coverage in range showed a blank tile)."""
    for panel in _panels(json.loads(DASHBOARD.read_text())["panels"]):
        if panel.get("type") != "stat":
            continue
        for mapping in panel["fieldConfig"]["defaults"].get("mappings", []):
            if mapping["type"] == "special" and mapping["options"]["match"] == "null":
                color = mapping["options"]["result"]["color"]
                assert panel["options"]["colorMode"] == "background" or color != "transparent", panel["title"]


def test_outages_need_no_look_ahead_subquery():
    """Design 5.4: the exporter infers cuts, so the dashboard reads ting_power_cut and needs no 12 h look-ahead
    (and VictoriaMetrics no -search.maxPointsSubqueryPerTimeseries)."""
    text = "\n".join(_exprs(json.loads(DASHBOARD.read_text())))
    assert "ting_power_cut" in text and "ting_stream_gap" in text and "offset -" not in text
    assert "maxPointsSubquery" not in (ROOT / "deploy" / "compose.yml").read_text()


def test_context_and_cloud_rows():
    rows = {p["title"]: p for p in json.loads(DASHBOARD.read_text())["panels"] if p["type"] == "row"}
    context = rows["Power source (context marks)"]
    assert rows["Ting cloud status"]["collapsed"] and context["collapsed"]
    exprs = [t["expr"] for p in context["panels"] for t in p["targets"]]
    assert all('last_over_time(ting_context{context="source"' in e for e in exprs)


def test_sites_come_from_a_site_file(tmp_path, capsys):
    site_file = tmp_path / "site.toml"
    site_file.write_text('[dashboard]\ndescription = "Two homes"\n[sites.north]\nname = "North"\ncolour = "#3987e5"\n'
                         'grid = "Eastern"\nhifi = "about 10"\n')
    gen = _generator()
    gen.main(["--sites-file", str(site_file)])
    built = json.loads(capsys.readouterr().out)
    assert built["description"] == "Two homes"
    text = json.dumps(built)
    assert "^north.*" in text and "north (North): Eastern" in text and "north about 10" in text
