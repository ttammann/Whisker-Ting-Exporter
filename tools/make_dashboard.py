"""Generate the Grafana dashboard (classic Grafana JSON, imports on any version) (design 11).

    python tools/make_dashboard.py > deploy/grafana/ting-dashboard.json          the example sites a and b
    python tools/make_dashboard.py --sites-file overlay/site.toml > ...          the real ones (tools/overlay.py render)

site.toml:
    [dashboard]
    description = "..."                       optional
    [sites.a]
    name = "Example A"                        shown in panel descriptions
    colour = "#3987e5"                        categorical slots: blue, orange, aqua, yellow
    grid = "Eastern Interconnection"          optional, for the frequency panel
    hifi = "about 10"                         optional, the site's normal Hi-Fi level

Dashboards read the 1-minute rollups (ting:*:*_1m) by default, so a month over the
site tunnel is ~43 k points per series; the collapsed "Raw 4 Hz" row reads the raw
series for zooms of a few hours. tests/test_dashboard.py fails if a query names a
rollup or metric that the signal registry or the exporter does not produce.

Colour follows the site, never its position: SITES fixes each site's hue
(categorical slots 1-3, validated for Grafana's dark theme). A new sensor's site
takes the next slot here, and gets its own voltage, frequency and raw panels.

Markers belong to a site, so they are not dashboard annotations (Grafana draws
those on every panel, and repeated panels share one id, so they cannot be told
apart): each site's voltage, frequency and raw panel draws its own markers as
full-height bands, from queries on that panel's $site. The Markers dropdown
picks the kinds: each marker query carries a gate that is empty unless its kind
is selected. "None" comes first because Grafana falls back to the first option
when nothing is selected. Grafana annotations are left for your own Notes.
"""

from __future__ import annotations

import argparse
import json
import sys
import tomllib

DS = {"type": "prometheus", "uid": "${ds}"}
EXAMPLE_SITES = {"a": {"name": "Example A", "colour": "#3987e5"}, "b": {"name": "Example B", "colour": "#d95926"}}
SITES: dict[str, tuple[str, str]] = {}  # site label -> (name, colour), set by configure()
SITE_INFO: dict[str, dict] = {}
DESCRIPTION = ("Whisker Labs Ting sensors via two ting-exporters (active-active) and a VictoriaMetrics at each site. "
               "Both stores hold the same sensor data, notifications, inferred cuts and context marks; exporter "
               "health, storage and alerts are each store's own host.")


def configure(sites: dict[str, dict], description: str | None = None) -> None:
    global DESCRIPTION
    SITES.clear()
    SITE_INFO.clear()
    for site, info in sites.items():
        SITES[str(site)] = (str(info["name"]), str(info["colour"]))
        SITE_INFO[str(site)] = dict(info)
    if description:
        DESCRIPTION = description
SEL = 'site=~"$site"'
# The Site list. Not from the rollups alone: they stop with the data, and VictoriaMetrics answers label_values
# from its per-day index, so a site 24-48 h into an outage dropped out of the list (and its panels with it).
# ting_stream_up is scraped every 30 s for every streamed sensor, dark or not; the rollups still cover imported
# history from before the exporter ran.
SITE_QUERY = 'label_values({__name__=~"ting_stream_up|ting:voltage_volts:avg_1m"}, site)'
MARKERS = {  # dropdown value -> (label, colour, style); DEFAULT_MARKERS are selected on load
    # "band": shades the panel's height while it lasts; "dot": a dot along the top edge, for events that last
    # minutes and would be a pixel wide as a band over days. Colours stay clear of the site colours and of each
    # other (an outage band can follow right after a power event dot).
    "none": ("None", "", ""),
    "outages": ("Outages", "#e34948", "band"),
    "notifications": ("Ting notifications", "#73bf69", "dot"),
    "power_events": ("Power events", "#f2cc0c", "dot"),
    "restarts": ("Sensor restarts", "#9085e9", "dot"),
    "gaps": ("Data gaps", "#8e8e8e", "band"),
    "alerts": ("Exporter alerts", "#e87ba4", "dot"),
    "context": ("Context marks", "#5794f2", "dot"),
}
MARKER_MEANING = {  # the colour key under the stats
    "outages": "outage: Ting reported it, or inferred (no data, then a sensor restart)",
    "gaps": "data gap",
    "power_events": "power event: below 110 V",
    "notifications": "other Ting notification (brownout, surge, ...)",
    "restarts": "sensor restarted",
    "alerts": "exporter alert",
    "context": "context mark (e.g. power source changed)",
}
DEFAULT_MARKERS = ("outages", "notifications", "power_events")
VOLT_STEPS = [
    {"color": "red", "value": None},
    {"color": "orange", "value": 110},
    {"color": "green", "value": 114},
    {"color": "red", "value": 126},
]

POWER_EVENT_VOLTS = 110  # ANSI C84.1 Range B lower limit; a minute whose lowest reading is below is marked
# A Ting is powered from the outlet it measures, so a power cut at its own site shows no low reading at all:
# it goes dark and restarts. A restart resets the sensor's own high/low window (VoltageHi/Lo, normally ~24 h
# wide and 4-10 V apart) to the current voltage, so the two sit within a volt or two of each other.
RESTART_SPREAD_VOLTS = 2.5
GAP_MIN_SAMPLES = 180  # a minute with fewer of its 240 samples (>= 15 s missing) is marked as a data gap
# A minute's rollup is written ~82 s after the minute (vmalert's eval_delay 60 s plus its phase): panels that
# divide by the time they cover end this long ago, or the unwritten newest minutes read as missing data.
ROLLUPS_SETTLED = "2m"
CONTEXT_LOOKBACK = "5y"  # a context mark is in force until the next one: look back to it

_ids = iter(range(1, 1000))
# "No data" in plain text: a value scale whose lowest step is red must not make a missing value look like an alarm
NO_DATA = [{"type": "special", "options": {"match": "null", "result": {"text": "No data", "color": "transparent"}}}]
# the same on a tile that colours its text rather than its background: transparent text would be invisible
NO_DATA_TEXT = [{"type": "special", "options": {"match": "null", "result": {"text": "No data", "color": "text"}}}]


def target(expr: str, legend: str = "", ref: str = "A", instant: bool = False, interval: str | None = None) -> dict:
    t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref, "editorMode": "code",
         "instant": instant, "range": not instant}
    if interval:
        t["interval"] = interval
    return t


def site_colours(suffixes: tuple[str, ...] = ("",)) -> list[dict]:
    """Overrides that give every series of a site its site colour (legends start with the site)."""
    return [
        {"matcher": {"id": "byRegexp", "options": f"^{site}{s}.*"},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": colour}}]}
        for site, (_, colour) in SITES.items() for s in suffixes
    ]


def band(site: str) -> list[dict]:
    """Shade between a site's min and max; min and max thin, avg the solid line."""
    return [
        {"matcher": {"id": "byName", "options": f"{site} max"},
         "properties": [{"id": "custom.fillBelowTo", "value": f"{site} min"}, {"id": "custom.fillOpacity", "value": 18},
                        {"id": "custom.lineWidth", "value": 1}]},
        {"matcher": {"id": "byName", "options": f"{site} min"}, "properties": [{"id": "custom.lineWidth", "value": 1}]},
    ]


def settled(panel: dict) -> dict:
    """End the panel's range ROLLUPS_SETTLED ago (Grafana's per-panel time shift)."""
    panel["timeShift"] = ROLLUPS_SETTLED
    panel["description"] += f" Ends {ROLLUPS_SETTLED.removesuffix('m')} minutes ago: the newest minutes are not rolled up yet."
    return panel


def stat(title: str, x: int, w: int, targets: list[dict], unit: str, desc: str, steps=None, decimals=None,
         mappings=None, color_mode: str = "value") -> dict:
    defaults: dict = {"unit": unit, "color": {"mode": "thresholds"},
                      "thresholds": {"mode": "absolute", "steps": steps or [{"color": "text", "value": None}]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    if mappings:
        defaults["mappings"] = mappings
    return {
        "id": next(_ids), "type": "stat", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": 0, "w": w, "h": 5}, "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": color_mode, "graphMode": "none", "justifyMode": "auto", "orientation": "horizontal",
                    "textMode": "value_and_name", "wideLayout": True, "showPercentChange": False},
    }


def ts(title: str, y: int, targets: list[dict], unit: str, desc: str, *, x: int = 0, w: int = 24, h: int = 8,
       custom=None, overrides=None, defaults=None, interval: str = "1m", repeat: bool = False) -> dict:
    c = {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 0, "showPoints": "never", "spanNulls": False,
         "thresholdsStyle": {"mode": "off"}}
    c.update(custom or {})
    d = {"unit": unit, "custom": c, "color": {"mode": "palette-classic"}}
    d.update(defaults or {})
    p = {
        "id": next(_ids), "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets, "interval": interval,
        "fieldConfig": {"defaults": d, "overrides": overrides or []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "none"}},
    }
    if repeat:
        p.update(repeat="site", repeatDirection="h", maxPerRow=3)
    return p


def marker_key(y: int) -> dict:
    """A transparent strip with every marker's colour, shape and meaning (bands as squares, dots as dots)."""
    items = []
    for kind, meaning in MARKER_MEANING.items():
        _, colour, style = MARKERS[kind]
        glyph = "■" if style == "band" else "●"
        items.append(f'<span style="color:{colour}">{glyph}</span> {meaning}')
    return {"id": next(_ids), "type": "text", "title": "", "transparent": True,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 2},
            "options": {"mode": "markdown", "content": "**Markers** (dropdown above, in each site's panels): &nbsp; "
                        + " &nbsp;·&nbsp; ".join(items) + " &nbsp;·&nbsp; hover a band or dot for its name"}}


def row(title: str, y: int, panels: list[dict] | None = None) -> dict:
    """A row; with `panels` it is collapsed and holds them."""
    r = {"id": next(_ids), "type": "row", "title": title, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
         "collapsed": panels is not None, "panels": panels or []}
    return r


def min_avg_max(prefix: str, sel: str) -> list[dict]:
    return [
        target(f"min_over_time({prefix}:min_1m{{{sel}}}[$__interval])", "{{site}} min", "A"),
        target(f"avg_over_time({prefix}:avg_1m{{{sel}}}[$__interval])", "{{site}} avg", "B"),
        target(f"max_over_time({prefix}:max_1m{{{sel}}}[$__interval])", "{{site}} max", "C"),
    ]


def gate(marker: str) -> str:
    """Empty unless `marker` is selected in the Markers dropdown (Grafana fills in the regex of the selection)."""
    return f'label_match(label_set(vector(1), "marker", "{marker}"), "marker", "${{markers:regex}}")'


def markers(sel: str) -> tuple[list[dict], list[dict]]:
    """Targets and overrides that draw the site's markers as bands in a per-site panel.

    Each marker is 1 for every window of $__interval that holds the event, so none falls between two plotted
    points when the range grows; every marker query has a 1-minute minimum step (the raw panel's is 250 ms). A band is a bar that ends at its point and is one step wide, i.e. exactly its
    window, on a hidden 0-1 axis, so it fills the panel's height; consecutive windows join into one band. A dot
    sits at the same value on a 0-1.06 axis, just below the top edge. (No copy shifted by a negative offset:
    VictoriaMetrics then carries sparse series forward for days.) In the tooltip as "<kind> ●" where one is
    active (Grafana leaves out series without a value at the hovered time); the legend names the kind.
    """
    count = f"ting:voltage_volts:count_1m{{{sel}}}"
    outage = f"max by (site) (max_over_time(ting_power_outage{{{sel}}}[$__interval]))"
    spread = (f"(max by (site) (ting_voltage_rolling_high_volts{{{sel}}}) - "
              f"min by (site) (ting_voltage_rolling_low_volts{{{sel}}}))")
    cut = f"max by (site) (max_over_time(ting_power_cut{{{sel}}}[$__interval]))"
    exprs = {
        # Outages as Ting reports them (the phone app's alerts; value = kind), and the cuts the exporter inferred
        # where Ting reported nothing: a silence that ended in a sensor restart (pipeline/cuts.py).
        "outages": (f'label_set({outage} == 2, "kind", "Outage (community, Ting)") or '
                    f'label_set({outage} == 1, "kind", "Outage (Ting)") or on (site) '
                    f'label_set({cut} == 1, "kind", "Outage (inferred: no data, then a restart)")',
                    "{{kind}}"),
        # Every other Ting notification (brownout, surge, hazard, ...).
        "notifications": (f'count by (site, title) (count_over_time(ting_notification{{{sel}, '
                          f'type!~"PowerOutage|CommunityPowerOutage|.*Restored"}}[$__interval])) > 0',
                          "{{title}} (Ting)"),
        # The lowest 0.25 s reading of a minute below 110 V, also while data keeps flowing (a brownout).
        "power_events": (f"min by (site) (min_over_time(ting:voltage_volts:min_1m{{{sel}}}[$__interval])) < {POWER_EVENT_VOLTS}",
                         f"Below {POWER_EVENT_VOLTS} V"),
        # The sensor's own high and low collapsed onto the current voltage: it restarted. Every minute of the
        # window is checked, since the collapse lasts only a minute or two once the voltage moves again.
        "restarts": (f"min_over_time({spread}[$__interval:1m]) < {RESTART_SPREAD_VOLTS}", "Sensor restarted"),
        # A window with a short minute (>= 15 s of its samples missing), with fewer minutes than it spans (a missing
        # minute has no point, so it can only be counted), or with none at all from a site that delivered within a
        # day. The newest minutes have no rollup yet (vmalert's eval_delay), so they are not a gap.
        "gaps": (f"((max by (site) (max_over_time(ting_stream_gap{{{sel}}}[$__interval])) == 1) or "
                 f"((min by (site) (min_over_time({count}[$__interval])) < {GAP_MIN_SAMPLES}) or "
                 f"((($__interval_ms / 60000) - max by (site) (count_over_time({count}[$__interval]))) >= 1) or "
                 f"(max by (site) (last_over_time({count}[1d])) unless max by (site) (count_over_time({count}[$__interval])))) "
                 f"and on () (vector(time()) < (${{__to}} / 1000 - 150)))",
                 "Data gap"),
        "alerts": (f'max by (site, alertname) (max_over_time(ALERTS{{alertstate="firing", alertname=~"Ting.*", {sel}}}[$__interval]))',
                   "{{alertname}}"),
        # A context mark (`ting-exporter mark`): its change points, 1 where a value starts and 0 where it ends.
        "context": (f'count by (site, context, value) (count_over_time(ting_context{{{sel}}}[$__interval])) > 0',
                    "{{context}}={{value}}"),
    }
    targets, overrides = [], []
    for i, (kind, (expr, legend)) in enumerate(exprs.items()):
        ref = f"M{i + 1}"
        gated = f"((({expr}) * 0 + 1) and on () {gate(kind)})"  # 1 while the event lasts, if its kind is selected
        targets.append(target(gated, legend, ref, interval="1m"))  # never a step under the 1-minute series they read
        _, colour, style = MARKERS[kind]
        if style == "band":
            look = [{"id": "max", "value": 1}, {"id": "custom.drawStyle", "value": "bars"},
                    {"id": "custom.barAlignment", "value": -1}, {"id": "custom.barWidthFactor", "value": 1},
                    {"id": "custom.lineWidth", "value": 0}, {"id": "custom.fillOpacity", "value": 22},
                    {"id": "custom.showPoints", "value": "never"}]
        else:
            look = [{"id": "max", "value": 1.06}, {"id": "custom.drawStyle", "value": "points"},
                    {"id": "custom.pointSize", "value": 11}, {"id": "custom.showPoints", "value": "always"}]
        overrides.append({"matcher": {"id": "byFrameRefID", "options": ref}, "properties": [
            {"id": "unit", "value": "none"}, {"id": "min", "value": 0}, *look,
            {"id": "custom.axisPlacement", "value": "hidden"}, {"id": "custom.spanNulls", "value": False},
            {"id": "custom.thresholdsStyle", "value": {"mode": "off"}},
            {"id": "custom.hideFrom", "value": {"legend": False, "tooltip": False, "viz": False}},
            {"id": "mappings", "value": [{"type": "value", "options": {"1": {"text": "●", "color": colour}}}]},
            {"id": "color", "value": {"mode": "fixed", "fixedColor": colour}},
        ]})
    return targets, overrides


def build() -> dict:
    status = [{"type": "value", "options": {"0": {"text": "Offline", "color": "red"}, "1": {"text": "Online", "color": "green"}}},
              {"type": "special", "options": {"match": "null", "result": {"text": "No data", "color": "red"}}}]
    panels = [
        stat("Stream", 0, 4, [target(f"max by (site) (ting_stream_up{{{SEL}}})", "{{site}}", instant=True)], "none",
             "1 while voltage samples arrive within the stale limit (60 s). Offline = the sensor, its site's power or "
             "network, or the Ting cloud.", steps=[{"color": "red", "value": None}, {"color": "green", "value": 1}],
             mappings=status, color_mode="background"),
        stat("Voltage now", 4, 4, [target(f"max by (site) (ting_voltage_volts{{{SEL}}})", "{{site}}", instant=True)], "volt",
             "Latest RMS reading. ANSI C84.1 Range A 114-126 V: green inside, orange 110-114, red outside.",
             steps=VOLT_STEPS, decimals=1, mappings=NO_DATA, color_mode="background"),
        stat("Voltage in range", 8, 4, [
            target(f"min by (site) (min_over_time(ting:voltage_volts:min_1m{{{SEL}}}[$__range]))", "{{site}} min", "A", True),
            target(f"max by (site) (max_over_time(ting:voltage_volts:max_1m{{{SEL}}}[$__range]))", "{{site}} max", "B", True)],
            "volt", "Lowest and highest 0.25 s reading in the selected range (from the 1-minute rollups).",
            steps=VOLT_STEPS, decimals=1, mappings=NO_DATA, color_mode="background"),
        stat("THD now", 12, 4, [target(f"max by (site) (ting_thd_ratio{{{SEL}}})", "{{site}}", instant=True)], "percentunit",
             "Total harmonic distortion of the voltage, latest (updates about every 30 s).", decimals=1),
        settled(stat("Coverage in range", 16, 4, [target(
            f"100 * sum by (site) (sum_over_time(ting:voltage_volts:count_1m{{{SEL}}}[$__range])) / ($__range_s * 4)",
            "{{site}}", instant=True)], "percent",
            "Share of the expected 4 samples per second that arrived. Gaps are outages at the site, in its network or in the cloud.",
            steps=[{"color": "red", "value": None}, {"color": "orange", "value": 95}, {"color": "green", "value": 99}], decimals=1,
            mappings=NO_DATA_TEXT)),
        stat("Delivery delay", 20, 4, [target(f"max by (site) (ting_clock_offset_seconds{{{SEL}}})", "{{site}}", instant=True)], "s",
             "Arrival minus the sensor's own timestamp, newest sample: ~0.55 s on the cloud's live path, 4-8 s buffered. "
             "Samples are stored at the sensor's time either way.", decimals=1),
    ]
    panels.append(marker_key(5))
    y = 7
    mark_targets, mark_overrides = markers('site="$site"')
    panels.append(ts("Voltage · site $site", y, min_avg_max("ting:voltage_volts", 'site="$site"') + mark_targets, "volt",
                     "Min, average and max per plotted point from the 1-minute rollups, so a sag of one 0.25 s sample "
                     "stays visible over a month. Dashed: 110 / 114 / 126 V. Bands: this site's markers (Markers dropdown).",
                     custom={"thresholdsStyle": {"mode": "dashed"}, "axisSoftMin": 110, "axisSoftMax": 128},
                     defaults={"thresholds": {"mode": "absolute", "steps": VOLT_STEPS}, "decimals": 1},
                     overrides=site_colours() + [o for s in SITES for o in band(s)] + mark_overrides, repeat=True, w=12, h=9))
    y += 9
    grids = "; ".join(f"{site} ({info['name']}): {info['grid']}" for site, info in SITE_INFO.items() if info.get("grid"))
    panels.append(ts("Frequency · site $site", y, min_avg_max("ting:frequency_hertz", 'site="$site"') + mark_targets, "hertz",
                     "Line frequency. Sites on different grids never move together" + (f" ({grids})" if grids else "")
                     + ". Bands: this site's markers.", custom={"axisSoftMin": 59.9, "axisSoftMax": 60.1},
                     defaults={"decimals": 3}, overrides=site_colours() + [o for s in SITES for o in band(s)] + mark_overrides,
                     repeat=True, w=12, h=8))
    y += 8
    panels.append(ts("THD", y, [
        target(f"avg_over_time(ting:thd_ratio:avg_1m{{{SEL}}}[$__interval])", "{{site}} avg", "A"),
        target(f"max_over_time(ting:thd_max_ratio:max_1m{{{SEL}}}[$__interval])", "{{site}} max", "B")],
        "percentunit", "Voltage THD: average (solid) and the sensor's interval maximum (dashed), per site.",
        custom={"axisSoftMin": 0}, defaults={"decimals": 1}, w=12,
        overrides=site_colours() + [{"matcher": {"id": "byRegexp", "options": ".* max$"},
                                     "properties": [{"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [6, 4]}},
                                                    {"id": "custom.lineWidth", "value": 1}]}]))
    panels.append(ts("Hi-Fi (peak)", y, [
        target(f"max_over_time(ting:hifi:max_1m{{{SEL}}}[$__interval])", "{{site}} max", "A")],
        "none", "Ting's Hi-Fi value (AveragePeaksMax), highest per point. Whisker Labs documents no unit; compare a "
        "site with its own normal level" + (" (" + ", ".join(f"{site} {info['hifi']}" for site, info in SITE_INFO.items()
                                                             if info.get("hifi")) + ")" if any(i.get("hifi") for i in SITE_INFO.values()) else "")
        + ".", x=12, w=12, custom={"axisSoftMin": 0},
        overrides=site_colours()))
    y += 8
    panels.append(settled(ts("Coverage", y, [target(
        f"100 * sum by (site) (sum_over_time(ting:voltage_volts:count_1m{{{SEL}}}[$__interval])) / ($__interval_ms / 250)",
        "{{site}}", "A")], "percent", "Share of expected samples per plotted point. A drop to 0 is an outage.",
        custom={"axisSoftMin": 0, "axisSoftMax": 100}, overrides=site_colours(), h=6)))
    y += 6

    own = " This data source's own exporter and VictoriaMetrics: each store scrapes only its own host."
    health = [
        ts("Delivery delay", 0, [target(f"max by (site) (max_over_time(ting_clock_offset_seconds{{{SEL}}}[$__interval]))", "{{site}}")],
           "s", "Live path ~0.55 s, buffered 4-8 s." + own, w=12, h=7, overrides=site_colours(), interval="30s"),
        ts("Reconnects", 0, [target(f"sum by (site, reason) (increase(ting_stream_disconnects_total{{{SEL}}}[$__interval]))",
                                    "{{site}} {{reason}}")], "none",
           "Hub sessions ended, by reason (stale = the sensor went silent)." + own, x=12, w=12, h=7,
           custom={"drawStyle": "bars", "fillOpacity": 60}, overrides=site_colours(), interval="5m"),
        ts("Push to VictoriaMetrics", 7, [target("sum by (target, result) (rate(ting_push_samples_total[$__interval]))",
                                                 "{{target}} {{result}}")],
           "short", "Samples per second by push target and outcome. Anything but ok for long means that target "
           "(the local VictoriaMetrics, or the peer over the tunnel) is not taking data." + own, w=12, h=7, interval="1m"),
        ts("Push lag", 7, [target("max by (target) (ting_push_lag_seconds)", "{{target}}", "A")],
           "s", "Age of the oldest sample each store has not taken yet. A store that is unreachable (the peer over "
           "the tunnel) falls behind; the outbox keeps everything (256 MiB, about two weeks) and catches up." + own,
           x=12, w=12, h=7, interval="1m"),
        ts("Storage", 14, [target('sum(vm_data_size_bytes{job="victoria-metrics"})', "VictoriaMetrics data", "A"),
                           target('min(vm_free_disk_space_bytes{job="victoria-metrics"})', "free on the data disk", "B")],
           "bytes", "Alert at 90 GB of data and below 25 GB free; VictoriaMetrics stops writing below 10 GB free." + own,
           w=12, h=7, interval="10m"),
        ts("Sensor's own high/low", 14, [
            target(f"max by (site) (ting_voltage_rolling_high_volts{{{SEL}}})", "{{site}} high", "A"),
            target(f"min by (site) (ting_voltage_rolling_low_volts{{{SEL}}})", "{{site}} low", "B")],
           "volt", "VoltageHi/Lo as the sensor reports them: extremes over a long window of its own, not per sample. "
           "A jump shows the sensor restarted its window.", x=12, w=12, h=7,
           custom={"lineInterpolation": "stepAfter"}, overrides=site_colours(), interval="5m"),
        ts("Outbox and rollup repair", 21, [target("max(ting_outbox_bytes)", "outbox bytes", "A"),
                                            target("sum by (target) (increase(ting_rollup_repaired_minutes_total[$__interval]))",
                                                   "{{target}} rollup minutes repaired", "B")],
           "short", "Outbox size on disk, and the 1-minute rollups the exporter filled where vmalert missed them." + own,
           w=12, h=7, interval="10m"),
        ts("Health checks", 21, [target("min by (check) (ting_health_check_up)", "{{check}}", "A")], "none",
           "The exporter's own checks of VictoriaMetrics, vmalert, Alertmanager and the peer (1 = passes). A check "
           "that fails 5 minutes notifies Home Assistant directly." + own, x=12, w=12, h=7, interval="1m",
           custom={"lineInterpolation": "stepAfter"}, defaults={"min": 0, "max": 1}),
    ]
    panels.append(row("Exporter and storage", y, health))
    y += 1
    raw = [ts("Raw voltage · site $site", 0, [
        target(f'min_over_time(ting_voltage_volts{{site="$site"}}[$__interval])', "{{site}} min", "A"),
        target(f'max_over_time(ting_voltage_volts{{site="$site"}}[$__interval])', "{{site}} max", "C")] + mark_targets,
        "volt", "Every 0.25 s sample. For zooms of up to a day; longer ranges belong to the rollup panels above.",
        custom={"thresholdsStyle": {"mode": "dashed"}}, defaults={"thresholds": {"mode": "absolute", "steps": VOLT_STEPS}},
        overrides=site_colours() + [o for s in SITES for o in band(s)] + mark_overrides, repeat=True, w=12, h=9,
        interval="250ms")]
    panels.append(row("Raw 4 Hz (zoom)", y, raw))
    y += 1
    active = f'(last_over_time(ting_context{{context="source", {SEL}}}[{CONTEXT_LOOKBACK}]) == 1)'
    context = [
        ts("Power source", 0, [target(f"max by (site, value) ({active})", "{{site}} {{value}}", "A", interval="5m")], "none",
           "The power source marked with `ting-exporter mark <site> source=<name>`, in force until the next mark.",
           w=24, h=5, custom={"drawStyle": "bars", "fillOpacity": 60, "lineWidth": 0}, defaults={"min": 0, "max": 1}),
        ts("THD by power source", 5, [target(
            f"100 * avg by (site, value) (avg_over_time(ting:thd_ratio:avg_1m{{{SEL}}}[$__interval]) "
            f"* on (site) group_left (value) {active})", "{{site}} {{value}}", "A")],
           "percent", "Average THD while each marked source was in force (legend: mean and max over the range).",
           w=12, h=8, interval="5m"),
        ts("Hi-Fi by power source", 5, [target(
            f"max by (site, value) (max_over_time(ting:hifi:max_1m{{{SEL}}}[$__interval]) "
            f"* on (site) group_left (value) {active})", "{{site}} {{value}}", "A")],
           "none", "Highest Hi-Fi while each marked source was in force (legend: mean and max over the range).",
           x=12, w=12, h=8, interval="5m"),
    ]
    for p in context[1:]:
        p["options"]["legend"] = {"displayMode": "table", "placement": "bottom", "showLegend": True, "calcs": ["mean", "max"]}
    panels.append(row("Power source (context marks)", y, context))
    y += 1
    cloud = [
        ts("Outdoor temperature", 0, [target(f"max by (site) (ting_outdoor_temperature_celsius{{{SEL}}})", "{{site}}", "A")],
           "celsius", "Outdoor temperature at the site, as Ting's conditions report it.", w=8, h=7, interval="5m",
           overrides=site_colours(), custom={"lineInterpolation": "stepAfter"}),
        ts("Outage risk", 0, [target(f"max by (site) (ting_outage_risk{{{SEL}}})", "{{site}}", "A")], "none",
           "Ting's outage risk for the site, the number Ting sends (it looks like a percentage).", x=8, w=8, h=7,
           interval="5m", overrides=site_colours(), custom={"lineInterpolation": "stepAfter"}),
        ts("Hazard state", 0, [target(f"max by (site) (ting_hazard_state{{{SEL}}})", "{{site}}", "A"),
                               target(f"max by (site) (ting_fire_detected{{{SEL}}})", "{{site}} fire", "B")], "none",
           "Ting's fire hazard state: 0 none, 1 learning, 2 reviewed not fire, 3 elevated suspicious, 4 power quality "
           "hazard, 5 fire hazard, -1 unknown.", x=16, w=8, h=7, interval="5m", overrides=site_colours(),
           custom={"lineInterpolation": "stepAfter"}),
    ]
    panels.append(row("Ting cloud status", y, cloud))

    return {
        "uid": "ting-power",
        "title": "Ting power quality",
        "description": DESCRIPTION,
        "tags": ["ting", "power"],
        "timezone": "browser",
        "editable": True,
        "graphTooltip": 1,
        "refresh": "1m",
        "time": {"from": "now-24h", "to": "now"},
        "timepicker": {"refresh_intervals": ["1m", "5m", "15m", "1h"]},
        "schemaVersion": 39,
        "templating": {"list": [
            {"name": "ds", "label": "Data source", "type": "datasource", "query": "prometheus", "current": {},
             "hide": 0, "refresh": 1, "regex": "", "options": [], "includeAll": False, "multi": False},
            {"name": "site", "label": "Site", "type": "query", "datasource": DS,
             "query": {"query": SITE_QUERY, "refId": "site"}, "definition": SITE_QUERY,
             "refresh": 2, "includeAll": True, "multi": True, "current": {"text": "All", "value": "$__all"},
             "sort": 1, "hide": 0, "options": []},
            {"name": "markers", "label": "Markers", "type": "custom", "multi": True, "includeAll": False, "hide": 0,
             "query": ", ".join(f"{label} : {value}" for value, (label, *_) in MARKERS.items()),
             "current": {"text": [MARKERS[m][0] for m in DEFAULT_MARKERS], "value": list(DEFAULT_MARKERS)},
             "options": [{"text": label, "value": value, "selected": value in DEFAULT_MARKERS}
                         for value, (label, *_) in MARKERS.items()]},
        ]},
        "annotations": {"list": [
            # Your own notes: Ctrl/Cmd+click (or +drag for a range) on a panel adds one there; one posted to
            # /api/annotations with this dashboard's uid and no panelId shows on every panel. Grafana keeps
            # them by dashboard uid, so they survive a re-import of this file.
            {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "hide": False,
             "iconColor": "rgba(0, 211, 255, 1)", "name": "Notes", "type": "dashboard"},
            # alerts that belong to no site (storage, vmalert): on every panel
            {"datasource": DS, "enable": True, "hide": True, "name": "exporter alerts (no site)", "iconColor": "#e87ba4",
             "expr": f'(ALERTS{{alertstate="firing", alertname=~"Ting.*|Recordings.*|VictoriaMetrics.*|Vmalert.*", site=""}}) '
                     f'and on () {gate("alerts")}',
             "step": "1m", "titleFormat": "{{alertname}}", "textFormat": "exporter alert", "useValueForTime": False},
        ]},
        "panels": panels,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--sites-file", help="site.toml (default: the example sites a and b)")
    args = parser.parse_args(argv)
    if args.sites_file:
        with open(args.sites_file, "rb") as f:
            doc = tomllib.load(f)
        configure(doc["sites"], doc.get("dashboard", {}).get("description"))
    else:
        configure(EXAMPLE_SITES)
    json.dump(build(), sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
