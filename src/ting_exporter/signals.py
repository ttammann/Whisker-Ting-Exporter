"""The signal registry: every exported stream signal, defined once (design 5.2).

A Signal says where a value comes from (hub message kind and field), what it is
called in VictoriaMetrics, how it is validated and rounded, how often it is
stored, and which 1-minute rollups vmalert computes from it. Everything else
derives from this table:

    pipeline/decode.py    which fields and categories to read, what to discard
    pipeline/__init__.py  storage rule, rounding, metric name
    cloud/hub.py          which DataElements to subscribe
    rules.py              deploy/vmalert/ting-rollups.yml (a test fails if the file differs)
    repair.py             the rollup expressions it evaluates to fill missing minutes
    tools/make_dashboard.py, tools/reference_rollups.py (restates the table independently)

Adding a signal: one Signal here, then `ting-exporter rules > deploy/vmalert/ting-rollups.yml`,
the reference script if it has rollups, and the golden files (README, Development).

The series that are not stream samples (notifications, inferred cuts, context,
REST values) are listed in DERIVED_METRICS, so the dashboard and alert tests know them.

Hub message kinds (design 2.4):

    COMBO        updateComboBinaryData, args[0] is a map with DataTimeUtc,
                 one message per 0.25 s sample (DataElement ComboBinaryData)
    CATEGORICAL  updateGraphMultiCategorical, args[0] is a list of
                 {Category, Value (string), ObsTime}; Category is the DataElement
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field

COMBO = "combo"
CATEGORICAL = "categorical"

COMBO_TARGET = "updateComboBinaryData"
CATEGORICAL_TARGET = "updateGraphMultiCategorical"
DUPLICATE_TARGET = "updateGraphMulti"  # an exact duplicate of the categorical data (design 2.4)
COMBO_ELEMENT = "ComboBinaryData"
COMBO_TIME_FIELD = "DataTimeUtc"
CATEGORICAL_TIME_FIELD = "ObsTime"
HIGH_FIELD, LOW_FIELD = "VoltageHi", "VoltageLo"  # the sensor's own long-window extremes (restart fingerprint)

ROLLUP_WINDOW = "1m"


@dataclass(frozen=True)
class Storage:
    """How often a signal is written to VictoriaMetrics.

    every       every sample (4 Hz)
    on_change   when the rounded value differs from the last stored one, or
                `seconds` of device time have passed since it (heartbeat)
    """

    kind: str
    seconds: float = 0.0

    def __str__(self) -> str:
        return self.kind if self.kind == "every" else f"{self.kind}({self.seconds:g} s)"


EVERY = Storage("every")


def on_change(heartbeat: float = 60.0) -> Storage:
    return Storage("on_change", heartbeat)


@dataclass(frozen=True)
class Rollup:
    """One 1-minute vmalert recording rule: `<agg>_over_time(metric[1m])`.

    `round_to` rounds an average to that step (MetricsQL round(q, nearest)).
    """

    agg: str  # min | max | avg | count
    round_to: float | None = None

    def __post_init__(self) -> None:
        if self.agg not in ("min", "max", "avg", "count"):
            raise ValueError(f"unknown rollup {self.agg}")


@dataclass(frozen=True)
class Signal:
    key: str  # short name, used in logs, discard reasons and probe output
    source: str  # COMBO or CATEGORICAL
    field: str  # map key (COMBO) or Category (CATEGORICAL)
    metric: str
    unit: str
    help: str
    decimals: int  # rounding before the push; 0 = integer
    storage: Storage = EVERY
    check: Callable[[float], bool] = field(default=math.isfinite, compare=False)
    required: bool = False  # COMBO only: no sample at all without it
    primary: bool = False  # drives the stale watchdog, delay and gap statistics, inferred cuts
    rollups: tuple[Rollup, ...] = ()

    @property
    def element(self) -> str:
        """The hub DataElement to subscribe to for this signal."""
        return COMBO_ELEMENT if self.source == COMBO else self.field

    def record_name(self, rollup: Rollup) -> str:
        return f"ting:{self.metric.removeprefix('ting_')}:{rollup.agg}_{ROLLUP_WINDOW}"


MIN, MAX, COUNT = Rollup("min"), Rollup("max"), Rollup("count")


def _volts(v: float) -> bool:
    return 0 < v <= 300


REGISTRY: tuple[Signal, ...] = (
    Signal(
        "voltage", COMBO, "Voltage", "ting_voltage_volts", "V",
        "RMS line voltage per 0.25 s sample.",
        decimals=3, check=_volts, required=True, primary=True,
        rollups=(MIN, MAX, Rollup("avg", 0.0001), COUNT),
    ),
    Signal(
        "hifi", COMBO, "AveragePeaksMax", "ting_hifi", "1",
        "Ting Hi-Fi value (AveragePeaksMax), an integer; Whisker Labs documents no unit.",
        decimals=0, check=lambda v: v >= 0,
        rollups=(MIN, MAX, Rollup("avg", 0.01)),
    ),
    Signal(
        "rolling_high", COMBO, HIGH_FIELD, "ting_voltage_rolling_high_volts", "V",
        "The sensor's own long-window voltage high (not a per-sample maximum).",
        decimals=3, storage=on_change(60), check=_volts,
    ),
    Signal(
        "rolling_low", COMBO, LOW_FIELD, "ting_voltage_rolling_low_volts", "V",
        "The sensor's own long-window voltage low (not a per-sample minimum).",
        decimals=3, storage=on_change(60), check=_volts,
    ),
    Signal(
        "frequency", CATEGORICAL, "frequency", "ting_frequency_hertz", "Hz",
        "Line frequency per 0.25 s sample.",
        decimals=4, check=lambda v: 0 < v < 1000,
        rollups=(MIN, MAX, Rollup("avg", 0.00001)),
    ),
    Signal(
        "thd", CATEGORICAL, "thdAvg", "ting_thd_ratio", "ratio",
        "Total harmonic distortion as a ratio (0.03 = 3 %), average; updates about every 30 s.",
        decimals=5, storage=on_change(60), check=lambda v: v >= 0,
        rollups=(Rollup("avg", 0.00001),),
    ),
    Signal(
        "thd_min", CATEGORICAL, "thdMin", "ting_thd_min_ratio", "ratio",
        "Total harmonic distortion as a ratio, minimum over the sensor's THD interval.",
        decimals=5, storage=on_change(60), check=lambda v: v >= 0,
        rollups=(MIN,),
    ),
    Signal(
        "thd_max", CATEGORICAL, "thdMax", "ting_thd_max_ratio", "ratio",
        "Total harmonic distortion as a ratio, maximum over the sensor's THD interval.",
        decimals=5, storage=on_change(60), check=lambda v: v >= 0,
        rollups=(MAX,),
    ),
)

COMBO_SIGNALS = tuple(s for s in REGISTRY if s.source == COMBO)
CATEGORICAL_SIGNALS = {s.field: s for s in REGISTRY if s.source == CATEGORICAL}
PRIMARY = next(s for s in REGISTRY if s.primary)
BY_KEY = {s.key: s for s in REGISTRY}

# Pushed series that do not come from the stream registry. name -> meaning (for docs, dashboard and alert tests).
DERIVED_METRICS = {
    "ting_notification": "one sample per Ting notification at its event time (notifications.py)",
    "ting_power_outage": "1 site / 2 community outage every minute it lasts, 0 at its end (notifications.py)",
    "ting_power_cut": "inferred power cut: silence that ended in a sensor restart (pipeline/cuts.py)",
    "ting_stream_gap": "inferred data gap: silence that ended without a restart (pipeline/cuts.py)",
    "ting_context": "context marks, e.g. the power source (context.py)",
}


def elements() -> tuple[str, list[str]]:
    """(required element, optional elements) for InitializeStreaming, in registry order."""
    optional: list[str] = []
    for s in REGISTRY:
        if s.element != COMBO_ELEMENT and s.element not in optional:
            optional.append(s.element)
    return COMBO_ELEMENT, optional


def validate(registry: tuple[Signal, ...] = REGISTRY) -> None:
    """Consistency checks, run by the tests and at the start of every command."""
    metrics = [s.metric for s in registry]
    if len(set(metrics)) != len(metrics):
        raise ValueError("duplicate metric name in the registry")
    if len({s.key for s in registry}) != len(registry):
        raise ValueError("duplicate signal key in the registry")
    if sum(s.primary for s in registry) != 1:
        raise ValueError("exactly one signal must be primary")
    for s in registry:
        if s.source not in (COMBO, CATEGORICAL):
            raise ValueError(f"{s.key}: unknown source {s.source}")
        if not s.metric.startswith("ting_"):
            raise ValueError(f"{s.key}: metric names start with ting_")
        if s.metric in DERIVED_METRICS:
            raise ValueError(f"{s.key}: {s.metric} is a derived series")
        if s.required and s.source != COMBO:
            raise ValueError(f"{s.key}: only COMBO fields can be required")
        if s.primary and not (s.required and s.storage.kind == "every"):
            raise ValueError(f"{s.key}: the primary signal is required and stored every sample")
        if s.storage.kind not in ("every", "on_change"):
            raise ValueError(f"{s.key}: unknown storage rule {s.storage.kind}")
        if s.storage.kind != "every" and s.storage.seconds <= 0:
            raise ValueError(f"{s.key}: {s.storage.kind} needs seconds > 0")
        for r in s.rollups:
            if r.agg == "avg" and r.round_to is None:
                raise ValueError(f"{s.key}: avg rollups need round_to")
            if r.agg == "count" and s.storage.kind != "every":
                raise ValueError(f"{s.key}: count is only meaningful for every-sample signals")
    if not any(r.agg == "count" for r in next(s for s in registry if s.primary).rollups):
        raise ValueError("the primary signal needs a count rollup (coverage, rollup repair)")
