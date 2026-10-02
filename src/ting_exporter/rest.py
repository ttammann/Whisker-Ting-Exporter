"""REST values: hazards, outdoor temperature, outage risk, frozen pipe (design 0.5, 6.6).

The endpoints and field names come from the community integration
(Underzenith85 ha-whisker-ting, MIT), checked against a real account's replies
(`ting-exporter probe --rest` prints what an account answers): the device
records carry no isOnline and no fireHazardStatus.hazardSeverityLevel, so
neither is read; efhStatus/ufhStatus.level is null while there is no hazard;
currentOutageRisks holds a plain number per site, which looks like a
percentage. Every field is optional: a missing or unexpected value writes no
sample and is counted (ting_rest_missing), and an endpoint the account may not
use (403, 404) is asked again only hourly.

    GET /api/v1/Users/{user_id}             devices[].fireHazardStatus, isFire, hasFrozenPipe, siteId
    GET /api/v1/Users/{user_id}/conditions  currentTemperatures{siteId}, currentOutageRisks{siteId},
                                            devices[] (fresher copies of the device fields)
    GET /api/v1/FrozenPipe/{serial}         level

One registry, like the stream's (signals.py): REST_REGISTRY. Values are pushed
with the poll's whole minute as their timestamp, every poll.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

USER, CONDITIONS, FROZEN_PIPE = "user", "conditions", "frozen_pipe"
PATHS = {
    USER: "/api/v1/Users/{user_id}",
    CONDITIONS: "/api/v1/Users/{user_id}/conditions",
    FROZEN_PIPE: "/api/v1/FrozenPipe/{serial}",
}

# The overall hazard as a number (from the community integration's reading of efhStatus/ufhStatus).
HAZARD_STATES = {
    "no_hazards": 0, "learning": 1, "reviewed_not_fire": 2, "elevated_suspicious": 3,
    "power_quality_hazard": 4, "fire_hazard": 5, "unknown": -1,
}
_NORMAL = {None, "", "None", "NoHazard", "NoHazards", "Normal"}


@dataclass
class View:
    """What the polls returned, for one sensor."""

    device: dict[str, Any]  # from Users/{id}, overlaid with the fresher fields of conditions.devices[]
    temperature: Any = None  # conditions.currentTemperatures[siteId]
    outage_risk: Any = None  # conditions.currentOutageRisks[siteId]
    frozen_pipe: dict[str, Any] | None = None


@dataclass(frozen=True)
class RestSignal:
    key: str
    endpoint: str  # the endpoint it needs besides the device list
    metric: str
    unit: str
    help: str
    extract: Callable[[View], float | None]


def number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def flag(value: Any) -> float | None:
    return float(value) if isinstance(value, bool) else None


def _hazard(view: View) -> dict[str, Any]:
    h = view.device.get("fireHazardStatus")
    return h if isinstance(h, dict) else {}


def _status(h: dict[str, Any], key: str) -> Any:
    s = h.get(key)
    return s.get("status") if isinstance(s, dict) else None


def hazard_state(view: View) -> float | None:
    h = _hazard(view)
    if not h and "isFire" not in view.device:
        return None
    if h.get("learningMode") is True:
        state = "learning"
    elif view.device.get("isFire") is True or _status(h, "efhStatus") in ("PossibleFire", "HazardFound"):
        state = "fire_hazard"
    elif _status(h, "ufhStatus") == "PowerQualityHazard":
        state = "power_quality_hazard"
    elif _status(h, "efhStatus") == "ElevatedSuspicious":
        state = "elevated_suspicious"
    elif _status(h, "efhStatus") == "ReviewedNotFire":
        state = "reviewed_not_fire"
    elif _status(h, "efhStatus") in _NORMAL and _status(h, "ufhStatus") in _NORMAL:
        state = "no_hazards"
    else:
        state = "unknown"
    return float(HAZARD_STATES[state])


def _level(key: str) -> Callable[[View], float | None]:
    def get(view: View) -> float | None:
        s = _hazard(view).get(key)
        return number(s.get("level")) if isinstance(s, dict) else None
    return get


def outage_risk(view: View) -> float | None:
    """The number Ting sends for the site (a plain number; a wrapping object is tolerated)."""
    risk = view.outage_risk
    if isinstance(risk, dict):
        risk = next((risk[k] for k in ("level", "risk", "value") if k in risk), None)
    return number(risk)


def frozen_pipe_level(view: View) -> float | None:
    return number(view.frozen_pipe.get("level")) if isinstance(view.frozen_pipe, dict) else None


REST_REGISTRY: tuple[RestSignal, ...] = (
    RestSignal("hazard_state", USER, "ting_hazard_state", "1",
               "Ting's overall fire hazard state: 0 none, 1 learning, 2 reviewed not fire, 3 elevated suspicious, "
               "4 power quality hazard, 5 fire hazard, -1 unknown.", hazard_state),
    RestSignal("efh_level", USER, "ting_hazard_efh_level", "1",
               "Electrical fire hazard (EFH) level; Ting sends none while there is no hazard.", _level("efhStatus")),
    RestSignal("ufh_level", USER, "ting_hazard_ufh_level", "1",
               "Utility fire hazard (UFH) level; Ting sends none while there is no hazard.", _level("ufhStatus")),
    RestSignal("fire", USER, "ting_fire_detected", "1", "1 while Ting reports a fire condition (isFire).",
               lambda v: flag(v.device.get("isFire"))),
    RestSignal("frozen_pipe", USER, "ting_frozen_pipe_detected", "1", "1 while Ting reports a frozen pipe (hasFrozenPipe).",
               lambda v: flag(v.device.get("hasFrozenPipe"))),
    RestSignal("outdoor_temperature", CONDITIONS, "ting_outdoor_temperature_celsius", "Cel",
               "Outdoor temperature at the sensor's site, from Ting's conditions.", lambda v: number(v.temperature)),
    RestSignal("outage_risk", CONDITIONS, "ting_outage_risk", "1",
               "Ting's outage risk for the site, the number Ting sends (it looks like a percentage).", outage_risk),
    RestSignal("frozen_pipe_level", FROZEN_PIPE, "ting_frozen_pipe_level", "1", "Frozen-pipe risk level (FrozenPipe).",
               frozen_pipe_level),
)
REST_METRICS = {s.metric for s in REST_REGISTRY}


def views(user: Any, conditions: Any, frozen: dict[str, Any]) -> dict[str, View]:
    """serial -> View from the raw replies (any of them may be None)."""
    out: dict[str, View] = {}
    devices = user.get("devices") if isinstance(user, dict) else None
    for raw in devices if isinstance(devices, list) else []:
        if isinstance(raw, dict) and isinstance(raw.get("serialNumber"), str) and raw["serialNumber"]:
            out[raw["serialNumber"]] = View(dict(raw))
    if isinstance(conditions, dict):
        cond_devices = conditions.get("devices")
        for raw in cond_devices if isinstance(cond_devices, list) else []:
            if isinstance(raw, dict) and raw.get("serialNumber") in out:
                view = out[raw["serialNumber"]]
                for key in ("isFire", "isHvacVerified", "hasFrozenPipe", "fireHazardStatus"):
                    if key in raw:
                        view.device[key] = raw[key]
        temps = conditions.get("currentTemperatures")
        risks = conditions.get("currentOutageRisks")
        for view in out.values():
            site_id = view.device.get("siteId")
            if site_id is None:
                continue
            if isinstance(temps, dict):
                view.temperature = temps.get(str(site_id), temps.get(site_id))
            if isinstance(risks, dict):
                view.outage_risk = risks.get(str(site_id), risks.get(site_id))
    for serial, record in frozen.items():
        if serial in out and isinstance(record, dict):
            out[serial].frozen_pipe = record
    return out


def validate() -> None:
    metrics = [s.metric for s in REST_REGISTRY]
    if len(set(metrics)) != len(metrics) or len({s.key for s in REST_REGISTRY}) != len(REST_REGISTRY):
        raise ValueError("duplicate in the REST registry")
    for s in REST_REGISTRY:
        if s.endpoint not in PATHS or not s.metric.startswith("ting_"):
            raise ValueError(f"REST signal {s.key}: bad endpoint or metric")
