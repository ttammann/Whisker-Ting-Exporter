"""The REST registry: the shapes a real account sends in, one number per value out, unknown shapes skipped."""

import pytest

from ting_exporter import rest


def view(device=None, **kw):
    return rest.View(device or {}, **kw)


@pytest.mark.parametrize(("device", "state"), [
    ({"fireHazardStatus": {"learningMode": True}}, 1),
    ({"isFire": True, "fireHazardStatus": {}}, 5),
    ({"fireHazardStatus": {"efhStatus": {"status": "HazardFound"}}}, 5),
    ({"fireHazardStatus": {"ufhStatus": {"status": "PowerQualityHazard"}}}, 4),
    ({"fireHazardStatus": {"efhStatus": {"status": "ElevatedSuspicious"}}}, 3),
    ({"fireHazardStatus": {"efhStatus": {"status": "ReviewedNotFire"}}}, 2),
    ({"fireHazardStatus": {"efhStatus": {"status": None}, "ufhStatus": {"status": "NoHazards"}}}, 0),
    ({"fireHazardStatus": {"efhStatus": {"status": "Detected"}}}, -1),
    ({}, None),
])
def test_hazard_state(device, state):
    assert rest.hazard_state(view(device)) == state


@pytest.mark.parametrize(("risk", "value"), [(29, 29.0), (0.5, 0.5), ({"level": 3}, 3.0), ({"status": "Elevated"}, None),
                                             ("High", None), (True, None), (None, None)])
def test_outage_risk(risk, value):
    assert rest.outage_risk(view(outage_risk=risk)) == value


def test_views_join_the_three_replies():
    user = {"devices": [{"serialNumber": "TNG000001", "siteId": 100, "isFire": False},
                        {"serialNumber": "TNG000002", "siteId": 200}, "junk", {"serialNumber": ""}]}
    conditions = {"currentTemperatures": {"100": 21.5, "200": "warm"}, "currentOutageRisks": {"200": 29},
                  "devices": [{"serialNumber": "TNG000002", "hasFrozenPipe": True}]}
    views = rest.views(user, conditions, {"TNG000002": {"level": 55}})
    assert set(views) == {"TNG000001", "TNG000002"}
    got = {serial: {s.metric: s.extract(v) for s in rest.REST_REGISTRY} for serial, v in views.items()}
    assert got["TNG000001"]["ting_outdoor_temperature_celsius"] == 21.5
    assert got["TNG000001"]["ting_fire_detected"] == 0.0 and got["TNG000001"]["ting_hazard_efh_level"] is None
    assert got["TNG000002"]["ting_outdoor_temperature_celsius"] is None  # "warm" is not a number
    assert got["TNG000002"]["ting_outage_risk"] == 29.0 and got["TNG000002"]["ting_frozen_pipe_detected"] == 1.0
    assert got["TNG000002"]["ting_frozen_pipe_level"] == 55.0 and got["TNG000001"]["ting_frozen_pipe_level"] is None
    assert rest.views(None, None, {}) == {} and rest.views("x", [], {}) == {}


def test_registry():
    rest.validate()
    assert all(s.metric.startswith("ting_") for s in rest.REST_REGISTRY)
