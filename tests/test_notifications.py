"""notifications.py: parsing, outage pairing and deterministic samples from the Ting notification history."""

from ting_exporter import notifications as nt

from . import fakes

SERIAL = fakes.SERIAL_A
SITES = {fakes.SERIAL_A: "home", fakes.SERIAL_B: "cabin"}
MS = 1000
LABELS = f'serial="{SERIAL}",site="home"'


def note(i, kind, utc, serial=SERIAL):
    return {"id": i, "eventType": kind, "eventCategory": "PowerQuality", "title": kind,
            "eventTimestampUtc": utc, "serialNumber": serial}


def ms(text):
    return nt.parse(note("x", "Sag", text)).ts_ms


def parsed(*records):
    return [nt.parse(r) for r in records]


def outage_samples(notes, now):
    return [(s.ts_ms, s.text) for s in nt.samples(notes, SITES, now) if s.metric == "ting_power_outage"]


def test_parse_takes_the_first_plausible_time_and_rejects_incomplete_records():
    n = nt.parse(fakes.NOTIFICATIONS[1])  # eventTimestampUtc is the API's 0001-01-01 placeholder
    assert (n.id, n.type, n.title, n.serial) == ("n2", "CommunityPowerOutage", "Community Power Outage", SERIAL)
    assert n.ts_ms == ms("2026-03-11T22:48:57.412Z")  # from eventTimestampLocal, -07:00
    record = {"id": "r", "eventType": "PowerRestored", "serialNumber": SERIAL, "eventTimestampUtc": "0001-01-01T00:00:00",
              "eventTimestampLocal": None, "sentUtc": "2026-03-12T01:48:10Z"}
    assert nt.parse(record).ts_ms == ms("2026-03-12T01:48:10Z")
    record["sentUtc"] = "2098-06-12T03:00:00Z"  # implausible future
    assert nt.parse(record, now_ms=ms("2026-03-12T02:00:00Z")) is None
    assert nt.parse({"eventType": "Sag", "serialNumber": "S", "eventTimestampUtc": "2026-03-11T22:00:00Z"}) is None  # no id
    assert nt.parse({"id": 0, "eventType": "Sag", "serialNumber": "S", "eventTimestampUtc": "2026-03-11T22:00:00Z"}).id == "0"


def test_outages_pair_per_sensor_upgrade_to_community_and_close_on_any_restore():
    notes = parsed(
        note("1", "PowerOutage", "2026-03-07T21:29:30Z"), note("2", "PowerRestored", "2026-03-07T21:31:35Z"),
        note("3", "PowerOutage", "2026-03-11T23:32:04Z"), note("4", "CommunityPowerOutage", "2026-03-11T23:33:00Z"),
        note("5", "InternetAndPowerRestored", "2026-03-12T01:47:54Z"),  # an unknown *Restored type still closes it
        note("6", "PowerOutageAndRestored", "2026-01-15T01:07:25Z", fakes.SERIAL_B),
        note("7", "PowerRestored", "2026-01-15T03:00:00Z", fakes.SERIAL_B),  # a restore without a start: ignored
        note("8", "Sag", "2026-01-13T20:06:06Z", fakes.SERIAL_B),
    )
    got = [(o.serial, o.kind, o.start_ms, o.end_ms, o.capped) for o in nt.outages(notes, ms("2026-03-12T03:00:00Z"))]
    assert got == [
        (SERIAL, 1, ms("2026-03-07T21:29:30Z"), ms("2026-03-07T21:31:35Z"), False),
        (SERIAL, 2, ms("2026-03-11T23:32:04Z"), ms("2026-03-12T01:47:54Z"), False),
        (fakes.SERIAL_B, 1, ms("2026-01-15T01:07:25Z"), ms("2026-01-15T01:07:25Z") + 60 * MS, False),
    ]


def spans(notes, now):
    return [(o.kind, o.start_ms, o.end_ms, o.capped) for o in nt.outages(notes, ms(now))]


def test_a_missed_restore_does_not_merge_two_outages():
    """The next cut proves the power came back in between: the open outage ends there."""
    notes = parsed(note("1", "PowerOutage", "2026-01-13T22:14:03Z"),  # its restore never arrives
                   note("2", "PowerOutage", "2026-01-15T13:00:00Z"), note("3", "PowerRestored", "2026-01-15T13:05:00Z"))
    assert spans(notes, "2026-01-16T03:00:00Z") == [
        (1, ms("2026-01-13T22:14:03Z"), ms("2026-01-15T13:00:00Z"), False),
        (1, ms("2026-01-15T13:00:00Z"), ms("2026-01-15T13:05:00Z"), False)]


def test_a_brownout_after_a_cut_ends_it_and_two_cuts_within_a_day_stay_apart():
    """M15: a Sag needs a powered sensor, so the cut is over (the power is back, if low)."""
    notes = parsed(note("1", "PowerOutage", "2026-03-11T13:00:00Z"), note("2", "Sag", "2026-03-11T15:00:00Z"),
                   note("3", "PowerOutage", "2026-03-11T17:00:00Z"), note("4", "PowerRestored", "2026-03-11T17:05:00Z"))
    assert spans(notes, "2026-03-12T03:00:00Z") == [
        (1, ms("2026-03-11T13:00:00Z"), ms("2026-03-11T15:00:00Z"), False),
        (1, ms("2026-03-11T17:00:00Z"), ms("2026-03-11T17:05:00Z"), False)]
    tr = nt.Tracker(SITES)
    tr.update([note(*a) for a in (("1", "PowerOutage", "2026-03-11T13:00:00Z"), ("2", "Sag", "2026-03-11T15:00:00Z"))],
              ms("2026-03-11T16:00:00Z"))
    assert tr.open_outages(ms("2026-03-11T16:00:00Z")) == {}


def test_an_outage_over_a_day_ends_at_its_restore_however_late():
    """M6: a 40 h storm outage is 40 h, not cut at 24 h; with a brownout at 30 h it ends there (the power was back)."""
    start, restore = note("1", "CommunityPowerOutage", "2026-02-10T03:00:00Z"), note("2", "PowerRestored", "2026-02-11T19:00:00Z")
    assert spans(parsed(start, restore), "2026-02-12T03:00:00Z") == [
        (2, ms("2026-02-10T03:00:00Z"), ms("2026-02-11T19:00:00Z"), False)]
    samples = outage_samples(parsed(start, restore), ms("2026-02-12T03:00:00Z"))
    assert samples[-1] == (ms("2026-02-11T19:00:00Z"), "0") and len(samples) == 40 * 60 + 1
    brownout = note("3", "Sag", "2026-02-11T09:00:00Z")
    assert spans(parsed(start, brownout, restore), "2026-02-12T03:00:00Z") == [
        (2, ms("2026-02-10T03:00:00Z"), ms("2026-02-11T09:00:00Z"), False)]


def test_an_outage_with_no_news_for_a_day_is_drawn_a_day_and_still_closes_at_a_late_restore():
    start = note("1", "PowerOutage", "2026-02-10T03:00:00Z")
    tr = nt.Tracker(SITES)
    written = tr.update([start], ms("2026-02-11T09:00:00Z"))[0]
    assert tr.open_outages(ms("2026-02-11T09:00:00Z")) == {}  # capped: not reported active
    assert max(s.ts_ms for s in written) < ms("2026-02-11T03:00:00Z") and {s.text for s in written if s.metric == nt.OUTAGE_METRIC} == {"1"}
    later = tr.update([start, note("2", "PowerRestored", "2026-02-11T12:00:00Z")], ms("2026-02-11T12:30:00Z"))[0]
    outage = sorted((s.ts_ms, s.text) for s in later if s.metric == nt.OUTAGE_METRIC)
    assert outage[0][0] >= ms("2026-02-11T03:00:00Z") and outage[-1] == (ms("2026-02-11T12:00:00Z"), "0")
    assert all(text == "1" for _, text in outage[:-1])  # no 0 was written at the cap


def test_a_short_outage_reported_during_an_open_one_ends_the_open_one_first():
    """P1: PowerOutageAndRestored needs the power back; it closes the open outage, then is its own minute."""
    notes = parsed(note("1", "CommunityPowerOutage", "2026-03-11T13:00:00Z"),
                   note("2", "PowerOutageAndRestored", "2026-03-11T13:30:20Z"),
                   note("3", "PowerRestored", "2026-03-11T14:00:00Z"))
    assert spans(notes, "2026-03-11T15:00:00Z") == [
        (2, ms("2026-03-11T13:00:00Z"), ms("2026-03-11T13:30:20Z"), False),
        (1, ms("2026-03-11T13:30:20Z"), ms("2026-03-11T13:31:20Z"), False)]
    by_time = {}
    for t, text in outage_samples(notes, ms("2026-03-11T15:00:00Z")):  # VictoriaMetrics: the larger value on a tie
        by_time[t] = max(by_time.get(t, 0), int(text))
    after = [v for t, v in sorted(by_time.items()) if t >= ms("2026-03-11T13:00:00Z")]
    assert 0 not in after[:-1] and after[-1] == 0  # no 0 inside the outage, one at its very end


def test_a_community_notice_just_after_a_short_cuts_restore_upgrades_that_cut():
    """P4: the classification can lag a cut that lasts under a minute."""
    notes = parsed(note("1", "PowerOutage", "2026-03-11T23:00:00Z"), note("2", "PowerRestored", "2026-03-11T23:00:40Z"),
                   note("3", "CommunityPowerOutage", "2026-03-11T23:01:10Z"))
    assert spans(notes, "2026-03-12T00:00:00Z") == [(2, ms("2026-03-11T23:00:00Z"), ms("2026-03-11T23:00:40Z"), False)]
    later = parsed(note("4", "PowerRestored", "2026-03-11T23:00:40Z"), note("5", "CommunityPowerOutage", "2026-03-12T00:00:00Z"))
    assert spans(later, "2026-03-12T00:05:00Z") == [(2, ms("2026-03-12T00:00:00Z"), None, False)]  # an hour later: a new one


def test_the_shapes_in_the_owners_alerts_screen():
    """Brownouts and cuts are separate events; a cut may have a brownout before or after it."""
    cut = parsed(note("1", "Sag", "2026-01-13T21:06:00Z"), note("2", "PowerOutage", "2026-01-13T23:14:05Z"),
                 note("3", "PowerOrInternetRestored", "2026-01-13T23:14:40Z"))
    assert outage_samples(cut, ms("2026-01-14T03:00:00Z")) == [(ms("2026-01-13T23:14:05Z"), "1"), (ms("2026-01-13T23:14:40Z"), "0")]
    assert [s.labels for s in nt.samples(cut, SITES, ms("2026-01-14T03:00:00Z")) if s.metric == nt.NOTIFICATION_METRIC][0].count('type="Sag"') == 1
    community = parsed(note("4", "CommunityPowerOutage", "2026-01-06T18:57:10Z"), note("5", "PowerRestored", "2026-01-06T18:57:50Z"))
    assert spans(community, "2026-01-07T03:00:00Z") == [(2, ms("2026-01-06T18:57:10Z"), ms("2026-01-06T18:57:50Z"), False)]
    point = parsed(note("6", "PowerOutageAndRestored", "2026-01-15T02:07:00Z"))
    assert spans(point, "2026-01-15T03:00:00Z") == [(1, ms("2026-01-15T02:07:00Z"), ms("2026-01-15T02:08:00Z"), False)]
    assert outage_samples(parsed(note("7", "Sag", "2025-12-16T14:28:00Z")), ms("2025-12-17T03:00:00Z")) == []


def test_a_start_and_its_restore_in_the_same_millisecond_still_pair():
    notes = parsed(note("n10", "PowerRestored", "2026-03-11T23:00:00Z"), note("n9", "PowerOutage", "2026-03-11T23:00:00Z"))
    assert spans(notes, "2026-03-12T00:00:00Z") == [(1, ms("2026-03-11T23:00:00Z"), ms("2026-03-11T23:00:00Z"), False)]


def test_counts_follow_the_history_and_old_records_are_forgotten():
    """the gauge is what the history holds now; records that left it are dropped once they cannot pair."""
    tr = nt.Tracker(SITES)
    old = [note(str(i), "Sag", f"2025-11-{9 + i}T15:00:00Z") for i in range(1, 6)]
    tr.update(old, ms("2026-02-10T03:00:00Z"))
    assert sum(tr.counts().values()) == 5
    newer = old[3:] + [note("9", "Sag", "2026-02-11T15:00:00Z")]  # 1-3 left the server's history
    tr.update(newer, ms("2026-02-12T03:00:00Z"))
    assert sum(tr.counts().values()) == 3
    # 1 and 2 are gone; 3 is kept: within a day before the oldest record still in the history, it could still pair
    assert set(tr.notes) == {"3", "4", "5", "9"} and all(k[2] >= ms("2025-11-12T15:00:00Z") for k in tr.pushed)


def test_samples_minute_by_minute_with_the_kind_as_value_and_a_closing_zero():
    notes = parsed(*fakes.NOTIFICATIONS)  # community outage 15:48:57.412 - 15:49:06.428 local (-07:00)
    out = nt.samples(notes, SITES, now_ms=ms("2026-03-12T00:00:00Z"))
    assert [(s.ts_ms, s.text, s.labels) for s in out if s.metric == "ting_power_outage"] == [
        (ms("2026-03-11T22:48:57.412Z"), "2", LABELS), (ms("2026-03-11T22:49:00Z"), "2", LABELS),
        (ms("2026-03-11T22:49:06.428Z"), "0", LABELS)]
    points = [s for s in out if s.metric == "ting_notification"]
    assert len(points) == 3 and all(s.text == "1" for s in points)
    assert 'title="Power Brownout",type="Sag"' in points[0].labels and 'site="cabin"' in points[0].labels
    assert nt.samples(notes, SITES, ms("2026-03-12T00:00:00Z")) == out  # deterministic


def test_an_open_outage_stays_behind_now_so_a_later_restore_is_never_overshot():
    start = note("1", "CommunityPowerOutage", "2026-03-11T23:32:04Z")
    notes = parsed(start)
    assert outage_samples(notes, ms("2026-03-11T23:33:30Z")) == [(ms("2026-03-11T23:32:04Z"), "2")]  # within the lag
    assert [t for t, _ in outage_samples(notes, ms("2026-03-11T23:40:30Z"))] == [
        ms("2026-03-11T23:32:04Z")] + [ms(f"2026-03-11T23:{m}:00Z") for m in range(33, 38)]
    # the restore at 20:47:54 is seen at the next poll (20:48:30): every minute written so far is before it
    tr = nt.Tracker(SITES)
    written = []
    for now in ("2026-03-11T23:44:30Z", "2026-03-11T23:45:30Z", "2026-03-11T23:46:30Z", "2026-03-11T23:47:30Z"):
        written += tr.update([start], ms(now))[0]
    after, _ = tr.update([start, note("2", "PowerRestored", "2026-03-11T23:47:54Z")], ms("2026-03-11T23:48:30Z"))
    end = ms("2026-03-11T23:47:54Z")
    assert all(s.ts_ms < end for s in written if s.metric == "ting_power_outage")
    assert [(s.ts_ms, s.text) for s in after if s.metric == "ting_power_outage"][-1] == (end, "0")


def test_tracker_rewrites_an_upgraded_outage_and_reports_new_notifications_once():
    tr = nt.Tracker(SITES)
    site_start = note("1", "PowerOutage", "2026-03-11T23:00:00Z")
    first, fresh = tr.update([site_start], ms("2026-03-11T23:10:00Z"))
    assert [n.id for n, _ in fresh] == ["1"] and tr.open_outages(ms("2026-03-11T23:10:00Z")) == {SERIAL: "site"}
    assert {s.text for s in first if s.metric == "ting_power_outage"} == {"1"}
    upgrade = note("2", "CommunityPowerOutage", "2026-03-11T23:02:00Z")
    again, fresh = tr.update([site_start, upgrade, {"bad": 1}], ms("2026-03-11T23:10:30Z"))
    assert [n.id for n, _ in fresh] == ["2"] and tr.unparsable == 1
    # the same timestamps again, now 2: VictoriaMetrics keeps the larger value, no second series
    assert {s.ts_ms for s in again if s.metric == "ting_power_outage"} >= {s.ts_ms for s in first if s.metric == "ting_power_outage"}
    assert {s.text for s in again if s.metric == "ting_power_outage"} == {"2"}
    assert {s.labels for s in again + first if s.metric == "ting_power_outage"} == {LABELS}
    assert tr.open_outages(ms("2026-03-11T23:10:30Z")) == {SERIAL: "community"}
    nothing, fresh = tr.update([site_start, upgrade], ms("2026-03-11T23:10:40Z"))
    assert nothing == [] and fresh == [] and tr.unparsable == 0
    assert tr.counts() == {(SERIAL, "PowerOutage"): 1, (SERIAL, "CommunityPowerOutage"): 1}
