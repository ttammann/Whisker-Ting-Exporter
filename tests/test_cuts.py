"""Inferred power cuts and data gaps (design 5.4, T16)."""

import asyncio
import io
from datetime import datetime, timezone

from ting_exporter.pipeline import Pipeline
from ting_exporter.pipeline.cuts import CONFIRM_S, CORRELATE_MS, HOLD_MS, MAX_DRAW_MS, SilenceTracker, restarted
from ting_exporter.replay import replay

from .fakes import CUT

T0 = 1_790_600_000_000


def utc(text):
    return int(datetime.fromisoformat(text).replace(tzinfo=timezone.utc).timestamp() * 1000)


def test_the_cut_is_inferred_from_the_recording():
    """Replay the f-cut fixture (sensor B): the 3 min silence that a subscription confirmed is a gap, the 52 min
    silence that ended in a restart a power cut, and the silence after a socket error is not inferred (unwatched)."""
    pipeline = Pipeline({"TNG000002": "b"})
    samples = []
    process = pipeline.process

    def keep(*args):
        out, primary = process(*args)
        samples.extend(s for s in out if s.metric in ("ting_power_cut", "ting_stream_gap"))
        return out, primary

    pipeline.process = keep
    asyncio.run(replay([CUT], pipeline, dry_run=True, out=io.StringIO()))
    gap = [(s.ts_ms, s.value) for s in samples if s.metric == "ting_stream_gap"]
    cut = [(s.ts_ms, s.value) for s in samples if s.metric == "ting_power_cut"]
    assert gap[0] == (utc("2026-03-11T15:31:41.406"), 1.0) and gap[-1] == (utc("2026-03-11T15:35:01.406"), 0.0)
    assert cut[0] == (utc("2026-03-11T15:41:39.406"), 1.0) and cut[-1] == (utc("2026-03-11T16:33:50.406"), 0.0)
    assert len(cut) == 54 and all(ts % 60_000 == 0 and v == 1.0 for ts, v in cut[1:-1])
    assert pipeline.silences.sensors["TNG000002"].found == {"cut": 1, "gap": 1, "unattributed": 1}


def test_restarted_needs_a_narrow_window_that_shrank():
    assert restarted(121.3, 121.0, 128.6, 55.0) is True  # reset from a window a brownout and its spike had widened
    assert restarted(121.6, 120.4, 121.6, 120.5) is False  # as narrow, but only widened since the last restart
    assert restarted(125.0, 45.5, 125.0, 119.4) is False  # wide: no restart
    assert restarted(124.0, 123.0, None, None) is True  # nothing known from before
    assert restarted(None, 123.0, 125.0, 119.0) is None


def feed(tracker, serial, start_ms, seconds, *, hi=125.0, lo=119.0, arrival_offset=0.6, step_ms=250):
    out = []
    for i in range(int(seconds * 1000 / step_ms)):
        ts = start_ms + i * step_ms
        out += tracker.primary(serial, ts, ts / 1000 + arrival_offset, hi, lo)
    return out


def test_a_watched_and_confirmed_silence_without_a_restart_is_a_gap():
    t = SilenceTracker()
    feed(t, "S", T0, 10)
    t.event("S", "ended", {"reason": "stale"}, T0 / 1000 + 70)
    t.event("S", "subscribed", {}, T0 / 1000 + 75)
    out = feed(t, "S", T0 + 120_000, 2)  # back 110 s later, 45 s after a subscription that stayed empty
    assert [(s.kind, s.start_ms, s.end_ms) for s in out] == [("gap", T0 + 9_750 + 250, T0 + 120_000)]


def test_a_silence_ending_right_after_a_subscribe_is_not_a_gap():
    """Our own network came back and the hub sent its catch-up: nothing proves the sensor was silent."""
    t = SilenceTracker()
    feed(t, "S", T0, 10)
    t.event("S", "subscribed", {}, T0 / 1000 + 119)
    assert feed(t, "S", T0 + 118_000, 2) == []
    assert t.sensors["S"].found["unattributed"] == 1


def test_a_silence_we_did_not_watch_is_never_inferred():
    t = SilenceTracker()
    feed(t, "S", T0, 10)
    t.event("S", "ended", {"reason": "ws_error"}, T0 / 1000 + 70)  # our connect failed
    t.event("S", "subscribed", {}, T0 / 1000 + 200)
    assert feed(t, "S", T0 + 300_000, 2, hi=123.1, lo=123.0) == []  # not even a cut


def test_a_cut_needs_no_confirmation_and_waits_for_hi_lo():
    t = SilenceTracker()
    feed(t, "S", T0, 10)
    assert t.primary("S", T0 + 600_000, T0 / 1000 + 600.6, None, None) == []  # no Hi/Lo in the first sample
    assert t.primary("S", T0 + 600_250, T0 / 1000 + 600.9, 123.2, 123.0) == []  # decided, held for HOLD_MS
    [cut] = feed(t, "S", T0 + 600_500, HOLD_MS / 1000, hi=123.2, lo=123.0)  # released two minutes later
    assert (cut.kind, cut.start_ms, cut.end_ms) == ("cut", T0 + 10_000, T0 + 600_000)
    points = cut.points()
    assert points[0] == (T0 + 10_000, 1) and points[-1] == (T0 + 600_000, 0) and len(points) == 2 + 10


def test_state_across_a_restart():
    t = SilenceTracker()
    feed(t, "S", T0, 10)
    state = t.state()
    quick, slow = SilenceTracker(), SilenceTracker()
    quick.load(state, down_seconds=120)
    slow.load(state, down_seconds=3600)
    assert feed(quick, "S", T0 + 900_000, 1, hi=123.1, lo=123.0) == [] and [s.kind for s in quick.finish()] == ["cut"]
    assert feed(slow, "S", T0 + 900_000, 1, hi=123.1, lo=123.0) == [] and slow.finish() == []  # down an hour
    gap = SilenceTracker()
    gap.load(state, down_seconds=120)
    gap.event("S", "subscribed", {}, T0 / 1000 + 600)
    assert feed(gap, "S", T0 + 900_000, 1) == []  # a gap is never inferred across a restart


def test_late_samples_do_not_start_a_silence_and_long_ones_are_not_drawn():
    t = SilenceTracker()
    feed(t, "S", T0, 10)
    assert t.primary("S", T0 - 5_000, T0 / 1000 + 11, 125.0, 119.0) == []
    assert t.primary("S", T0 + MAX_DRAW_MS + 60_000, T0 / 1000 + 9e6, 123.1, 123.0) == []
    assert CONFIRM_S == 10.0


def sites(serial):
    return {"A": "a", "B": "b", "A2": "a"}[serial]


def go_dark_and_return(t, serial, back_ms):
    feed(t, serial, T0, 10)
    return t.primary(serial, back_ms, back_ms / 1000 + 0.6, 123.1, 123.0)  # restarted: window collapsed


def test_two_sites_restarting_together_is_not_a_power_cut():
    """2026-09-29 22:01Z: both sensors, at sites on different grids, silent for 67 s, back seconds apart with
    their windows reset. Upstream (the Ting cloud), not a power cut at both sites."""
    t = SilenceTracker(sites)
    assert go_dark_and_return(t, "A", T0 + 77_000) == []
    assert go_dark_and_return(t, "B", T0 + 80_000) == []
    out = feed(t, "A", T0 + 77_250, 130, hi=123.1, lo=123.0) + feed(t, "B", T0 + 80_250, 130, hi=123.1, lo=123.0)
    assert out == [] and t.finish() == []
    assert t.sensors["A"].found == {"cut": 0, "gap": 0, "unattributed": 1} == t.sensors["B"].found


def test_restarts_far_apart_or_at_one_site_are_power_cuts():
    t = SilenceTracker(sites)
    go_dark_and_return(t, "A", T0 + 77_000)
    go_dark_and_return(t, "B", T0 + 77_000 + CORRELATE_MS + 1_000)  # another site, but more than a minute later
    assert [s.serial for s in t.finish()] == ["A", "B"]
    same = SilenceTracker(sites)
    go_dark_and_return(same, "A", T0 + 77_000)
    go_dark_and_return(same, "A2", T0 + 78_000)  # the same site: one power cut, seen by both
    assert [s.serial for s in same.finish()] == ["A", "A2"]


def test_a_held_cut_survives_a_crash_in_the_state():
    t = SilenceTracker(sites)
    go_dark_and_return(t, "A", T0 + 77_000)
    state = t.state()
    assert state["_held"] and state["_restarts"]
    again = SilenceTracker(sites)
    again.load(state, down_seconds=30)
    [cut] = feed(again, "A", T0 + 77_250, HOLD_MS / 1000 + 1, hi=123.1, lo=123.0)
    assert (cut.serial, cut.end_ms) == ("A", T0 + 77_000)


def test_sites_on_one_power_feed_keep_their_cut():
    """A second sensor in the same house (TING_LOCATIONS): a cut there restarts both, and stays a cut."""
    from ting_exporter.pipeline import Pipeline

    p = Pipeline({"A": "a", "T": "Testing", "B": "b"}, locations={"Testing": "a"})
    assert [p.location(s) for s in ("A", "T", "B")] == ["a", "a", "b"]
    t = SilenceTracker(p.location)
    go_dark_and_return(t, "A", T0 + 77_000)
    go_dark_and_return(t, "T", T0 + 78_000)
    assert [s.serial for s in t.finish()] == ["A", "T"]
    t = SilenceTracker(p.location)
    go_dark_and_return(t, "T", T0 + 77_000)
    go_dark_and_return(t, "B", T0 + 78_000)  # Testing is at a, B elsewhere: upstream
    assert t.finish() == []
