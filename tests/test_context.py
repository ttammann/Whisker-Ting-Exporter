"""Context marks (design 0.4): `mark b source=inverter` writes ting_context change points, looked up in the store."""

import pytest
import aiohttp
from aiohttp import web

from ting_exporter import context
from ting_exporter.vmquery import VmQuery

from . import fakes

T = 1_790_600_000.0


def test_parse_assignment():
    assert context.parse_assignment("source=inverter") == ("source", "inverter")
    assert context.parse_assignment("source=") == ("source", None)
    for bad in ("source", "Source=x", "source=two words", "=x", 'source=a"b'):
        with pytest.raises(context.MarkError):
            context.parse_assignment(bad)


async def test_marks_replace_each_other_in_the_store():
    vm = fakes.FakeVM()
    app = web.Application()
    vm.routes(app)
    runner, base = await fakes.start_app(app)
    kw = dict(store=base, serial="TNG000002", site="b", key="source", outbox_dir=None)
    async with aiohttp.ClientSession() as session:
        first, _ = await context.mark(session, value="mains", at=T, **kw)
        second, _ = await context.mark(session, value="inverter", at=T + 3600, **kw)
        with pytest.raises(context.MarkError, match="already set"):
            await context.mark(session, value="inverter", at=T + 3700, **kw)
        now = await context.active(VmQuery(session, base), at=T + 3700)
        ended, _ = await context.mark(session, value=None, at=T + 7200, **kw)
        later = await context.active(VmQuery(session, base), at=T + 7300)
        before = await context.active(VmQuery(session, base), at=T + 60)
    await runner.cleanup()
    assert first == ['ting_context{context="source",serial="TNG000002",site="b",value="mains"} 1 1790600000000\n']
    assert second == ['ting_context{context="source",serial="TNG000002",site="b",value="mains"} 0 1790603600000\n',
                      'ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 1 1790603600000\n']
    assert [m["value"] for m in now] == ["inverter"] and later == [] and [m["value"] for m in before] == ["mains"]
    assert ended == ['ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 0 1790607200000\n']


def test_the_inbox_must_exist(tmp_path):
    with pytest.raises(context.MarkError, match="does not exist"):
        context.write_inbox(tmp_path / "nowhere", ["x"])
    (tmp_path / "inbox").mkdir()
    path = context.write_inbox(tmp_path, ['ting_context{a="b"} 1 1\n'])
    assert path.suffix == ".prom" and path.read_text() == 'ting_context{a="b"} 1 1\n'


async def test_the_lookup_asks_for_the_newest_samples_too():
    """VictoriaMetrics hides the last 30 s from queries (-search.latencyOffset); without the override the lookup misses
    a mark written a moment ago (T22 checks this against the real one)."""
    vm = fakes.FakeVM()
    app = web.Application()
    vm.routes(app)
    runner, base = await fakes.start_app(app)
    async with aiohttp.ClientSession() as session:
        await context.mark(session, store=base, serial="TNG000002", site="b", key="source", value="mains", at=T,
                           outbox_dir=None)
        await context.active(VmQuery(session, base), at=T)
    await runner.cleanup()
    assert [p.get("latency_offset") for p in vm.query_params] == ["1ms", "1ms"]


def test_in_inbox_reads_each_series_up_to_the_time(tmp_path):
    inbox = tmp_path / "inbox"
    inbox.mkdir()
    (inbox / "mark-1-1-0.prom").write_text(
        'ting_context{context="source",serial="TNG000002",site="b",value="mains"} 1 1000\n'
        'ting_context{context="source",serial="TNG000001",site="a",value="solar"} 1 1000\n'
        'ting_voltage_volts{serial="TNG000001",site="a"} 120 1000\n')
    (inbox / "mark-2-1-0.prom").write_text(
        'ting_context{context="source",serial="TNG000002",site="b",value="mains"} 0 2000\n'
        'ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 1 2000\n'
        'ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 0 2000\n')  # VM keeps the 1
    (inbox / "mark-3-1-0.tmp").write_text(  # still being written
        'ting_context{context="source",serial="TNG000002",site="b",value="grid"} 1 1500\n')

    def values(at_ms):
        return sorted((labels["site"], labels["value"], v) for labels, v in context.in_inbox(tmp_path, at_ms))

    assert values(2000) == [("a", "solar", 1.0), ("b", "inverter", 1.0), ("b", "mains", 0.0)]
    assert values(1999) == [("a", "solar", 1.0), ("b", "mains", 1.0)]
    assert context.in_inbox(tmp_path / "nowhere", 2000) == []


async def test_the_list_shows_the_store_and_the_inbox(tmp_path):
    vm = fakes.FakeVM()
    app = web.Application()
    vm.routes(app)
    runner, base = await fakes.start_app(app)
    (tmp_path / "inbox").mkdir()
    async with aiohttp.ClientSession() as session:
        await context.mark(session, store=base, serial="TNG000001", site="a", key="source", value="grid", at=T,
                           outbox_dir=None)  # in the store
        await context.mark(session, store=base, serial="TNG000002", site="b", key="source", value="mains", at=T,
                           outbox_dir=tmp_path)  # in the inbox
        await context.mark(session, store=base, serial="TNG000001", site="a", key="source", value=None, at=T + 1,
                           outbox_dir=tmp_path)  # ends a's, in the inbox
        listed = await context.current(VmQuery(session, base), at=T + 2, outbox_dir=tmp_path)
        store_only = await context.current(VmQuery(session, base), at=T + 2)
    await runner.cleanup()
    assert [(m["site"], m["value"]) for m in listed] == [("b", "mains")]
    assert [(m["site"], m["value"]) for m in store_only] == [("a", "grid")]


async def test_a_mark_still_in_the_inbox_counts(tmp_path):
    """The exporter takes the inbox at its next push; until then the store does not have the mark, and the next mark
    must still end it."""
    vm = fakes.FakeVM()
    app = web.Application()
    vm.routes(app)
    runner, base = await fakes.start_app(app)
    (tmp_path / "inbox").mkdir()
    kw = dict(store=base, serial="TNG000002", site="b", key="source", outbox_dir=tmp_path)
    async with aiohttp.ClientSession() as session:
        await context.mark(session, value="mains", at=T, **kw)
        with pytest.raises(context.MarkError, match="already set"):
            await context.mark(session, value="mains", at=T + 1, **kw)
        second, _ = await context.mark(session, value="inverter", at=T + 2, **kw)
        ended, _ = await context.mark(session, value=None, at=T + 3, **kw)
        with pytest.raises(context.MarkError, match="no source is set"):
            await context.mark(session, value=None, at=T + 4, **kw)
    await runner.cleanup()
    assert vm.received == 0 and len(list((tmp_path / "inbox").glob("*.prom"))) == 3
    assert second == ['ting_context{context="source",serial="TNG000002",site="b",value="mains"} 0 1790600002000\n',
                      'ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 1 1790600002000\n']
    assert ended == ['ting_context{context="source",serial="TNG000002",site="b",value="inverter"} 0 1790600003000\n']
