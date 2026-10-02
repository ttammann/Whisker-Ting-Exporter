"""Rollup repair (design 6.8, T17): fill the minutes vmalert missed, never touch the ones it wrote."""

import aiohttp
from aiohttp import web

from ting_exporter import rules
from ting_exporter.repair import COUNT_RULE, RollupRepair, runs

from . import fakes

T0 = 1_790_600_040  # a whole minute, seconds


def raw_minutes(vm, serial, site, first_minute, minutes):
    lines = []
    for m in range(minutes):
        for i in range(240):
            ts = (T0 + (first_minute + m) * 60 - 60) * 1000 + 250 + i * 250  # inside (T-60 s, T]
            lines.append(f'ting_voltage_volts{{serial="{serial}",site="{site}"}} {120 + (i % 9) / 10} {ts}')
            if i % 4 == 0:
                lines.append(f'ting_frequency_hertz{{serial="{serial}",site="{site}"}} 60.01 {ts}')
    vm.load("\n".join(lines) + "\n")


def vmalert_wrote(vm, serial, site, minute, count=240):
    vm.load(f'{COUNT_RULE}{{serial="{serial}",site="{site}"}} {count} {(T0 + minute * 60) * 1000}\n'
            f'ting:voltage_volts:min_1m{{serial="{serial}",site="{site}"}} 999 {(T0 + minute * 60) * 1000}\n')


async def test_missing_minutes_are_filled_and_written_ones_are_untouched():
    vm = fakes.FakeVM()
    app = web.Application()
    vm.routes(app)
    runner, base = await fakes.start_app(app)
    raw_minutes(vm, "TNG000001", "a", 0, 40)
    for minute in range(10):  # vmalert wrote the first 10 minutes, then missed 30 (VictoriaMetrics was down)
        vmalert_wrote(vm, "TNG000001", "a", minute)
    vmalert_wrote(vm, "TNG000001", "a", 39, count=100)  # computed from partial data: stale, cannot be fixed
    async with aiohttp.ClientSession() as session:
        repair = RollupRepair(session, {"local": base}, lambda name: 0.0)
        filled = await repair.repair("local", base, now=T0 + 40 * 60 + 600)
    await runner.cleanup()
    assert filled == 29 and repair.repaired["local"] == 29 and repair.stale["local"] == 1
    count = {ts: v for (n, _l, ts), v in vm.samples.items() if n == COUNT_RULE}
    assert len(count) == 40 and all(count[(T0 + m * 60) * 1000] == 240 for m in range(10, 39))
    mins = {ts: v for (n, _l, ts), v in vm.samples.items() if n == "ting:voltage_volts:min_1m"}
    assert mins[(T0 + 5 * 60) * 1000] == 999 and mins[(T0 + 20 * 60) * 1000] == 120.0  # untouched; filled
    names = {n for (n, _l, _ts) in vm.samples if n.startswith("ting:")}
    assert "ting:frequency_hertz:avg_1m" in names and "ting:thd_ratio:avg_1m" not in names  # no THD raw: no point
    assert any('{serial="TNG000001",site="a"}' in q for q in vm.queries)  # narrowed to the series that misses them


async def test_a_target_with_a_backlog_is_skipped():
    calls = []

    class Repair(RollupRepair):
        async def repair(self, name, url, now):
            calls.append(name)
            return 0

    import asyncio

    from ting_exporter.clock import ScaledClock

    repair = Repair(None, {"local": "http://x", "peer": "http://y"}, lambda name: 120.0 if name == "peer" else 0.0,
                    interval=60, clock=ScaledClock(1000.0))
    stop = asyncio.Event()
    task = asyncio.create_task(repair.run(stop))
    await asyncio.sleep(0.15)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert calls and set(calls) == {"local"} and repair.runs[("peer", "skipped")] >= 1


def test_runs_and_expressions():
    assert runs([60, 120, 180, 300, 360, 600]) == [(60, 180), (300, 360), (600, 600)]
    exprs = [rules.expression(s, r, 'serial="X"') for s, r in rules.every_rule()]
    assert 'round(avg_over_time(ting_voltage_volts{serial="X"}[1m]), 0.0001)' in exprs
    assert len(exprs) == sum(len(s.rollups) for s, _ in {(s, None) for s, _r in rules.every_rule()})


async def test_repair_range_fills_imported_history_a_day_at_a_time():
    """`ting-exporter repair --from`: history imported days ago (outside the 48 h of the periodic task), no cap."""
    from ting_exporter.repair import DAY, repair_range

    vm = fakes.FakeVM()
    app = web.Application()
    vm.routes(app)
    runner, base = await fakes.start_app(app)
    raw_minutes(vm, "TNG000002", "b", 0, 3)
    raw_minutes(vm, "TNG000002", "b", 1440 + 100, 2)  # the next day
    reports = []
    async with aiohttp.ClientSession() as session:
        total = await repair_range(session, {"local": base}, T0 - 60, T0 + 2 * DAY, T0 + 30 * DAY,
                                   lambda *a: reports.append(a))
    await runner.cleanup()
    assert total == 5 and [r[2] for r in reports] == [3, 2, 0]  # two days and a minute: three passes
    assert vm.count(COUNT_RULE) == 5
