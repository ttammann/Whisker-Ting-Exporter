"""Pushers (design 7.2, 6.1): retry with the cursor kept, refused batches to rejected/, catch-up batches."""

import asyncio
import gzip

import aiohttp
from aiohttp import web

from ting_exporter.clock import ScaledClock
from ting_exporter.outbox import Outbox
from ting_exporter.pusher import CATCHUP_BATCH, LIVE_BATCH, OK, REJECT, RETRY, Pusher, VmClient

from . import fakes


def line(i):
    return f'ting_voltage_volts{{serial="TNG000001",site="a"}} 120 {1790550000000 + i * 250}\n'


async def run_until(pusher, stop, cond, timeout=5.0):
    task = asyncio.create_task(pusher.run(stop))
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not cond() and loop.time() < end:
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 5)


async def test_a_failing_store_keeps_the_cursor_and_catches_up(tmp_path):
    vm = fakes.FakeVM()
    app = web.Application()
    vm.routes(app)
    runner, base = await fakes.start_app(app)
    box = Outbox(tmp_path / "o", ["local"])
    box.open()
    box.append([line(i) for i in range(50)])
    await box.flush()
    vm.mode = "500"
    async with aiohttp.ClientSession() as session:
        pusher = Pusher("local", VmClient(session, base), box, clock=ScaledClock(100.0))
        stop = asyncio.Event()
        task = asyncio.create_task(pusher.run(stop))
        await asyncio.sleep(0.3)
        assert not pusher.up and box.lag("local")[0] == 50 and vm.received == 0
        vm.mode = "ok"
        loop = asyncio.get_running_loop()
        end = loop.time() + 5
        while box.lag("local")[0] and loop.time() < end:
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, 5)
    await runner.cleanup()
    assert pusher.up and vm.count("ting_voltage_volts") == 50 and pusher.results["ok"] == 50
    assert pusher.client.requests["500"] >= 2 and pusher.client.requests["204"] >= 1


async def test_a_refused_batch_goes_to_rejected_and_the_cursor_moves_on(tmp_path):
    seen = []

    async def handle(request):
        seen.append(await request.read())
        return web.Response(status=400, body=b"\xff\xfe cannot parse")  # not UTF-8: still a response

    app = web.Application()
    app.router.add_post("/api/v1/import/prometheus", handle)
    runner, base = await fakes.start_app(app)
    box = Outbox(tmp_path / "o", ["local"])
    box.open()
    box.append([line(i) for i in range(5)])
    await box.flush()
    async with aiohttp.ClientSession() as session:
        pusher = Pusher("local", VmClient(session, base), box, clock=ScaledClock(100.0))
        await run_until(pusher, asyncio.Event(), lambda: box.lag("local")[0] == 0)
    await runner.cleanup()
    assert pusher.results["rejected"] == 5 and len(seen) == 1
    [kept] = list((tmp_path / "o" / "rejected").iterdir())
    assert gzip.decompress(kept.read_bytes()).decode() == "".join(line(i) for i in range(5))


async def test_catch_up_uses_big_batches(tmp_path):
    sizes = []

    class Client:
        requests, duration = {}, None

        async def push(self, lines):
            sizes.append(len(lines))
            return OK

    box = Outbox(tmp_path / "o", ["local"])
    box.open()
    box.append([line(i) for i in range(CATCHUP_BATCH + 3 * LIVE_BATCH)])
    await box.flush()
    pusher = Pusher("local", Client(), box, clock=ScaledClock(1000.0))
    await run_until(pusher, asyncio.Event(), lambda: box.lag("local")[0] == 0)
    assert sizes == [CATCHUP_BATCH, 3 * LIVE_BATCH]  # more than a live batch behind: catch-up batches


async def test_the_client_never_raises(tmp_path):
    async with aiohttp.ClientSession() as session:
        client = VmClient(session, "http://127.0.0.1:9", timeout=1.0)  # nothing listens there
        assert await client.push([line(0)]) == RETRY
    assert client.requests["error"] == 1 and {OK, RETRY, REJECT} == {"ok", "retry", "reject"}
