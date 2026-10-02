"""ting-exporter: Whisker Labs Ting sensor data to VictoriaMetrics (design 7.7).

Commands
    serve                    run forever: stream, outbox, push to every store, flight recorder, /metrics (default)
    probe [--seconds N]      sign in, list sensors, stream and print decoded values with delay and path
      --notifications        the account's device fields and notification history, read-only (--raw: JSON)
      --rest                 what the REST values (hazards, conditions, frozen pipe) read, read-only (--raw: JSON)
      --voltage-history      the cloud's voltage history --from T --to T, read-only (so far it answers 403)
    record --seconds N --out DIR    the recorder's format, everything included
    import|replay FILE|DIR...       the live pipeline over recorded frames: --vm-url URL, or --dry-run (1-min rollups)
    mark SITE CONTEXT=VALUE  a context mark, e.g. `mark b source=inverter` (--at T; `source=` ends it; --list)
    repair --from T [--to T] fill the 1-minute rollups vmalert never wrote, e.g. for imported history (--vm-url)
    status                   the running exporter's /readyz: sensors, stores, sign-in (exit 0 ready, 1 not)
    check-config             validate the environment, the password file and the outbox, no network
    rules                    print the vmalert rollup rules generated from the signal registry
    compare-stores [A B]     samples per series name in two stores (default: the push targets; exit 1 if they differ)

No command releases a hub subscription unless TING_RELEASE_OTHERS=true. Configuration comes from
environment variables, see README.md. Times without an offset are UTC.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

import aiohttp

from . import __version__, compare, context, logs, probe, recorder, repair, replay, rest, rules, serve, signals
from .config import Config, ConfigError
from .outbox import orphaned_cursors
from .pipeline import Pipeline
from .vmquery import VmError, VmQuery

COMMANDS = ("serve", "probe", "record", "replay", "import", "mark", "repair", "status", "check-config", "rules",
            "compare-stores")


def _config(need_credentials: bool = True) -> Config:
    try:
        return Config.from_env(need_credentials=need_credentials)
    except ConfigError as err:
        for problem in err.problems:
            print(f"config: {problem}", file=sys.stderr)
        raise SystemExit(2) from None


def _writable(path: Path) -> bool:
    probe_dir = path if path.exists() else path.parent
    return os.access(probe_dir, os.W_OK)


def check_config() -> int:
    try:
        cfg = Config.from_env()
    except ConfigError as err:
        for problem in err.problems:
            print(f"config: {problem}")
        return 2
    problems: list[str] = []
    try:
        if not cfg.password_file.read_text(encoding="utf-8").strip():
            problems.append(f"{cfg.password_file} is empty")
    except OSError as err:
        problems.append(f"cannot read {cfg.password_file}: {err.strerror}")
    except UnicodeDecodeError:  # not the error's text: it quotes the password's bytes
        problems.append(f"{cfg.password_file} is not UTF-8 text: save the password as UTF-8")
    for name, path in (("TING_OUTBOX_DIR", cfg.outbox_dir), ("TING_RECORD_DIR", cfg.record_dir),
                       ("TING_STATE_FILE", cfg.state_file and cfg.state_file.parent)):
        if path is not None and not _writable(path):
            problems.append(f"{name} {path} is not writable")
    if cfg.health_checks:
        try:
            if not cfg.alert_webhook_file.read_text(encoding="utf-8").strip().startswith(("http://", "https://")):
                problems.append(f"TING_ALERT_WEBHOOK_FILE {cfg.alert_webhook_file} holds no http(s) URL")
        except (OSError, UnicodeDecodeError):
            problems.append(f"TING_ALERT_WEBHOOK_FILE {cfg.alert_webhook_file} is not readable (the health checks notify through it)")
    print(repr(cfg))
    print(f"streams: {', '.join(cfg.streamed_serials) or 'every sensor on the account'}")
    for serial in cfg.streamed_serials:
        print(f"  {serial} site={cfg.sites.get(serial, 'unknown (not in TING_SITES)')}")
    print(f"role: {cfg.role} on {cfg.host}; pushes to:")
    for name, url in cfg.vm_targets:
        print(f"  {name} {url}")
    for target in orphaned_cursors(cfg.outbox_dir, [n for n, _ in cfg.vm_targets]):
        print(f"note: the outbox has a cursor for {target}, which is no longer a push target; serve deletes it")
    if cfg.health_checks:
        print(f"health checks (notify after {cfg.health_for:.0f} s): " + ", ".join(f"{n} {u}" for n, u in cfg.health_checks))
    if cfg.release_others:
        print("TING_RELEASE_OTHERS=true: subscribing ends every other client's stream (never with a second exporter)")
    for p in problems:
        print(f"problem: {p}")
    print("ok" if not problems else "not ok")
    return 0 if not problems else 2


async def _mark(cfg: Config, args: argparse.Namespace) -> int:
    store = args.vm_url or cfg.vm_url
    async with aiohttp.ClientSession() as session:
        if args.list:
            try:
                rows = await context.current(VmQuery(session, store), at=time.time(),
                                             outbox_dir=None if args.vm_url else cfg.outbox_dir)
            except VmError as err:
                print(f"{store}: {err}", file=sys.stderr)
                return 2
            for m in rows:
                print(f"site={m.get('site')} serial={m.get('serial')} {m.get('context')}={m.get('value')}")
            if not rows:
                print("no context is set")
            return 0
        if not args.site or not args.assignment:
            print("mark needs SITE CONTEXT=VALUE (or --list)", file=sys.stderr)
            return 2
        serial = cfg.site_serial(args.site)
        if serial is None:
            print(f"site {args.site!r} is not in TING_SITES ({', '.join(sorted(cfg.sites.values())) or 'empty'})", file=sys.stderr)
            return 2
        try:
            key, value = context.parse_assignment(args.assignment)
            at = recorder.parse_when(args.at) if args.at else time.time()
            lines, where = await context.mark(session, store=store, serial=serial, site=args.site, key=key, value=value,
                                              at=at, outbox_dir=None if args.vm_url else cfg.outbox_dir)
        except (context.MarkError, VmError, ValueError) as err:
            print(f"mark: {err}", file=sys.stderr)
            return 2
    for line in lines:
        print(line, end="")
    print(f"written to {where}")
    return 0


async def _repair(cfg: Config, args: argparse.Namespace) -> int:
    targets = {"store": args.vm_url} if args.vm_url else dict(cfg.vm_targets)
    now = time.time()
    start = recorder.parse_when(args.start)
    end = recorder.parse_when(args.end) if args.end else now - repair.SETTLE

    def report(target: str, window: str, filled: int, stale: int) -> None:
        print(f"{target} {window} UTC: filled {filled} minutes" + (f", {stale} computed from partial data" if stale else ""))

    async with aiohttp.ClientSession() as session:
        try:
            total = await repair.repair_range(session, targets, start, end, now, report)
        except VmError as err:
            print(f"repair: {err}", file=sys.stderr)
            return 2
    print(f"filled {total} minutes in total")
    return 0


def status(cfg: Config) -> int:
    """GET the running exporter's /readyz (inside its container: `docker exec ting-exporter python -m ting_exporter status`)."""
    import json
    import urllib.error
    import urllib.request

    url = f"http://127.0.0.1:{cfg.listen_port}/readyz"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            body, ready = resp.read().decode(), True
    except urllib.error.HTTPError as err:  # 503: not every sensor is up; the body says which
        body, ready = err.read().decode(), False
    except OSError as err:
        print(f"{url}: {err}; is the exporter running?", file=sys.stderr)
        return 2
    doc = json.loads(body)
    print(f"{'ready' if ready else 'NOT READY'}  role={doc.get('role')} host={doc.get('host')} sign-in={doc.get('auth_state')}"
          + (f" ({doc['auth_error']})" if doc.get("auth_error") else ""))
    for serial, s in doc.get("sensors", {}).items():
        print(f"  sensor {serial} site={s.get('site')} {'up' if s.get('up') else 'DOWN'} last sample "
              f"{s.get('last_sample_age_seconds')} s ago, delay {s.get('delay_seconds')} s" +
              (f", last error: {s['last_error']}" if s.get("last_error") else ""))
    for name, t in doc.get("targets", {}).items():
        print(f"  store {name} {'up' if t.get('up') else 'FAILING'} behind {t.get('lag_samples')} samples "
              f"({t.get('lag_seconds')} s)")
    outbox = doc.get("outbox", {})
    print(f"  outbox {outbox.get('bytes')} bytes" + ("" if outbox.get("disk_ok", True) else ", DISK FAILING (memory only)"))
    return 0 if ready else 1


def _parser(command: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog=f"ting-exporter {command}", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--log-level", default=None)
    if command == "probe":
        p.add_argument("--seconds", type=float, default=60)
        p.add_argument("--notifications", action="store_true")
        p.add_argument("--rest", action="store_true")
        p.add_argument("--voltage-history", action="store_true")
        p.add_argument("--raw", action="store_true")
        p.add_argument("--from", dest="start")
        p.add_argument("--to", dest="end")
        p.add_argument("--no-release", action="store_true", help="release nothing even with TING_RELEASE_OTHERS=true")
    elif command == "record":
        p.add_argument("--seconds", type=float, default=60)
        p.add_argument("--out", type=Path, default=Path("/data/capture"))
    elif command in ("replay", "import"):
        p.add_argument("files", nargs="+", type=Path)
        p.add_argument("--vm-url", help="the store to push to (default: the first of TING_VM_URLS)")
        p.add_argument("--dry-run", action="store_true", help="print the 1-minute rollups as CSV instead")
        p.add_argument("--speed", type=float, default=0.0, help="pace at N x recorded speed (default: at once)")
        p.add_argument("--from", dest="start", help="only records received at or after this time")
        p.add_argument("--to", dest="end", help="only records received before this time")
    elif command == "mark":
        p.add_argument("site", nargs="?")
        p.add_argument("assignment", nargs="?", help="CONTEXT=VALUE, e.g. source=inverter; CONTEXT= ends it")
        p.add_argument("--at", help="when (default: now)")
        p.add_argument("--vm-url", help="look up and write to this store directly, not through the exporter's outbox")
        p.add_argument("--list", action="store_true", help="the contexts set now")
    elif command == "repair":
        p.add_argument("--from", dest="start", required=True, help="the first minute to look at")
        p.add_argument("--to", dest="end", help="the last (default: 10 minutes ago)")
        p.add_argument("--vm-url", help="one store (default: every push target of TING_VM_URLS)")
    elif command == "compare-stores":
        compare.add_arguments(p)
    return p


def main(argv: list[str] | None = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("--version", "-V"):
        print(__version__)
        return
    if argv and argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return
    command = argv.pop(0) if argv and not argv[0].startswith("-") else "serve"
    if command not in COMMANDS:
        print(f"unknown command {command!r}; one of {', '.join(COMMANDS)}", file=sys.stderr)
        raise SystemExit(2)
    args = _parser(command).parse_intermixed_args(argv)  # files may follow options on every Python
    signals.validate()
    rest.validate()
    logs.setup(args.log_level or os.environ.get("TING_LOG_LEVEL", "INFO"), os.environ.get("TING_LOG_FORMAT", "text"))

    if command == "rules":
        sys.stdout.write(rules.render())
    elif command == "check-config":
        raise SystemExit(check_config())
    elif command == "compare-stores":
        if not (args.a and args.b):  # in the exporter's container: its own two stores
            targets = [url for _, url in _config(need_credentials=False).vm_targets]
            if len(targets) < 2:
                print("compare-stores needs two stores: A and B, or two push targets in TING_VM_URLS", file=sys.stderr)
                raise SystemExit(2)
            args.a, args.b = args.a or targets[0], args.b or targets[1]
        raise SystemExit(compare.run(args))
    elif command == "mark":
        raise SystemExit(asyncio.run(_mark(_config(need_credentials=False), args)))
    elif command == "repair":
        raise SystemExit(asyncio.run(_repair(_config(need_credentials=False), args)))
    elif command == "status":
        raise SystemExit(status(_config(need_credentials=False)))
    elif command in ("replay", "import"):
        cfg = _config(need_credentials=False)
        window = (recorder.parse_when(args.start) if args.start else None, recorder.parse_when(args.end) if args.end else None)
        pipeline = Pipeline(cfg.sites, cfg.timestamp_source)
        paths = recorder.expand(args.files)
        totals = asyncio.run(replay.replay(paths, pipeline, vm_url=None if args.dry_run else (args.vm_url or cfg.vm_url),
                                           dry_run=args.dry_run, speed=args.speed, window=window))
        print(f"{len(paths)} files: {totals}", file=sys.stderr)
        print(replay.summary(pipeline), file=sys.stderr)
    else:
        cfg = _config()
        logs.setup(args.log_level or cfg.log_level, cfg.log_format)
        if command == "probe":
            if args.notifications:
                asyncio.run(probe.notifications(cfg, raw=args.raw))
            elif args.rest:
                asyncio.run(probe.rest_values(cfg, raw=args.raw))
            elif args.voltage_history:
                if not (args.start and args.end):
                    raise SystemExit("probe --voltage-history needs --from and --to")
                asyncio.run(probe.voltage_history(cfg, recorder.parse_when(args.start), recorder.parse_when(args.end), raw=args.raw))
            else:
                asyncio.run(probe.probe(cfg, args.seconds, release=False if args.no_release else None))
        elif command == "record":
            asyncio.run(probe.record(cfg, args.seconds, args.out))
        else:
            raise SystemExit(asyncio.run(serve.serve(cfg)))  # 1: the watchdog stopped it; Docker restarts it


if __name__ == "__main__":
    main()
