# ting-exporter 3

Whisker Labs Ting sensor data to VictoriaMetrics: every 0.25 s sample at the sensor's own timestamp, the outages
and alerts Ting reports, power cuts Ting does not report, Ting's hazard and weather values, and the exporter's
own health. Two exporters at two sites run active-active and write both sites' stores, so one site's internet,
host or exporter outage leaves no hole.

The design, with every decision and why, is [docs/design.md](docs/design.md). This file is how to run it.

```
                    Whisker cloud (Cognito, REST, SignalR hub)
                     |                                   |
        ting-exporter (primary, site A)       ting-exporter (secondary, site B)
          pipeline -> outbox -> pushers          pipeline -> outbox -> pushers
              |  \______________  ______________/   |
              v                 \/                  v
        VictoriaMetrics A <----/  \----> VictoriaMetrics B   (each exporter writes both)
        vmalert (rollups, alerts)        vmalert (rollups, alerts) -> Alertmanager -> Home Assistant
              \__ health checks (both exporters) ------------------------------^ (direct, if the chain is down)
```

## What it stores

Stream signals (design 5.2), labels `serial` and `site`, at device time:

| Metric | What | Stored |
|---|---|---|
| `ting_voltage_volts` | RMS voltage per 0.25 s sample | every sample |
| `ting_hifi` | Ting's Hi-Fi value (AveragePeaksMax), no documented unit | every sample |
| `ting_frequency_hertz` | line frequency per sample | every sample |
| `ting_thd_ratio`, `ting_thd_min_ratio`, `ting_thd_max_ratio` | THD as a ratio (0.03 = 3 %) | on change, 60 s heartbeat |
| `ting_voltage_rolling_high_volts`, `ting_voltage_rolling_low_volts` | the sensor's own long-window high and low (not per-sample extremes) | on change, 60 s heartbeat |

1-minute rollups by vmalert, `ting:<metric>:<agg>_1m` (min, max, avg, and `count` for voltage), generated from the
registry (`ting-exporter rules`). Minutes vmalert missed are filled by the exporter's rollup repair.

Derived series: `ting_notification` (one sample per Ting notification), `ting_power_outage` (1 at the site, 2
community-wide, every minute an outage Ting reported lasts), `ting_power_cut` and `ting_stream_gap` (inferred:
a silence of 60 s or more that ended in a sensor restart, or without one), `ting_context` (context marks).

Ting's REST values, every 5 minutes: `ting_hazard_state`, `ting_hazard_efh_level`, `ting_hazard_ufh_level`,
`ting_fire_detected`, `ting_frozen_pipe_detected`, `ting_frozen_pipe_level`, `ting_outdoor_temperature_celsius`,
`ting_outage_risk`. The field names come from the community integration, checked against a real account's replies;
`probe --rest` shows what your account answers.

The exporter's own metrics are scraped from `/metrics` (design 5.6), for example `ting_stream_up`,
`ting_push_lag_seconds{target}`, `ting_inferred_silences_total{kind}`, `ting_health_check_up{check}`.

## Commands

```
ting-exporter serve                         run (default)
ting-exporter probe [--seconds N]           stream and print decoded values, delay and live/buffered path
ting-exporter probe --notifications [--raw] the account's notification history, read-only
ting-exporter probe --rest [--raw]          what the REST values read, read-only
ting-exporter record --seconds N --out DIR  the flight recorder's format
ting-exporter import DIR|FILE... --vm-url URL | --dry-run [--from T] [--to T]
ting-exporter mark SITE CONTEXT=VALUE [--at T] [--vm-url URL]    | mark --list
ting-exporter repair --from T [--to T] [--vm-url URL]   fill rollups vmalert never wrote (imported history)
ting-exporter status                        the running exporter's sensors, stores and sign-in
ting-exporter check-config                  validate the environment, no network
ting-exporter rules                         the vmalert rollup rules
ting-exporter compare-stores [A B] [--lookback 120d] [--at T]   default: this exporter's two stores
```

No command releases a hub subscription (that would end every other client's stream, the other exporter's too)
unless `TING_RELEASE_OTHERS=true`. Times without an offset are UTC.

**Context marks.** For comparing power sources: `docker exec ting-exporter python -m ting_exporter mark b
source=inverter` writes `ting_context{site="b",context="source",value="inverter"}` = 1 from now on (and 0 for the
value it replaces), through the running exporter's outbox to both stores. `mark b source=` ends it. The
dashboard's "Power source" row shows THD and Hi-Fi by source.

## Configuration

Environment variables (design 7.6). The ones a deploy sets come from the host's `site.env` (the overlay below).

| Variable | Default | Meaning |
|---|---|---|
| `TING_USERNAME` | required | the Ting app e-mail (personal data: never logged) |
| `TING_PASSWORD_FILE` | `/run/secrets/ting/ting_password` | the password, in its own mounted directory |
| `TING_SITES` | | `serial=site,...`: labels, and the sensors to stream; one sensor per site |
| `TING_SERIALS` | | overrides the list; neither set: every sensor on the account |
| `TING_VM_URLS` | `local=http://victoriametrics:8428` | push targets `name=url,...`; the first is the local store |
| `TING_ROLE`, `TING_HOST` | `primary`, the host name | labels, logs, alerts |
| `TING_HEALTH_CHECKS` | | `name=url,...` checked every minute; a failure for `TING_HEALTH_FOR_SECONDS` (300) goes straight to the webhook |
| `TING_ALERT_WEBHOOK_FILE` | `/run/secrets/alert/alert_webhook_url` | the Home Assistant webhook URL (a secret) |
| `TING_OUTBOX_DIR`, `TING_OUTBOX_MAX_BYTES` | `/data/outbox`, 256 MiB | the durable log; about two weeks for a store that is down |
| `TING_OUTBOX_REPLAY_NEW` | `false` | a new push target starts at the start of the log, not its end |
| `TING_STATE_FILE` | `/data/state.json` | last samples, so a restart during a cut still infers it |
| `TING_RECORD_DIR`, `TING_RECORD_RETENTION_DAYS` | `/data/raw`, 90 | the flight recorder (`off` disables) |
| `TING_PUSH_INTERVAL_SECONDS` | 5 | outbox flush and push interval |
| `TING_NOTIFICATIONS_INTERVAL_SECONDS`, `TING_REST_INTERVAL_SECONDS`, `TING_REPAIR_INTERVAL_SECONDS` | 60, 300, 300 | `0` disables |
| `TING_STALE_SECONDS` | 60 | reconnect after this long without voltage |
| `TING_TIMESTAMP_SOURCE` | `device` | or `arrival` (diagnosis) |
| `TING_AUTH_HOLD_SECONDS` | 21600 | no sign-in for this long after a rejected password (or until the file changes) |
| `TING_RELEASE_OTHERS` | `false` | never with a second exporter |
| `TING_LISTEN`, `TING_LOG_LEVEL`, `TING_LOG_FORMAT` | `0.0.0.0:9786`, `INFO`, `text` | |

Variables of earlier versions (`TING_VM_URL`, `TING_SPOOL_DIR`, `TING_QUEUE_MAX_SAMPLES`) are refused with a message.

## Deploy

**[docs/install.md](docs/install.md)** is the first install, step by step, for two hosts. In short:

- The repository is public; everything about the real hosts lives in a private overlay in `./overlay`
  (gitignored, its own git repository, mirrored to a readable copy). It holds values only: per host a
  `site.env`, the dashboard's sites, extra alert rules (design 13.2). `python3 tools/overlay.py check`
  validates it and the host pair's invariants; a leak guard on every commit and push keeps it out of here.
- `python3 tools/overlay.py bundle HOST` makes a tarball of a clean commit plus that host's values, and prints
  the commands to apply it. It connects to nothing; you run them.
- On each host the bundle lives in `~/stack/ting`, and the host's own `~/stack/docker-compose.yml` includes it:
  ```yaml
  include:
    - path: [ting/deploy/compose.yml, ting/deploy/compose.notify.yml]
      env_file: [ting/site.env, secrets/ting.env]
  ```
  (`compose.notify.yml`, Alertmanager, on the notifying host only.)
- Secrets stay on the host: `~/stack/secrets/ting.env` (`VM_ADMIN_KEY`, `TING_USERNAME`),
  `~/stack/secrets/ting/ting_password`, `~/stack/secrets/alert/alert_webhook_url`. The data lives on a data
  disk (`DATA_ROOT`) in `ting/`, `victoriametrics/` and `alertmanager/`, next to the guard file `.ssd-mounted`.
  Every mount has `create_host_path: false`: without the disk, the containers do not start, and nothing is
  written to the root disk.
- Updates: a new bundle, the dry run, build and start. History from a previous install: `import` for
  flight-recorder files, vmctl for a VictoriaMetrics data directory, then `repair` (docs/install.md, step 11).

## Development

```bash
python3.13 -m venv .venv && .venv/bin/pip install -e ".[test]" && .venv/bin/pytest
```

The suite is offline and takes about 40 s. An optional part, T22, runs against a real VictoriaMetrics, vmalert and
vmctl when they are there: either the release images (`TING_VM_DOCKER=v1.152.0`, with Docker Desktop on a
Mac: vmalert reaches the stores through `host.docker.internal`, which a Linux Docker Engine does not route to the
host's 127.0.0.1; `TING_EXPORTER_IMAGE=ting-exporter:3.0.0` adds the built image to the import rehearsal) or the
release binaries (`TING_VM_BIN_DIR=~/vm/bin`, holding `victoria-metrics-prod` and `vmalert-prod`).

```bash
TING_VM_DOCKER=v1.152.0 TING_EXPORTER_IMAGE=ting-exporter:3.0.0 .venv/bin/pytest tests/test_realvm.py
```

The tests run against local stand-ins: FakeCognito (a real SRP-6a server, cross-checked against pycognito), the
REST API, FakeHub (replays recordings faster than real time; refuses, goes silent, closes, drops, serves two
clients), and FakeVM (stores like VictoriaMetrics' dedup and answers the few queries the exporter sends).
`tests/fixtures/capture/` holds six synthetic recordings in the flight recorder's format, written by
`tools/make_fixtures.py` (two simulated sensors, seeded, delivered the way the hub delivers: live and buffered
paths, catch-up, reordering, a reconnect, a gap and a power cut). The golden files come from
`tools/reference_rollups.py`, which restates the rules without importing the package:

```bash
.venv/bin/python tools/make_fixtures.py tests/fixtures/capture
.venv/bin/python tools/reference_rollups.py --sites TNG000001=a,TNG000002=b tests/fixtures/capture/[a-e]-*.jsonl.gz \
  > tests/fixtures/golden-rollups.csv 2> tests/fixtures/golden-pushed.txt && sed -i '' 's/^pushed //' tests/fixtures/golden-pushed.txt
.venv/bin/ting-exporter rules > deploy/vmalert/ting-rollups.yml
.venv/bin/python tools/make_dashboard.py > deploy/grafana/ting-dashboard.json
```

## Credits

The Cognito SRP flow, the SignalR hub protocol and the REST endpoints follow the MIT-licensed community
integrations (simplytoast1, jasonjhofmann, Underzenith85 `ha-whisker-ting`, coffee-the-dev `ting-hass`). Whisker
Labs documents none of it; it can change without notice. MIT licence.
