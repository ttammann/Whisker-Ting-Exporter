# First install

Two hosts at two sites, each running VictoriaMetrics, vmalert and a ting-exporter; one of them also runs
Alertmanager. Each exporter streams every sensor and writes both stores. The examples use the names of
`overlay.example`: `host-a` (primary, 192.0.2.10) and `host-b` (secondary, notifies, 198.51.100.10).

Every code block can be pasted as a whole: there are no comments or placeholders inside them. On a host, the
blocks read that host's values from `~/stack/ting/site.env`, which the bundle puts there (step 3).

## What you need

- **Two Linux hosts** with Docker Engine and Docker Compose 2.20 or later (for `include`). The ssh user should
  have the uid that the containers run as (`TING_UID`, usually 1000).
- **A data disk on each host**, mounted for example at `/mnt/data`, with an empty file `.ssd-mounted` at
  its root (the mount guard: without the mounted disk, the containers refuse to start), and at least 30 GB free
  (VictoriaMetrics stops accepting data below 10 GB free; the alerts warn at 25 GB). Five years of two sensors
  take about 5 GB.
- **A fixed LAN address on each host** that the other host reaches on port 8428 (the same LAN, or a site-to-site
  tunnel). Never forward that port from the internet: reads and writes are not authenticated.
- **Synchronized time** on both hosts (NTP): device timestamps are checked against it.
- **Home Assistant** for the alerts (optional, but without it nothing reaches your phone).
- **The Ting account** (the phone app's e-mail and password) and each sensor's serial number.
- **A workstation** with git, Python 3.11 or later, and ssh access to both hosts.

## 1. Workstation: the overlay

Clone this repository and work in its directory. Your hosts' values live in a private overlay, `./overlay`, its
own git repository, never committed here. Make it, with a readable copy elsewhere, and install the leak guard:

```bash
python3 tools/overlay.py init --mirror ~/ting-overlay-mirror && python3 tools/overlay.py install-hooks
```

Then edit, in `overlay/`:

- `overlay.toml`: one `[hosts.NAME]` per host with its `role` (`primary`, `secondary`), `address` (its LAN
  address), `ssh` (user@address, only printed) and `notify` (true on exactly one host).
- `hosts/NAME/site.env` for each host (rename the example directories to your host names):

  | Value | Meaning |
  |---|---|
  | `TING_HOST`, `TING_ROLE`, `VM_LISTEN` | the host's name, role and LAN address, as in `overlay.toml` |
  | `TING_SITES` | `serial=site,...`, the same on both hosts: one sensor per site, short site names |
  | `TING_VM_URLS` | `local=http://victoriametrics:8428,peer=http://OTHER-ADDRESS:8428` |
  | `TING_HEALTH_CHECKS` | `vm=http://victoriametrics:8428/health,vmalert=http://vmalert:8880/health,peer-vm=http://OTHER-ADDRESS:8428/health`, plus `alertmanager=http://alertmanager:9093/-/healthy` on the notifying host |
  | `VMALERT_NOTIFIER` | `-notifier.url=http://alertmanager:9093` on the notifying host, `-notifier.blackhole` on the other |
  | `DATA_ROOT`, `TING_UID`, `TING_GID` | the data disk's mount point; the uid and gid that own the data |
  | `VM_VERSION`, `ALERTMANAGER_VERSION` | exact releases, e.g. `v1.152.0` and `v0.28.1` |
  | `PYTHON_IMAGE` | `python:3.13-slim@sha256:...`, pinned by digest (`docker buildx imagetools inspect python:3.13-slim`) |

- `site.toml`: a name and a colour per site, for the dashboard.

Do not know the serials? `probe --notifications` lists the account's sensors; it runs on the workstation with
the password in a file of your own (`pip install -e .` first):

```bash
printf 'Ting app e-mail: ' && read -r TING_USERNAME && printf 'password file: ' && read -r TING_PASSWORD_FILE && export TING_USERNAME TING_PASSWORD_FILE && ting-exporter probe --notifications
```

Check the values and the pair's invariants, then commit the overlay (the bundle records which commit it used):

```bash
python3 tools/overlay.py check && git -C overlay add -A && git -C overlay commit -m "my hosts"
```

## 2. Workstation: the dashboard

```bash
python3 tools/overlay.py render
```

It writes `overlay/build/ting-dashboard.json` for step 10.

## 3. Bundle and copy (one host at a time, the one you can reach physically first)

```bash
python3 tools/overlay.py bundle host-a
```

It needs a clean commit of this repository, connects to nothing, and prints the commands for the rest of the
deploy. Run its first two blocks now: the copy (on the workstation) and the unpack (on the host, into
`~/stack/ting`). If the host has no Docker Compose project in `~/stack` yet, make one first:

```bash
mkdir -p ~/stack && touch ~/stack/docker-compose.yml
```

## 4. Host: check it (read-only)

On the host, load its values into the shell. Do this in every new ssh session before the following steps:

```bash
cd ~/stack/ting && set -a && . ./site.env && set +a && cd ~/stack && echo "$TING_HOST $DATA_ROOT $VM_LISTEN"
```

```bash
docker compose version; timedatectl show -p NTPSynchronized --value; findmnt "$DATA_ROOT"; ls -la "$DATA_ROOT/.ssd-mounted"; ip -4 -br addr | grep -w "$VM_LISTEN"; id -u; id -g
```

Expect a Compose version of 2.20 or later, `yes` for the time, the data disk mounted with its guard file,
`VM_LISTEN` on an interface, and `id` equal to `TING_UID` and `TING_GID`. VictoriaMetrics binds that one
address, so Docker must start only once it is up; the network manager's wait-online service does that:

```bash
systemctl is-enabled NetworkManager-wait-online.service systemd-networkd-wait-online.service 2>/dev/null
```

The one your host uses must say `enabled` (`sudo systemctl enable` it otherwise).

## 5. Host: directories and secrets

The data directories (the containers never create them):

```bash
sudo install -d -o "$TING_UID" -g "$TING_GID" -m 750 "$DATA_ROOT/victoriametrics" "$DATA_ROOT/ting"
```

On the notifying host only, also Alertmanager's:

```bash
sudo install -d -o "$TING_UID" -g "$TING_GID" -m 750 "$DATA_ROOT/alertmanager"
```

The secrets: the account e-mail and a new admin key for VictoriaMetrics's admin endpoints, and the password in
a directory of its own (a mounted single file would not see a password an editor rewrote):

```bash
mkdir -p -m 700 ~/stack/secrets/ting ~/stack/secrets/alert && printf 'Ting app e-mail: ' && read -r U && printf 'Ting password: ' && read -rs P && echo && printf 'TING_USERNAME=%s\nVM_ADMIN_KEY=%s\n' "$U" "$(openssl rand -hex 16)" > ~/stack/secrets/ting.env && printf '%s' "$P" > ~/stack/secrets/ting/ting_password && chmod 600 ~/stack/secrets/ting.env ~/stack/secrets/ting/ting_password; unset U P
```

## 6. Home Assistant: the webhook

Alerts reach your phone through a Home Assistant webhook: Alertmanager's alerts on the notifying host, and
both exporters' health checks (they post to the Home Assistant whose URL each host holds). For each Home
Assistant you use:

1. A long random webhook id:
   ```bash
   openssl rand -hex 24
   ```
2. In Home Assistant: `ting_alert_webhook_id: THE-ID` in `secrets.yaml`, the automation from
   `deploy/homeassistant/ting-alerts-automation.yaml` in `automations.yaml` (replace `notify.notify` with your
   phone's notifier), then reload the automations.
3. On the host, the URL, e.g. `http://192.0.2.10:8123/api/webhook/THE-ID`:
   ```bash
   printf 'webhook URL: ' && read -r W && printf '%s\n' "$W" > ~/stack/secrets/alert/alert_webhook_url && chmod 600 ~/stack/secrets/alert/alert_webhook_url; unset W
   ```
4. A test message to your phone:
   ```bash
   curl -s -X POST -H 'Content-Type: application/json' "$(cat ~/stack/secrets/alert/alert_webhook_url)" -d '{"alerts":[{"status":"firing","labels":{"alertname":"TingWebhookTest","severity":"info"},"annotations":{"summary":"webhook test"},"startsAt":"2026-01-01T00:00:00Z"}]}'
   ```

## 7. Host: include, dry run, build

Add the include block the bundle printed at the top level of `~/stack/docker-compose.yml`. Then the dry run;
only the Ting containers may appear (victoriametrics, vmalert, ting-exporter, and alertmanager on the notifying
host):

```bash
cd ~/stack && docker compose config --quiet && docker compose up -d --dry-run 2>&1 | tail -20
```

On the notifying host, Alertmanager's configuration (must say `SUCCESS`):

```bash
docker run --rm -v ~/stack/ting/deploy/alertmanager:/etc/alertmanager:ro -v ~/stack/secrets/alert:/run/secrets/alert:ro --entrypoint amtool "prom/alertmanager:$ALERTMANAGER_VERSION" check-config /etc/alertmanager/alertmanager.yml
```

Build the exporter and check its configuration (must end in `ok`):

```bash
cd ~/stack && docker compose build ting-exporter && docker compose run --rm --no-deps ting-exporter check-config
```

Try the stream for a minute (one sign-in; it lists the sensors and prints a line per sensor per second, with
the delivery delay: about 0.55 s on Ting's live path, 4 to 8 s buffered):

```bash
cd ~/stack && docker compose run --rm --no-deps ting-exporter probe --seconds 60
```

## 8. Host: start

On the notifying host:

```bash
cd ~/stack && docker compose up -d victoriametrics alertmanager vmalert ting-exporter
```

On the other host:

```bash
cd ~/stack && docker compose up -d victoriametrics vmalert ting-exporter
```

After three minutes, the exporter's view (every sensor `up`, the local store `up`; the peer's store fails until
the other host is installed):

```bash
docker exec ting-exporter python -m ting_exporter status
```

The store: about 240 samples a minute per sensor in the rollups, and its admin endpoints locked (`401`):

```bash
curl -s "http://$VM_LISTEN:8428/api/v1/query" --data-urlencode 'query=ting:voltage_volts:count_1m'; echo; curl -s -o /dev/null -w '%{http_code}\n' "http://$VM_LISTEN:8428/flags"
```

## 9. The second host

Steps 3 to 8 again, for `host-b`. Then on either host, the two stores must hold the same pushed series (it
counts up to 10 minutes ago; right after the second install, give it 15 minutes):

```bash
docker exec ting-exporter python -m ting_exporter compare-stores
```

Check the alert path once: on the notifying host, stop vmalert for six minutes. Its exporter's health check
reports `TingHealthCheckFailing` to your phone, then the resolution:

```bash
docker stop vmalert && sleep 400 && docker start vmalert
```

## 10. Grafana

Add a Prometheus-type data source per store, with the URL `http://VM_LISTEN:8428` of each host (e.g.
`http://192.0.2.10:8428`), and import `overlay/build/ting-dashboard.json`, choosing one of them. Your own notes:
Ctrl/Cmd+click on a panel.

## 11. Optional: history from elsewhere

**Flight-recorder files** (this exporter's `raw/` directory, a `record` capture, or a previous version's): the
live pipeline replays them with their recorded arrival times, into one store per run. With the files in
`/path/to/raw`:

```bash
cd ~/stack && docker compose run --rm --no-deps -v /path/to/raw:/import:ro ting-exporter import /import
```

`import` writes to the first push target (the local store); add `--vm-url http://OTHER-ADDRESS:8428` for the
peer's.

**Another VictoriaMetrics data directory** (a previous install's): serve it with a temporary VictoriaMetrics on
the compose network, then copy the pushed series and rollups (not the old self-metrics and alerts) with vmctl
into every push target of `TING_VM_URLS`, the local store and the peer's, so the two stay the same:

```bash
docker run -d --name vm-old --network stack_default --user "$TING_UID:$TING_GID" -v /path/to/old/victoriametrics:/storage "victoriametrics/victoria-metrics:$VM_VERSION" -storageDataPath=/storage -retentionPeriod=100y
```

```bash
for target in $(printf '%s' "$TING_VM_URLS" | tr ',' ' '); do echo "to ${target%%=*}" && docker run --rm --network stack_default "victoriametrics/vmctl:$VM_VERSION" vm-native -s --vm-native-src-addr=http://vm-old:8428 --vm-native-dst-addr="${target#*=}" --vm-native-filter-match='{__name__=~"ting_.*|ting:.*",job=""}' --vm-native-filter-time-start=2020-01-01T00:00:00Z || break; done
```

```bash
docker rm -f vm-old
```

`stack_default` is the network of the project in `~/stack` (`docker network ls` shows it).

**Then the rollups** for the imported time: vmalert never evaluates the past, so the repair fills every minute
that has raw samples and no rollup, in every push target:

```bash
docker exec ting-exporter python -m ting_exporter repair --from 2026-01-01
```

## Updating, removing

**Update:** a new bundle (step 3, including the swap it prints), then the dry run, build and start of steps 7
and 8. Directories, secrets and Home Assistant stay as they are.

**Remove:** stop and delete the Ting containers, then take the include block out of `~/stack/docker-compose.yml`.
The data directories and secrets stay until you delete them.

```bash
cd ~/stack && docker compose rm -s -f ting-exporter vmalert victoriametrics alertmanager
```
