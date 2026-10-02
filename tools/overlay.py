"""The private overlay: values for the real hosts, kept out of this public repository (design 13.2).

    python3 tools/overlay.py check   [--overlay DIR]     validate the overlay and the host pair's invariants
    python3 tools/overlay.py render  [--overlay DIR]     the real dashboard into DIR/build/
    python3 tools/overlay.py bundle HOST [--overlay DIR] a deploy bundle for HOST; prints the commands to apply it
    python3 tools/overlay.py leakcheck [--staged | --range A..B | --public] [PATH ...]
    python3 tools/overlay.py init --mirror PATH          a new overlay from overlay.example, with a mirror copy
    python3 tools/overlay.py install-hooks               the leak guard as this repository's pre-commit/pre-push

The overlay lives in ./overlay (gitignored here), is its own git repository, and holds values only, never
copies or patches of base files:

    overlay.toml                 contract version, hosts (role, address, ssh, notify), leak-guard terms
    hosts/<host>/site.env        non-secret values for deploy/compose.yml (secrets stay on the host)
    hosts/<host>/compose.override.yml   optional, merged after the base files
    rules/*.yml                  extra vmalert rule files
    site.toml                    dashboard site names and colours
    runbook.md, docs/            private notes
    tests/                       tests of the host pair's invariants (pytest overlay/tests)

Nothing here connects to a host: `bundle` prints the commands, the owner runs them (design 13.3).
Standard library only (Python 3.11+), so it runs on the Mac without the exporter's dependencies.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import ipaddress
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONTRACT = 1
LOCAL_VM = "http://victoriametrics:8428"
NOTIFY = "-notifier.url=http://alertmanager:9093"
BLACKHOLE = "-notifier.blackhole"
REQUIRED = ("VM_VERSION", "PYTHON_IMAGE", "VM_LISTEN", "DATA_ROOT", "TING_UID", "TING_GID", "TING_HOST", "TING_ROLE",
            "TING_SITES", "TING_VM_URLS", "TING_HEALTH_CHECKS", "VMALERT_NOTIFIER")
SECRET_KEYS = re.compile(r"^(TING_USERNAME|TING_PASSWORD.*|VM_ADMIN_KEY|.*WEBHOOK.*|.*TOKEN.*|.*SECRET.*)$")
PLACEHOLDER = re.compile(r"TODO|<[^>]*>|\.\.\.")
SERIAL = re.compile(r"^[A-Za-z0-9]{4,32}$")
NAME = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")

# public rules (no overlay needed): what may never appear in this repository
DOC_NETS = [ipaddress.ip_network(n) for n in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24", "127.0.0.0/8", "0.0.0.0/32")]
IPV4 = re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])")
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")
EMAIL_OK = re.compile(r"@(?:[\w-]+\.)*example\.(?:org|com|net|invalid)$|@anthropic\.com$")
SERIAL_SHAPED = re.compile(r"\b(?=[0-9A-F]{9}\b)(?=[0-9A-F]*[A-F])(?=[0-9A-F]*[0-9])[0-9A-F]{9}\b")


class OverlayError(Exception):
    pass


def read_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for n, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        text = raw.strip()
        if not text or text.startswith("#"):
            continue
        key, sep, value = text.partition("=")
        if not sep:
            raise OverlayError(f"{path}:{n}: not KEY=VALUE")
        value = value.split(" #", 1)[0].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        out[key.strip()] = value
    return out


def named_urls(value: str) -> dict[str, str]:
    out = {}
    for entry in (e.strip() for e in value.split(",") if e.strip()):
        name, _, url = entry.partition("=")
        out[name.strip()] = url.strip().rstrip("/")
    return out


def sites_of(value: str) -> dict[str, str]:
    out = {}
    for pair in (p.strip() for p in value.split(",") if p.strip()):
        serial, _, site = pair.partition("=")
        out[serial.strip()] = site.strip()
    return out


class Overlay:
    def __init__(self, directory: Path) -> None:
        self.dir = directory
        manifest = directory / "overlay.toml"
        if not manifest.is_file():
            raise OverlayError(f"{manifest} is missing (python3 tools/overlay.py init --mirror PATH makes one)")
        with open(manifest, "rb") as f:
            self.manifest = tomllib.load(f)
        self.hosts: dict[str, dict] = dict(self.manifest.get("hosts", {}))
        self.envs: dict[str, dict[str, str]] = {}
        for host in self.hosts:
            path = directory / "hosts" / host / "site.env"
            self.envs[host] = read_env(path) if path.is_file() else {}
        site_file = directory / "site.toml"
        self.sites_doc = tomllib.load(open(site_file, "rb")) if site_file.is_file() else {}

    def check(self) -> list[str]:
        problems: list[str] = []
        if self.manifest.get("contract") != CONTRACT:
            problems.append(f"overlay.toml: contract {self.manifest.get('contract')!r}, this repository expects {CONTRACT}")
        if not self.hosts:
            problems.append("overlay.toml: no [hosts.<name>]")
        for host, spec in self.hosts.items():
            env, where = self.envs[host], f"hosts/{host}/site.env"
            if not env:
                problems.append(f"{where}: missing or empty")
                continue
            for key in REQUIRED + (("ALERTMANAGER_VERSION",) if spec.get("notify") else ()):
                if not env.get(key):
                    problems.append(f"{where}: {key} is not set")
                elif PLACEHOLDER.search(env[key]):
                    problems.append(f"{where}: {key} still holds a placeholder ({env[key]!r})")
            for key in env:
                if SECRET_KEYS.match(key):
                    problems.append(f"{where}: {key} does not belong in the overlay; secrets and personal data stay "
                                    "in ~/stack/secrets on the host")
            if env.get("TING_RELEASE_OTHERS", "").lower() in ("1", "true", "yes", "on"):
                problems.append(f"{where}: TING_RELEASE_OTHERS would end the other exporter's stream")
            expect = {"TING_HOST": host, "TING_ROLE": spec.get("role"), "VM_LISTEN": spec.get("address")}
            for key, want in expect.items():
                if env.get(key) and env[key] != want:
                    problems.append(f"{where}: {key}={env[key]} but overlay.toml says {want}")
            if not NAME.match(host):
                problems.append(f"overlay.toml: host name {host!r} must be 1-32 letters, digits, '_', '.' or '-'")
            notifier = NOTIFY if spec.get("notify") else BLACKHOLE
            if env.get("VMALERT_NOTIFIER") and env["VMALERT_NOTIFIER"] != notifier:
                problems.append(f"{where}: VMALERT_NOTIFIER must be {notifier} (notify = {bool(spec.get('notify'))})")
            if env.get("PYTHON_IMAGE") and "@sha256:" not in env["PYTHON_IMAGE"]:
                problems.append(f"{where}: PYTHON_IMAGE must be pinned by digest (...@sha256:...)")
            if env.get("VM_VERSION") and not re.match(r"^v\d+\.\d+\.\d+$", env["VM_VERSION"]):
                problems.append(f"{where}: VM_VERSION must be an exact release such as v1.152.0")
            targets = named_urls(env.get("TING_VM_URLS", ""))
            if targets and targets.get("local") != LOCAL_VM:
                problems.append(f"{where}: TING_VM_URLS needs local={LOCAL_VM}")
            sites = sites_of(env.get("TING_SITES", ""))
            for serial, site in sites.items():
                if not SERIAL.match(serial) or not NAME.match(site):
                    problems.append(f"{where}: TING_SITES entry {serial}={site} is not serial=site")
            if len(set(sites.values())) != len(sites):
                problems.append(f"{where}: TING_SITES has two sensors at one site (design 5.1)")
            for site in sites.values():
                info = self.sites_doc.get("sites", {}).get(site)
                if not info or not info.get("name") or not re.match(r"^#[0-9a-fA-F]{6}$", str(info.get("colour", ""))):
                    problems.append(f"site.toml: [sites.{site}] needs a name and a colour (#rrggbb)")
        notifying = [h for h, s in self.hosts.items() if s.get("notify")]
        if self.hosts and len(notifying) != 1:
            problems.append(f"overlay.toml: exactly one host notifies (now: {', '.join(notifying) or 'none'})")
        if len(self.hosts) == 2:
            problems += self._pair()
        for path in sorted((self.dir / "rules").glob("*.yml")) if (self.dir / "rules").is_dir() else []:
            if (ROOT / "deploy" / "vmalert" / path.name).exists():
                problems.append(f"rules/{path.name}: has the name of a base rule file; choose another")
        return problems

    def _pair(self) -> list[str]:
        problems = []
        (a, sa), (b, sb) = self.hosts.items()
        ea, eb = self.envs[a], self.envs[b]
        if sa.get("role") == sb.get("role"):
            problems.append("overlay.toml: the two hosts need different roles")
        for key in ("TING_SITES", "VM_VERSION", "PYTHON_IMAGE"):
            if key in ea and key in eb and (sites_of(ea[key]) if key == "TING_SITES" else ea[key]) != \
                    (sites_of(eb[key]) if key == "TING_SITES" else eb[key]):
                problems.append(f"{key} differs between {a} and {b}: both stores must hold the same series")
        for host, env, other in ((a, ea, sb), (b, eb, sa)):
            peer = f"http://{other.get('address')}:8428"
            if peer not in named_urls(env.get("TING_VM_URLS", "")).values():
                problems.append(f"hosts/{host}/site.env: TING_VM_URLS must push to the peer, {peer}")
            if f"{peer}/health" not in named_urls(env.get("TING_HEALTH_CHECKS", "")).values():
                problems.append(f"hosts/{host}/site.env: TING_HEALTH_CHECKS should check the peer's {peer}/health")
        return problems

    def leak_terms(self) -> set[str]:
        terms = set(self.manifest.get("leak", {}).get("terms", []))
        for host, spec in self.hosts.items():
            terms.add(host)
            for key in ("address", "ssh"):
                if spec.get(key):
                    terms.add(str(spec[key]))
                    terms.update(str(spec[key]).split("@"))
            env = self.envs[host]
            terms.update(sites_of(env.get("TING_SITES", "")))
            for value in env.values():
                terms.update(IPV4.findall(value))
        for path in self.dir.rglob("*"):
            if path.is_file() and ".git" not in path.parts and "build" not in path.parts:
                try:
                    terms.update(EMAIL.findall(path.read_text(encoding="utf-8", errors="ignore")))
                except OSError:
                    pass
        return {t for t in terms if len(t) >= 4 and not _public_ok(t)}


def _public_ok(term: str) -> bool:
    return term in ("localhost", "victoriametrics", "vmalert", "alertmanager") or bool(EMAIL_OK.search(term))


def public_problems(text: str) -> list[str]:
    out = []
    for ip in IPV4.findall(text):
        try:
            address = ipaddress.ip_address(ip)
        except ValueError:
            continue
        if not any(address in net for net in DOC_NETS):
            out.append(f"IP address {ip} (use the RFC 5737 documentation ranges)")
    for email in EMAIL.findall(text):
        if IPV4.fullmatch(email.rpartition("@")[2]):
            continue  # user@address: the address is checked above
        if not EMAIL_OK.search(email):
            out.append(f"e-mail address {email} (use example.org)")
    for serial in SERIAL_SHAPED.findall(text):
        out.append(f"serial-shaped {serial} (use TNG0000xx)")
    return out


# ---- commands --------------------------------------------------------------------------------------


def git(*args: str, cwd: Path = ROOT, data: bytes | None = None) -> bytes:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, input=data).stdout


def cmd_check(overlay: Overlay) -> int:
    problems = overlay.check()
    for p in problems:
        print(f"problem: {p}")
    print("overlay ok" if not problems else f"{len(problems)} problems")
    return 1 if problems else 0


def render_dashboard(overlay: Overlay, out: Path) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    target = out / "ting-dashboard.json"
    with open(target, "w") as f:
        subprocess.run([sys.executable, str(ROOT / "tools" / "make_dashboard.py"), "--sites-file", str(overlay.dir / "site.toml")],
                       check=True, stdout=f)
    return target


def cmd_render(overlay: Overlay) -> int:
    if cmd_check(overlay):
        return 1
    print(f"wrote {render_dashboard(overlay, overlay.dir / 'build')}")
    return 0


def cmd_bundle(overlay: Overlay, host: str) -> int:
    if host not in overlay.hosts:
        print(f"no host {host!r} in overlay.toml ({', '.join(overlay.hosts)})")
        return 2
    if cmd_check(overlay):
        return 1
    if git("status", "--porcelain").strip():
        print("the repository has uncommitted changes: commit them first (a bundle is a clean HEAD)")
        return 1
    commit = git("rev-parse", "--short=12", "HEAD").decode().strip()
    try:
        overlay_commit = git("rev-parse", "--short=12", "HEAD", cwd=overlay.dir).decode().strip()
        dirty = bool(git("status", "--porcelain", cwd=overlay.dir).strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        overlay_commit, dirty = "none", True
    if dirty:
        print("note: the overlay has uncommitted changes; the bundle records it as such")
    build = overlay.dir / "build"
    dashboard = render_dashboard(overlay, build)
    name = f"ting-{host}-{commit}.tar.gz"
    archive = io.BytesIO(git("archive", "--format=tar", "--prefix=ting/", "HEAD"))
    stamp = f"host={host}\nrepository={commit}\noverlay={overlay_commit}{' (uncommitted changes)' if dirty else ''}\n" \
            f"built={dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ}\n"
    with tarfile.open(fileobj=archive) as base, tarfile.open(build / name, "w:gz") as out:
        for member in base.getmembers():
            if member.name == "ting/deploy/grafana/ting-dashboard.json":
                continue
            out.addfile(member, base.extractfile(member) if member.isfile() else None)
        extra = {"ting/site.env": overlay.dir / "hosts" / host / "site.env",
                 "ting/deploy/grafana/ting-dashboard.json": dashboard}
        if (override := overlay.dir / "hosts" / host / "compose.override.yml").is_file():
            extra["ting/compose.override.yml"] = override
        for rule in sorted((overlay.dir / "rules").glob("*.yml")) if (overlay.dir / "rules").is_dir() else []:
            extra[f"ting/deploy/vmalert/{rule.name}"] = rule
        for arcname, path in extra.items():
            out.add(path, arcname=arcname)
        info = tarfile.TarInfo("ting/BUNDLE")
        info.size, info.mtime, info.mode = len(stamp), int(dt.datetime.now().timestamp()), 0o644
        out.addfile(info, io.BytesIO(stamp.encode()))
    with open(overlay.dir / "deployments.log", "a") as log:
        log.write(f"{dt.datetime.now(dt.timezone.utc):%Y-%m-%dT%H:%M:%SZ} {host} {commit} overlay={overlay_commit} {name}\n")
    spec = overlay.hosts[host]
    ssh = spec.get("ssh", f"<user>@{spec.get('address')}")
    files = ["ting/deploy/compose.yml"] + (["ting/deploy/compose.notify.yml"] if spec.get("notify") else []) + \
            (["ting/compose.override.yml"] if "ting/compose.override.yml" in extra else [])
    notify = spec.get("notify")
    services = "victoriametrics vmalert" + (" alertmanager" if notify else "") + " ting-exporter"
    include = "\n".join(["  include:", f"    - path: [{', '.join(files)}]", "      env_file: [ting/site.env, secrets/ting.env]"])
    print(f"""
bundle: {build / name}
Nothing was copied or run. Every block below is safe to paste as a whole.

On the Mac:

  scp {build / name} {ssh}:/tmp/{name}

On {host} ({ssh}), unpack next to the running stack, then swap (ting.prev keeps the old bundle):

  cd ~/stack && rm -rf ting.new && mkdir ting.new && tar -xzf /tmp/{name} -C ting.new --strip-components=1
  cd ~/stack && rm -rf ting.prev && {{ [ ! -d ting ] || mv ting ting.prev; }} && mv ting.new ting && cat ting/BUNDLE

A first install also needs, before the next step, the data directories, the secrets and Home Assistant's
webhook: docs/install.md, steps 4 to 6.

The include block in ~/stack/docker-compose.yml for {host}:

{include}

Dry run; only {services} may appear as created or recreated:

  cd ~/stack && docker compose config --quiet && docker compose up -d --dry-run 2>&1 | tail -20

Build, check, start:

  cd ~/stack && docker compose build ting-exporter && docker compose run --rm --no-deps ting-exporter check-config
  cd ~/stack && docker compose up -d {services}

To go back to the previous bundle:

  cd ~/stack && rm -rf ting && mv ting.prev ting && docker compose up -d
""")
    return 0


def added_lines(args: argparse.Namespace) -> list[tuple[str, str]]:
    """(where, text) of what to check."""
    if args.paths:
        out = []
        for p in map(Path, args.paths):
            files = [f for f in p.rglob("*") if f.is_file()] if p.is_dir() else [p]
            for f in files:
                out += [(f"{f}:{n}", line) for n, line in enumerate(_text(f).splitlines(), 1)]
        return out
    if args.staged:
        diff = git("diff", "--cached", "-U0", "--no-color", "--no-ext-diff")
    elif args.range:
        diff = git("diff", "-U0", "--no-color", "--no-ext-diff", args.range)
    else:
        out = []
        for name in git("ls-files", "-z").decode().split("\0"):
            if name and (ROOT / name).is_file():
                out += [(f"{name}:{n}", line) for n, line in enumerate(_text(ROOT / name).splitlines(), 1)]
        return out
    out, current = [], "?"
    for raw in diff.decode("utf-8", "replace").splitlines():
        if raw.startswith("+++ "):
            current = raw[6:] if raw.startswith("+++ b/") else raw[4:]
        elif raw.startswith("+") and not raw.startswith("+++"):
            out.append((current, raw[1:]))
    return out


def _text(path: Path) -> str:
    try:
        data = path.read_bytes()
    except OSError:
        return ""
    if path.suffix == ".gz":
        import gzip
        try:
            data = gzip.decompress(data)
        except OSError:
            return ""
    return data.decode("utf-8", "ignore")


def cmd_leakcheck(args: argparse.Namespace) -> int:
    lines = added_lines(args)
    terms: set[str] = set()
    if not args.public and (args.overlay / "overlay.toml").is_file():
        terms = Overlay(args.overlay).leak_terms()
    found = []
    for where, text in lines:
        for term in terms:
            if term in text:
                found.append(f"{where}: a private value from the overlay ({term[:2]}...{term[-1:]})")
        for problem in public_problems(text):
            found.append(f"{where}: {problem}")
    for f in found[:50]:
        print(f, file=sys.stderr)
    if found:
        print(f"leak guard: {len(found)} findings; nothing private may enter this repository (design 13.2)", file=sys.stderr)
        return 1
    return 0


def cmd_init(directory: Path, mirror: Path) -> int:
    if directory.exists():
        print(f"{directory} exists already")
        return 1
    shutil.copytree(ROOT / "overlay.example", directory)
    git("init", "-q", "-b", "main", cwd=directory)
    mirror.mkdir(parents=True, exist_ok=True)
    if not (mirror / ".git").exists():
        git("init", "-q", "-b", "main", cwd=mirror)
    git("config", "receive.denyCurrentBranch", "updateInstead", cwd=mirror)
    git("remote", "add", "mirror", str(mirror), cwd=directory)
    hook = directory / ".git" / "hooks" / "post-commit"
    hook.write_text("#!/bin/sh\n# every overlay commit goes to the readable mirror too (design 13.2)\n"
                    "git push --quiet mirror HEAD:main || echo 'overlay: the mirror push failed' >&2\n")
    hook.chmod(0o755)
    (directory / ".gitignore").write_text("build/\n")
    print(f"made {directory} from overlay.example (its own git repository, mirrored to {mirror}); fill in the values, "
          "then `python3 tools/overlay.py check`")
    return 0


def cmd_install_hooks() -> int:
    git("config", "core.hooksPath", "tools/hooks")
    print("leak guard installed: tools/hooks/pre-commit and pre-push run for every commit and push")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=["check", "render", "bundle", "leakcheck", "init", "install-hooks"])
    parser.add_argument("host", nargs="?", help="bundle: the host")
    parser.add_argument("paths", nargs="*", help="leakcheck: files or directories (default: every tracked file)")
    parser.add_argument("--overlay", type=Path, default=Path(os.environ.get("TING_OVERLAY", ROOT / "overlay")))
    parser.add_argument("--staged", action="store_true", help="leakcheck: the lines a commit adds")
    parser.add_argument("--range", help="leakcheck: the lines added in A..B (pre-push)")
    parser.add_argument("--public", action="store_true", help="leakcheck: the public rules only (CI, no overlay)")
    parser.add_argument("--mirror", type=Path, help="init: the readable copy, e.g. ~/Documents/net/Ting/overlay")
    args = parser.parse_intermixed_args(argv)
    try:
        if args.command == "leakcheck":
            if args.host:
                args.paths = [args.host, *args.paths]
            return cmd_leakcheck(args)
        if args.command == "install-hooks":
            return cmd_install_hooks()
        if args.command == "init":
            if not args.mirror:
                parser.error("init needs --mirror PATH")
            return cmd_init(args.overlay, args.mirror.expanduser())
        overlay = Overlay(args.overlay)
        if args.command == "check":
            return cmd_check(overlay)
        if args.command == "render":
            return cmd_render(overlay)
        if not args.host:
            parser.error("bundle needs a host")
        return cmd_bundle(overlay, args.host)
    except (OverlayError, tomllib.TOMLDecodeError, subprocess.CalledProcessError) as err:
        print(f"overlay: {err}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
