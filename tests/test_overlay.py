"""The overlay framework (design 13.2): checks, the leak guard, bundles that print commands and connect nowhere."""

import importlib.util
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def tool():
    spec = importlib.util.spec_from_file_location("overlay_tool", ROOT / "tools" / "overlay.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def example(tmp_path):
    shutil.copytree(ROOT / "overlay.example", tmp_path / "overlay")
    return tmp_path / "overlay"


def test_the_example_overlay_passes(tool, example):
    assert tool.Overlay(example).check() == []


@pytest.mark.parametrize(("host", "edit", "needle"), [
    ("host-a", ("TING_ROLE=primary", "TING_ROLE=secondary"), "TING_ROLE=secondary but overlay.toml says primary"),
    ("host-a", ("peer=http://198.51.100.10:8428", "peer=http://203.0.113.9:8428"), "must push to the peer"),
    ("host-a", ("TING_SITES=TNG000001=a,TNG000002=b", "TING_SITES=TNG000001=a"), "TING_SITES differs"),
    ("host-b", ("VMALERT_NOTIFIER=-notifier.url=http://alertmanager:9093", "VMALERT_NOTIFIER=-notifier.blackhole"),
     "VMALERT_NOTIFIER must be -notifier.url"),
    ("host-a", ("python:3.13-slim@sha256:" + "0" * 64, "python:3.13-slim"), "pinned by digest"),
    ("host-a", ("VM_VERSION=v1.152.0", "VM_VERSION=<pick one>"), "placeholder"),
    ("host-a", ("DATA_ROOT=/mnt/data\n", "DATA_ROOT=/mnt/data\nTING_USERNAME=me@example.org\n"), "does not belong"),
    ("host-a", ("TING_HOST=host-a\n", "TING_HOST=host-a\nTING_RELEASE_OTHERS=true\n"), "end the other exporter"),
])
def test_the_checks_catch_a_broken_pair(tool, example, host, edit, needle):
    env = example / "hosts" / host / "site.env"
    env.write_text(env.read_text().replace(*edit))
    problems = tool.Overlay(example).check()
    assert any(needle in p for p in problems), problems


def test_one_notifier_and_named_sites(tool, example):
    manifest = example / "overlay.toml"
    manifest.write_text(manifest.read_text().replace("notify = true", "notify = false"))
    (example / "site.toml").write_text("[sites.a]\nname = 'A'\ncolour = '#3987e5'\n")
    problems = tool.Overlay(example).check()
    assert any("exactly one host notifies" in p for p in problems)
    assert any("[sites.b] needs a name" in p for p in problems)


def test_the_leak_guard(tool, example):
    terms = tool.Overlay(example).leak_terms()
    assert {"198.51.100.10", "host-a", "TNG000001", "Example Street"} <= terms
    ip, mail, serial = ".".join(["10", "1", "2", "3"]), "me" + "@corp.test", "1F0" + "170089"  # not literal: this file is scanned too
    assert tool.public_problems(f"connect to 192.0.2.7 or {ip}, mail {mail}, sensor {serial}") == [
        f"IP address {ip} (use the RFC 5737 documentation ranges)",
        f"e-mail address {mail} (use example.org)",
        f"serial-shaped {serial} (use TNG0000xx)",
    ]
    assert tool.public_problems("v1.152.0 3.14.3 sha256:abc TNG000001 x@example.org admin@198.51.100.10 1.2.3.4.5") == []


def test_the_public_repository_holds_nothing_private(tool):
    """Public CI without the overlay: every file of the repository (fixtures decompressed) passes the public rules."""
    findings = []
    for path in ROOT.rglob("*"):
        parts = set(path.relative_to(ROOT).parts)
        if not path.is_file() or parts & {".git", ".venv", "overlay", "build", "__pycache__", ".pytest_cache"} or \
                path.suffix in (".pyc",) or ".egg-info" in str(path):
            continue
        for n, line in enumerate(tool._text(path).splitlines(), 1):
            findings += [f"{path.relative_to(ROOT)}:{n}: {p}" for p in tool.public_problems(line)]
    assert findings == []


def test_bundle_prints_commands_and_connects_nowhere(tool, tmp_path):
    """A clean checkout, the example overlay: a tarball with the host's values, rules and dashboard, and a log line."""
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    for name in ("tools", "deploy", "overlay.example"):
        shutil.copytree(ROOT / name, repo / name)
    (repo / ".gitignore").write_text("/overlay/\n")
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.org", "commit", "-q", "-m", "x"],
                   check=True)
    shutil.copytree(repo / "overlay.example", repo / "overlay")
    result = subprocess.run(["python3", str(repo / "tools" / "overlay.py"), "bundle", "host-b"], cwd=repo,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Nothing was copied or run" in result.stdout and "scp " in result.stdout
    assert "ting/deploy/compose.notify.yml" in result.stdout  # host-b notifies
    [bundle] = list((repo / "overlay" / "build").glob("ting-host-b-*.tar.gz"))
    with tarfile.open(bundle) as tar:
        names = set(tar.getnames())
        site_env = tar.extractfile("ting/site.env").read().decode()
        stamp = tar.extractfile("ting/BUNDLE").read().decode()
    assert {"ting/deploy/compose.yml", "ting/deploy/vmalert/site-rules.yml", "ting/deploy/grafana/ting-dashboard.json"} <= names
    assert "TING_HOST=host-b" in site_env and "host=host-b" in stamp
    assert "host-b" in (repo / "overlay" / "deployments.log").read_text()
    (repo / "deploy" / "compose.yml").write_text("changed\n")
    dirty = subprocess.run(["python3", str(repo / "tools" / "overlay.py"), "bundle", "host-b"], cwd=repo, capture_output=True, text=True)
    assert dirty.returncode == 1 and "uncommitted changes" in dirty.stdout
