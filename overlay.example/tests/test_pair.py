"""The host pair's invariants, plus whatever this overlay wants to pin down (pytest overlay/tests)."""

import importlib.util
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent


def _tool():
    root = HERE.parent if (HERE.parent / "tools" / "overlay.py").exists() else Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location("overlay_tool", root / "tools" / "overlay.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_overlay_passes_its_checks():
    tool = _tool()
    assert tool.Overlay(HERE).check() == []


def test_both_hosts_stream_the_same_sites():
    tool = _tool()
    overlay = tool.Overlay(HERE)
    sites = {host: tool.sites_of(env["TING_SITES"]) for host, env in overlay.envs.items()}
    assert len(set(map(lambda s: tuple(sorted(s.items())), sites.values()))) == 1
