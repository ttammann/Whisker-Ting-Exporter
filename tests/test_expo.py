"""The hand-written exposition format (replaces prometheus_client, design 4.2)."""

import math

import pytest

from ting_exporter import expo


def test_families_render_as_the_text_format():
    g = expo.gauge("ting_x", "Help with a \\ backslash\nand a newline.", ("site", "name")).add(("a", 'q"u\\o\nte'), 1.5)
    c = expo.counter("ting_y_total", "Count.").add((), 3)
    h = expo.histogram("ting_z_seconds", "Delay.", ("site",)).histogram(("a",), [("0.5", 1), ("+Inf", 4)], 2.25)
    empty = expo.gauge("ting_empty", "Nothing.")
    text = expo.render([g, c, h, empty]).decode()
    assert text == (
        "# HELP ting_x Help with a \\\\ backslash\\nand a newline.\n# TYPE ting_x gauge\n"
        'ting_x{site="a",name="q\\"u\\\\o\\nte"} 1.5\n'
        "# HELP ting_y_total Count.\n# TYPE ting_y_total counter\nting_y_total 3\n"
        "# HELP ting_z_seconds Delay.\n# TYPE ting_z_seconds histogram\n"
        'ting_z_seconds_bucket{site="a",le="0.5"} 1\nting_z_seconds_bucket{site="a",le="+Inf"} 4\n'
        'ting_z_seconds_sum{site="a"} 2.25\nting_z_seconds_count{site="a"} 4\n'
    )


def test_numbers_and_mistakes():
    assert [expo.format_number(v) for v in (1, 1.0, 0.1, math.inf, -math.inf, True, 1e20)] == \
        ["1", "1", "0.1", "+Inf", "-Inf", "1", "1e+20"]
    assert expo.format_number(math.nan) == "NaN"
    with pytest.raises(ValueError):
        expo.counter("ting_no_suffix", "x")
    with pytest.raises(ValueError):
        expo.gauge("ting_g", "x", ("a",)).add((), 1)


def test_process_families():
    names = {f.name for f in expo.process_families()}
    assert {"process_cpu_seconds_total", "process_resident_memory_bytes", "process_start_time_seconds", "python_info"} <= names
