"""The unit that leaves the pipeline: one sample for VictoriaMetrics."""

from __future__ import annotations

from dataclasses import dataclass


def format_value(value: float, decimals: int) -> str:
    """Shortest text for an already rounded value: 124.996, 42, 0.03836."""
    if decimals <= 0:
        return str(int(value))
    return f"{value:.{decimals}f}".rstrip("0").rstrip(".") or "0"


def label_text(labels: dict[str, str]) -> str:
    """`serial="TNG000001",site="a"`, sorted, escaped per the exposition format."""
    parts = []
    for key in sorted(labels):
        value = labels[key].replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
        parts.append(f'{key}="{value}"')
    return ",".join(parts)


@dataclass(frozen=True, slots=True)
class Sample:
    metric: str
    serial: str
    labels: str  # preformatted, see label_text
    ts_ms: int  # device time, milliseconds since the epoch
    value: float
    text: str  # value as pushed

    def line(self) -> str:
        """One line of the Prometheus text format with a millisecond timestamp."""
        return f"{self.metric}{{{self.labels}}} {self.text} {self.ts_ms}\n"
