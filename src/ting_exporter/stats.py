"""A plain histogram, so modules can keep their own statistics without prometheus_client."""

from __future__ import annotations

import math


class Hist:
    def __init__(self, buckets: tuple[float, ...]) -> None:
        self.bounds = tuple(buckets)
        self.counts = [0] * (len(buckets) + 1)
        self.sum = 0.0
        self.count = 0

    def observe(self, value: float) -> None:
        for i, bound in enumerate(self.bounds):
            if value <= bound:
                self.counts[i] += 1
                break
        else:
            self.counts[-1] += 1
        self.sum += value
        self.count += 1

    def cumulative(self) -> list[tuple[str, int]]:
        """[(le, cumulative count)], the shape HistogramMetricFamily wants."""
        out, total = [], 0
        for bound, n in zip((*self.bounds, math.inf), self.counts):
            total += n
            out.append(("+Inf" if bound == math.inf else str(float(bound)), total))
        return out
