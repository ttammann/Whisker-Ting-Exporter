"""Reading from VictoriaMetrics (the rollup repair, `mark`, `compare-stores`): instant and range queries."""

from __future__ import annotations

import asyncio
import gzip
import json
from typing import Any
from urllib.parse import urlencode

import aiohttp

TIMEOUT = 60.0


class VmError(Exception):
    """A query or import failed (network, HTTP status, or a reply of the wrong shape)."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status

    @property
    def refused(self) -> bool:
        """The store refused the request itself (4xx other than 429): retrying will not help."""
        return self.status is not None and 400 <= self.status < 500 and self.status != 429


Series = tuple[dict[str, str], list[tuple[float, str]]]


class VmQuery:
    def __init__(self, session: aiohttp.ClientSession, base_url: str, timeout: float = TIMEOUT) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    async def _get(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self.base_url}{path}?{urlencode(params)}"
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=self.timeout)) as resp:
                raw = await resp.read()
                if resp.status != 200:
                    raise VmError(f"HTTP {resp.status}: {raw[:300].decode('utf-8', 'replace').strip()}", resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
            raise VmError(f"{type(err).__name__}: {err}") from None
        try:
            body = json.loads(raw.decode("utf-8"))
            if body.get("status") != "success":
                raise VmError(f"query failed: {body.get('error') or body.get('status')}")
            return body["data"]["result"]
        except (ValueError, KeyError, TypeError, AttributeError) as err:
            raise VmError(f"unexpected reply ({type(err).__name__})") from None

    async def query(self, expr: str, at: float | None = None, *, fresh: bool = False) -> list[tuple[dict[str, str], str]]:
        """Instant query: [(labels, value)]. `fresh`: include the last 30 s, which VictoriaMetrics hides by default
        (-search.latencyOffset) because a scraped sample may still be on its way; a mark written a moment ago is not."""
        params: dict[str, Any] = {"query": expr}
        if at is not None:
            params["time"] = f"{at:.3f}"
        if fresh:
            params["latency_offset"] = "1ms"  # the smallest it accepts; 0 is refused
        try:
            return [(dict(r["metric"]), str(r["value"][1])) for r in await self._get("/api/v1/query", params)]
        except (KeyError, TypeError, IndexError) as err:
            raise VmError(f"unexpected reply ({type(err).__name__})") from None

    async def query_range(self, expr: str, start: float, end: float, step: int, nocache: bool = False) -> list[Series]:
        """Range query: [(labels, [(t, value)])]. `start` and `end` should be multiples of `step`."""
        params: dict[str, Any] = {"query": expr, "start": f"{start:.0f}", "end": f"{end:.0f}", "step": f"{step}s"}
        if nocache:
            params["nocache"] = "1"
        try:
            return [(dict(r["metric"]), [(float(t), str(v)) for t, v in r["values"]])
                    for r in await self._get("/api/v1/query_range", params)]
        except (KeyError, TypeError, ValueError) as err:
            raise VmError(f"unexpected reply ({type(err).__name__})") from None

    async def import_lines(self, lines: list[str]) -> None:
        """POST lines in the Prometheus text format (with timestamps) to /api/v1/import/prometheus."""
        body = gzip.compress("".join(lines).encode(), compresslevel=5)
        try:
            async with self.session.post(f"{self.base_url}/api/v1/import/prometheus", data=body,
                                         headers={"Content-Encoding": "gzip", "Content-Type": "text/plain"},
                                         timeout=aiohttp.ClientTimeout(total=self.timeout)) as resp:
                raw = await resp.read()
                if not 200 <= resp.status < 300:
                    raise VmError(f"import: HTTP {resp.status}: {raw[:300].decode('utf-8', 'replace').strip()}", resp.status)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as err:
            raise VmError(f"import: {type(err).__name__}: {err}") from None
