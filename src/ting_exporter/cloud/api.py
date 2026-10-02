"""Ting REST API (design 2.2): the device list, the notification history, the conditions and frozen-pipe
records (rest.py), and the cloud's voltage history (so far it answers 403; probe only)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp

from ..auth.cognito import Identity

BASE_URL = "https://api.wskr.io"
REQUEST_TIMEOUT = 20.0


class ApiError(Exception):
    """A REST call failed.

    `unauthorized` (401): the identity is stale, renew it. `forbidden` (403): the account may not use this
    resource (e.g. the v3 voltage history); renewing the identity does not help.
    """

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status

    @property
    def unauthorized(self) -> bool:
        return self.status == 401

    @property
    def forbidden(self) -> bool:
        return self.status == 403

    @property
    def unavailable(self) -> bool:
        """Not for the account (403) or not there (404): asking again soon will not help."""
        return self.status in (403, 404)


@dataclass(frozen=True)
class Device:
    serial: str
    name: str
    type: str
    firmware: str


def parse_devices(user: Any) -> list[Device]:
    devices: list[Device] = []
    for raw in (user.get("devices") if isinstance(user, dict) else None) or []:
        if not isinstance(raw, dict):
            continue
        serial = raw.get("serialNumber")
        if not isinstance(serial, str) or not serial:
            continue
        devices.append(
            Device(
                serial=serial,
                name=str(raw.get("name") or serial),
                type=str(raw.get("type") or "unknown"),
                firmware=str(raw.get("version") or "unknown"),
            )
        )
    return devices


async def _get_json(session: aiohttp.ClientSession, identity: Identity, path: str, base_url: str) -> Any:
    """GET `path` (with `{user_id}`) as JSON. Raises ApiError; the logged path never contains the user id."""
    headers = {
        "Authorization": f"Bearer {identity.access_token}",
        "x-wl-api-key": identity.api_key,
        "Accept": "application/json",
    }
    shown = path.replace("{user_id}", "<id>")
    url = base_url + path.replace("{user_id}", str(identity.user_id))
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                raise ApiError(f"GET {shown} failed: HTTP {resp.status}", resp.status)
            return await resp.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as err:
        raise ApiError(f"GET {shown} failed: {type(err).__name__}") from None


async def get_user(session: aiohttp.ClientSession, identity: Identity, base_url: str = BASE_URL) -> Any:
    """The raw account record: the device list with every field the API sends."""
    return await _get_json(session, identity, "/api/v1/Users/{user_id}", base_url)


async def get_conditions(session: aiohttp.ClientSession, identity: Identity, base_url: str = BASE_URL) -> Any:
    """Current conditions: outdoor temperature and outage risk per site, fresher device hazard fields (rest.py)."""
    return await _get_json(session, identity, "/api/v1/Users/{user_id}/conditions", base_url)


async def get_frozen_pipe(session: aiohttp.ClientSession, identity: Identity, serial: str, base_url: str = BASE_URL) -> Any:
    """The frozen-pipe status record of one sensor (rest.py)."""
    return await _get_json(session, identity, f"/api/v1/FrozenPipe/{quote(serial, safe='')}", base_url)


async def list_devices(session: aiohttp.ClientSession, identity: Identity, base_url: str = BASE_URL) -> list[Device]:
    """Raises ApiError, also for a reply that is not an object: the caller retries, it does not take it as no sensors."""
    user = await get_user(session, identity, base_url)
    if not isinstance(user, dict):
        raise ApiError(f"GET /api/v1/Users/<id> failed: the reply is a {type(user).__name__}, not an object")
    return parse_devices(user)


async def get_voltage_history(
    session: aiohttp.ClientSession, identity: Identity, serial: str, start: datetime, end: datetime, base_url: str = BASE_URL
) -> Any:
    """The cloud's own voltage history for one sensor, raw: GET /api/v3/Devices/{serial}/voltage/dateRange.

    At most 24 h per request (the community integration's limit; it goes back about 31 days). Times must be
    timezone-aware; they are sent as UTC.
    """
    if start.tzinfo is None or end.tzinfo is None or not start < end or end - start > timedelta(hours=24):
        raise ValueError("start < end, timezone-aware, at most 24 h apart")
    path = f"/api/v3/Devices/{quote(serial, safe='')}/voltage/dateRange?" + urlencode(
        {"startUtc": start.astimezone(timezone.utc).isoformat(), "endUtc": end.astimezone(timezone.utc).isoformat()})
    return await _get_json(session, identity, path, base_url)


async def list_notifications(session: aiohttp.ClientSession, identity: Identity, base_url: str = BASE_URL) -> list[dict[str, Any]]:
    """The account's notification history, as the phone app shows it (power outage, sag, hazard, ...).

    Raw records; field names as used by the community integrations: id, eventType, eventCategory,
    title, subtitle, message, eventTimestampLocal, sentUtc, serialNumber, siteId, isAcknowledged, isCleared.
    """
    data = await _get_json(session, identity, "/api/v1/Notifications/history/{user_id}", base_url)
    if isinstance(data, dict):  # tolerate a wrapped list
        data = next((v for v in data.values() if isinstance(v, list)), [])
    return [r for r in data if isinstance(r, dict)] if isinstance(data, list) else []
