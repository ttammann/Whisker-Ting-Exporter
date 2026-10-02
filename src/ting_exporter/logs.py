"""Logging setup: text or JSON lines, with a scrubber that masks registered secrets.

Secrets (password, access and refresh tokens, API key) are registered by the
IdentityManager as soon as it holds them. Every record is formatted first and
then scrubbed, so a secret cannot leak through an exception message or a
third-party library's log line either.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

MASK = "***"
_secrets: dict[str, None] = {}  # registered secrets, in order
_forms: list[str] = []  # every form of every secret, longest first: rebuilt when a secret comes or goes, not per line


def _forms_of(secret: str) -> set[str]:
    """The secret as it can appear in a log line: as is, inside a repr (quotes and backslashes escaped), and
    JSON-escaped, also both."""
    in_repr = repr(secret)[1:-1]
    return {secret, in_repr, json.dumps(secret, ensure_ascii=False)[1:-1], json.dumps(secret)[1:-1],
            json.dumps(in_repr, ensure_ascii=False)[1:-1]}


def _rebuild() -> None:
    global _forms
    _forms = sorted({form for secret in _secrets for form in _forms_of(secret) if len(form) >= 4}, key=len, reverse=True)


def register_secret(value: str | None) -> None:
    if value and len(value) >= 4 and value not in _secrets:
        _secrets[value] = None
        _rebuild()


def retire_secret(value: str | None) -> None:
    """Forget a secret that has been replaced (a renewed access token): it opens nothing any more, and keeping
    every one would grow the scrubber's work for the life of the process."""
    if value in _secrets:
        del _secrets[value]
        _rebuild()


def scrub(text: str) -> str:
    for form in _forms:
        if form in text:
            text = text.replace(form, MASK)
    return text


class _Scrubbing(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return scrub(super().format(record))


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": scrub(record.getMessage()),  # before json.dumps escapes quotes and backslashes in it
        }
        if record.exc_info:
            entry["exc"] = scrub(self.formatException(record.exc_info))
        return scrub(json.dumps(entry, ensure_ascii=False))


def setup(level: str = "INFO", fmt: str = "text", stream=None) -> None:
    """Log to stderr (or `stream`). The values may still be unchecked (main sets up logging before it reads the
    config, which reports a bad value itself): padding and case are ignored, and an unknown level is INFO."""
    level, fmt = level.strip().upper(), fmt.strip().lower()
    if not isinstance(logging.getLevelName(level), int):
        level = "INFO"
    handler = logging.StreamHandler(stream or sys.stderr)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(_Scrubbing("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    for old in list(root.handlers):
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(level)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
