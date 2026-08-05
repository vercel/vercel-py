"""Shared Runtime Cache document plumbing for the cache backend."""

from __future__ import annotations

from typing import Any

from datetime import datetime

from ..._time import as_utc

# 35 days. Docs are rewritten on every touch (wakes, activation-hook runs),
# so an active, paused, or dormant scheduler on a traffic-serving deployment
# never expires; the TTL only reaps abandoned namespaces (and, with them,
# runtime-added jobs and lifecycle flags — declared jobs come back from code).
# LRU eviction is the space-based reaper; this is the time-based one.
DOC_TTL_SECONDS = 35 * 24 * 3600
_INDEX_MERGE_ATTEMPTS = 4

__all__ = ["DOC_TTL_SECONDS"]


def iso(value: datetime | None) -> str | None:
    return None if value is None else as_utc(value, name="value").isoformat()


def from_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value)
