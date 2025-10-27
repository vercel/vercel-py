"""Telemetry client for tracking SDK usage."""

import os
import time
import uuid
from typing import Any, Dict, Literal, Optional

import httpx

_TELEMETRY_ENABLED = os.getenv("VERCEL_TELEMETRY_DISABLED") != "1"
_TELEMETRY_BRIDGE_URL = os.getenv(
    "VERCEL_TELEMETRY_BRIDGE_URL",
    "https://telemetry.vercel.com/api/vercel-py/v1/events",
)


EventType = Literal[
    "blob_put",
    "blob_delete",
    "cache_set",
    "cache_get",
    "project_create",
    "project_update",
    "project_delete",
    "deployment_create",
]


class TelemetryClient:
    """Client for sending telemetry events."""

    def __init__(self, session_id: Optional[str] = None):
        """Initialize telemetry client.

        Args:
            session_id: Unique session ID. If not provided, generates a new one.
        """
        self.session_id = session_id or str(uuid.uuid4())
        self._events: Dict[EventType, list[Dict[str, Any]]] = {
            "blob_put": [],
            "blob_delete": [],
            "cache_set": [],
            "cache_get": [],
            "project_create": [],
            "project_update": [],
            "project_delete": [],
            "deployment_create": [],
        }
        self._enabled = _TELEMETRY_ENABLED

    def track_blob_put(
        self,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
        access: str = "public",
        content_type: Optional[str] = None,
        size_bytes: Optional[int] = None,
        multipart: bool = False,
    ) -> None:
        """Track a blob PUT operation."""
        if not self._enabled:
            return

        event: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
            "access": access,
            "multipart": multipart,
        }

        if user_id:
            event["user_id"] = user_id
        if team_id:
            event["team_id"] = team_id
        if project_id:
            event["project_id"] = project_id
        if content_type:
            event["content_type"] = content_type
        if size_bytes is not None:
            event["size_bytes"] = size_bytes

        self._events["blob_put"].append(event)

    def track_blob_delete(
        self,
        count: int,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """Track a blob DELETE operation."""
        if not self._enabled:
            return

        event: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
            "count": count,
        }

        if user_id:
            event["user_id"] = user_id
        if team_id:
            event["team_id"] = team_id
        if project_id:
            event["project_id"] = project_id

        self._events["blob_delete"].append(event)

    def track_cache_set(
        self,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        has_tags: bool = False,
    ) -> None:
        """Track a cache SET operation."""
        if not self._enabled:
            return

        event: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
            "has_tags": has_tags,
        }

        if user_id:
            event["user_id"] = user_id
        if team_id:
            event["team_id"] = team_id
        if project_id:
            event["project_id"] = project_id
        if ttl_seconds is not None:
            event["ttl_seconds"] = ttl_seconds

        self._events["cache_set"].append(event)

    def track_cache_get(
        self,
        hit: bool,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """Track a cache GET operation."""
        if not self._enabled:
            return

        event: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
            "hit": hit,
        }

        if user_id:
            event["user_id"] = user_id
        if team_id:
            event["team_id"] = team_id
        if project_id:
            event["project_id"] = project_id

        self._events["cache_get"].append(event)

    def track_project_create(
        self,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """Track a project CREATE operation."""
        if not self._enabled:
            return

        event: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
        }

        if user_id:
            event["user_id"] = user_id
        if team_id:
            event["team_id"] = team_id
        if project_id:
            event["project_id"] = project_id

        self._events["project_create"].append(event)

    def track_project_update(
        self,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """Track a project UPDATE operation."""
        if not self._enabled:
            return

        event: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
        }

        if user_id:
            event["user_id"] = user_id
        if team_id:
            event["team_id"] = team_id
        if project_id:
            event["project_id"] = project_id

        self._events["project_update"].append(event)

    def track_project_delete(
        self,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """Track a project DELETE operation."""
        if not self._enabled:
            return

        event: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
        }

        if user_id:
            event["user_id"] = user_id
        if team_id:
            event["team_id"] = team_id
        if project_id:
            event["project_id"] = project_id

        self._events["project_delete"].append(event)

    def track_deployment_create(
        self,
        target: Optional[str] = None,
        force_new: bool = False,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> None:
        """Track a deployment CREATE operation."""
        if not self._enabled:
            return

        event: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
            "force_new": force_new,
        }

        if user_id:
            event["user_id"] = user_id
        if team_id:
            event["team_id"] = team_id
        if project_id:
            event["project_id"] = project_id
        if target:
            event["target"] = target

        self._events["deployment_create"].append(event)

    async def flush(self) -> None:
        """Flush all accumulated events to the telemetry bridge."""
        if not self._enabled or not any(self._events.values()):
            return

        # Build batch of events grouped by type
        batch: Dict[str, list] = {}
        for event_type, events in self._events.items():
            if events:
                batch[event_type] = events

        if not batch:
            return

        try:
            headers = {
                "x-vercel-py-session-id": self.session_id,
                "Content-Type": "application/json",
            }

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    _TELEMETRY_BRIDGE_URL,
                    headers=headers,
                    json=batch,
                )

                if response.status_code == 204:
                    if _TELEMETRY_DEBUG():
                        print(f"Telemetry events tracked")
                else:
                    if _TELEMETRY_DEBUG():
                        print(f"Failed to send telemetry: {response.status_code}")

        except Exception as e:
            if _TELEMETRY_DEBUG():
                print(f"Telemetry error: {e}")

        # Clear all events after flushing
        for event_type in self._events:
            self._events[event_type].clear()

    def reset(self) -> None:
        """Clear accumulated events."""
        for event_type in self._events:
            self._events[event_type].clear()


def _TELEMETRY_DEBUG() -> bool:
    """Check if telemetry debugging is enabled."""
    return os.getenv("VERCEL_TELEMETRY_DEBUG") == "1"

