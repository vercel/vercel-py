"""Telemetry client for tracking SDK usage."""

import atexit
import os
import time
import uuid
from typing import Any, Dict, Optional

import httpx

from .credentials import extract_credentials

_TELEMETRY_ENABLED = os.getenv("VERCEL_TELEMETRY_DISABLED") != "1"
_TELEMETRY_BRIDGE_URL = os.getenv(
    "VERCEL_TELEMETRY_BRIDGE_URL",
    "https://telemetry.vercel.com/api/vercel-py/v1/events",
)


class TelemetryClient:
    """Client for sending telemetry events."""

    def __init__(self, session_id: Optional[str] = None):
        """Initialize telemetry client.

        Args:
            session_id: Unique session ID. If not provided, generates a new one.
        """
        self.session_id = session_id or str(uuid.uuid4())
        self._events: list[Dict[str, Any]] = []
        self._enabled = _TELEMETRY_ENABLED
        # Register flush at exit so telemetry events are sent before program termination
        atexit.register(self._flush_at_exit)

    def track(
        self,
        event: str,
        *,
        user_id: Optional[str] = None,
        team_id: Optional[str] = None,
        project_id: Optional[str] = None,
        token: Optional[str] = None,
        **fields: Any,
    ) -> None:
        """
        Track a generic telemetry event.
        
        This is the single entry point for tracking all telemetry events.
        Use the @telemetry decorator or track() function from tracker module
        instead of calling this directly.
        
        Args:
            event: The event/action being tracked (e.g., 'blob_put', 'cache_get')
            user_id: Optional user ID
            team_id: Optional team ID
            project_id: Optional project ID
            token: Optional token to extract credentials from
            **fields: Additional event fields (whitelisted by schema)
        """
        if not self._enabled:
            return

        # Extract credentials if not explicitly provided
        extracted_user_id, extracted_team_id, extracted_project_id = extract_credentials(
            token=token,
            team_id=team_id,
            project_id=project_id,
            user_id=user_id,
        )
        
        # Use explicitly provided values, fall back to extracted
        final_user_id = user_id or extracted_user_id
        final_team_id = team_id or extracted_team_id
        final_project_id = project_id or extracted_project_id

        # Whitelist fields allowed by the generic schema for vercel_py
        allowed_keys = {
            "access",
            "content_type",
            "size_bytes",
            "multipart",
            "count",
            "ttl_seconds",
            "has_tags",
            "hit",
            "target",
            "force_new",
        }
        event_fields: Dict[str, Any] = {}
        for k, v in fields.items():
            if k in allowed_keys:
                if isinstance(v, float) and v.is_integer():
                    event_fields[k] = int(v)
                else:
                    event_fields[k] = v

        event_data: Dict[str, Any] = {
            "id": str(uuid.uuid4()),
            "event_time": int(time.time() * 1000),
            "session_id": self.session_id,
            "action": event,
        }

        if final_user_id:
            event_data["user_id"] = final_user_id
        if final_team_id:
            event_data["team_id"] = final_team_id
        if final_project_id:
            event_data["project_id"] = final_project_id

        # Merge whitelisted fields
        event_data.update(event_fields)
        self._events.append(event_data)

    def flush(self) -> None:
        """Flush all accumulated events to the telemetry bridge.
        
        This is a synchronous method that can be safely called from atexit
        handlers or from within existing event loops.
        """
        if not self._enabled or not self._events:
            return

        # Batch events by action type for efficient sending
        batch: Dict[str, list] = {}
        for event in self._events:
            action = event.get("action", "unknown")
            if action not in batch:
                batch[action] = []
            batch[action].append(event)

        if not batch:
            return

        try:
            headers = {
                "x-vercel-py-session-id": self.session_id,
                "Content-Type": "application/json",
            }

            # Group all events under "generic" key since we're using generic schema
            payload = {"generic": [event for events in batch.values() for event in events]}

            with httpx.Client(timeout=30.0) as client:
                response = client.post(
                    _TELEMETRY_BRIDGE_URL,
                    headers=headers,
                    json=payload,
                )

                if response.status_code == 204:
                    if _TELEMETRY_DEBUG():
                        print(f"Telemetry events tracked: {len(self._events)} events")
                    # Clear events only on successful delivery
                    self._events.clear()
                else:
                    if _TELEMETRY_DEBUG():
                        print(f"Failed to send telemetry: {response.status_code}")

        except Exception as e:
            if _TELEMETRY_DEBUG():
                print(f"Telemetry error: {e}")

    def reset(self) -> None:
        """Clear accumulated events."""
        self._events.clear()

    def _flush_at_exit(self) -> None:
        """Flush events at program exit (called by atexit)."""
        try:
            self.flush()
        except Exception:
            # Silently fail - don't interrupt program exit
            pass


def _TELEMETRY_DEBUG() -> bool:
    """Check if telemetry debugging is enabled."""
    return os.getenv("VERCEL_TELEMETRY_DEBUG") == "1"
