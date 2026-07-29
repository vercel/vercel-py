"""Shared helpers for the Connect examples."""

import os

from vercel.connect import ConnectApiError, ConnectError


def load_environment() -> None:
    """Load `.env.local`, which is where `vercel env pull` writes the OIDC token."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        # python-dotenv is a convenience for local runs, not a requirement.
        return
    load_dotenv()


def require_connector() -> str:
    """Read the connector under test, or explain how to set one."""
    connector = os.environ.get("CONNECTOR")
    if not connector:
        raise SystemExit(
            "Set CONNECTOR to a connector id or UID, for example:\n"
            "    export CONNECTOR=slack/my-bot\n"
            "List the connectors attached to this project with `vercel connect list`."
        )
    return connector


def mask(token: str) -> str:
    """Render a credential safely, keeping just enough to identify it."""
    if len(token) <= 12:
        return "*" * len(token)
    return f"{token[:6]}...{token[-4:]} ({len(token)} chars)"


def describe_error(error: BaseException) -> str:
    """Summarize a Connect failure, including the fields worth reporting."""
    if isinstance(error, ConnectApiError):
        parts = [f"{type(error).__name__}: {error}"]
        if error.code:
            parts.append(f"  code:       {error.code}")
        parts.append(f"  status:     {error.status_code} {error.status_text}")
        if error.request_id:
            parts.append(f"  request id: {error.request_id}")
        if error.vendor:
            parts.append(f"  vendor:     {error.vendor}")
        return "\n".join(parts)
    if isinstance(error, ConnectError):
        return f"{type(error).__name__}: {error}"
    return f"{type(error).__name__}: {error}"


HINTS = """
Common causes:
  - VERCEL_OIDC_TOKEN is missing or expired      -> vercel link && vercel env pull
  - the connector is not attached to this project or environment
                                                 -> vercel connect attach <connector>
  - the connector is attached to a different environment than the one you are in
"""
