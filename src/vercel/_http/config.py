"""HTTP configuration for Vercel API clients."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

DEFAULT_API_BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = 60.0


@dataclass
class HTTPConfig:
    """Configuration for HTTP requests to Vercel API."""

    base_url: str = DEFAULT_API_BASE_URL
    timeout: float = DEFAULT_TIMEOUT
    token: str | None = None
    default_headers: dict[str, str] = field(default_factory=dict)

    def get_headers(self, bearer: str) -> dict[str, str]:
        """Build request headers with authorization."""
        headers = {
            "authorization": f"Bearer {bearer}",
            "accept": "application/json",
            "content-type": "application/json",
            **self.default_headers,
        }
        return headers


def require_token(token: str | None) -> str:
    """Resolve token from argument or environment, raising if not found."""
    env_token = os.getenv("VERCEL_TOKEN")
    resolved = token or env_token
    if not resolved:
        raise RuntimeError("Missing Vercel API token. Pass token=... or set VERCEL_TOKEN.")
    return resolved


__all__ = ["HTTPConfig", "DEFAULT_API_BASE_URL", "DEFAULT_TIMEOUT", "require_token"]
