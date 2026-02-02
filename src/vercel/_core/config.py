"""Client configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

DEFAULT_API_BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = 60.0


@dataclass
class ClientConfig:
    """SDK configuration."""

    access_token: str | None = None
    base_url: str = DEFAULT_API_BASE_URL
    timeout: float = DEFAULT_TIMEOUT
    default_team_id: str | None = None
    default_slug: str | None = None
    headers: dict[str, str] = field(default_factory=dict)

    def resolve_token(self) -> str:
        env_token = os.getenv("VERCEL_TOKEN")
        resolved = self.access_token or env_token
        if not resolved:
            raise RuntimeError(
                "Missing Vercel API token. Pass access_token=... or set VERCEL_TOKEN."
            )
        return resolved

    def get_auth_headers(self) -> dict[str, str]:
        return {
            "authorization": f"Bearer {self.resolve_token()}",
            "accept": "application/json",
            "content-type": "application/json",
            **self.headers,
        }

    def build_url(self, path: str) -> str:
        return self.base_url.rstrip("/") + path
