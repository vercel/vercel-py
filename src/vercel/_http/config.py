"""HTTP configuration for Vercel API clients."""

from __future__ import annotations

from dataclasses import dataclass, field

DEFAULT_API_BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = 60.0


@dataclass
class HTTPConfig:
    """Configuration for HTTP requests to Vercel API."""

    base_url: str = DEFAULT_API_BASE_URL
    timeout: float | None = DEFAULT_TIMEOUT
    default_headers: dict[str, str] = field(default_factory=dict)


__all__ = ["HTTPConfig", "DEFAULT_API_BASE_URL", "DEFAULT_TIMEOUT"]
