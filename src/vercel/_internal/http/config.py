"""Transitional aliases for HTTP config now owned by internal core."""

from vercel.internal.core.http.config import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT

__all__ = ["DEFAULT_API_BASE_URL", "DEFAULT_TIMEOUT"]
