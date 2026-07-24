"""HTTP configuration constants shared by Vercel API clients."""

from datetime import timedelta

DEFAULT_API_BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = timedelta(seconds=60)


__all__ = ["DEFAULT_API_BASE_URL", "DEFAULT_TIMEOUT"]
