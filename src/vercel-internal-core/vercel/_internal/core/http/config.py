"""HTTP configuration constants shared by Vercel API clients."""

from datetime import timedelta

DEFAULT_API_BASE_URL = "https://api.vercel.com"
DEFAULT_TIMEOUT = timedelta(seconds=60)
# Idle time a pooled connection is kept for reuse. Callers such as agents often
# pause for longer than HTTPX's 5 second default between requests, which made
# the next request pay for a new TCP and TLS handshake.
DEFAULT_KEEPALIVE_EXPIRY = timedelta(seconds=60)


__all__ = ["DEFAULT_API_BASE_URL", "DEFAULT_KEEPALIVE_EXPIRY", "DEFAULT_TIMEOUT"]
