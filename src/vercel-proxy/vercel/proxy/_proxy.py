"""Proxy ASGI router."""

from __future__ import annotations

__all__ = ["Proxy"]


class Proxy:
    """ASGI-callable proxy router.

    Register route handlers with :meth:`route` and an optional fallback
    with :meth:`fallback` (or the *fallback* constructor argument).
    Unmatched requests fall through to ``Response.next()`` by default.
    """
