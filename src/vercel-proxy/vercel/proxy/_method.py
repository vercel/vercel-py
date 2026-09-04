"""HTTP method enum."""

from __future__ import annotations

import enum

__all__ = ["Method"]


class Method(enum.Enum):
    """HTTP request method."""

    CONNECT = "CONNECT"
    DELETE = "DELETE"
    GET = "GET"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"
    PATCH = "PATCH"
    POST = "POST"
    PUT = "PUT"
    TRACE = "TRACE"
