"""Proxy request."""

from starlette.requests import Request as StarletteRequest

__all__ = ["Request"]


class Request(StarletteRequest):
    """The incoming request passed to a proxy handler.

    Use ``method``, ``url``, ``headers``, ``query_params``, ``cookies``,
    ``path_params`` and ``client`` to decide how to route it. They work as in
    Starlette.
    """
