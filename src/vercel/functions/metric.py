from __future__ import annotations

from collections.abc import Mapping

from vercel.cache.context import get_context


def metric(
    name: str,
    value: int | float,
    tags: Mapping[str, str] | None = None,
) -> None:
    """Report a custom metric for the current Vercel Function invocation."""
    callback = get_context().metric
    if callback is not None:
        callback(name, value, tags)
