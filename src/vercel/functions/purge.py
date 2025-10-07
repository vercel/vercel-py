from __future__ import annotations

from typing import Any

from ._context import get_context

__all__ = [
    "invalidate_by_tag",
    "dangerously_delete_by_tag",
]


def invalidate_by_tag(tag: str | list[str]) -> Any:  # noqa: D401
    api = get_context().purge
    if api is None:
        return None
    return api.invalidate_by_tag(tag)  # type: ignore[attr-defined]


def dangerously_delete_by_tag(
    tag: str | list[str],
    *,
    revalidation_deadline_seconds: int | None = None,
) -> Any:  # noqa: D401
    api = get_context().purge
    if api is None:
        return None
    return api.dangerously_delete_by_tag(
        tag,
        revalidation_deadline_seconds=revalidation_deadline_seconds,
    )  # type: ignore[attr-defined]

