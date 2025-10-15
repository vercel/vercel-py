from __future__ import annotations

from typing import Any

from .context import get_context


def invalidate_by_tag(tag: str | list[str]) -> Any:
    api = get_context().purge
    if api is None:
        return None
    return api.invalidate_by_tag(tag)


def dangerously_delete_by_tag(
    tag: str | list[str],
    *,
    revalidation_deadline_seconds: int | None = None,
) -> Any:
    api = get_context().purge
    if api is None:
        return None
    return api.dangerously_delete_by_tag(
        tag,
        revalidation_deadline_seconds=revalidation_deadline_seconds,
    )
