"""Private option overlay helpers for the stable client surface."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any, TypeVar, cast

from vercel.stable.options import RootOptions

_T = TypeVar("_T")


def merge_mapping(
    base: Mapping[str, str] | None,
    override: Mapping[str, str] | None,
) -> dict[str, str]:
    merged: dict[str, str] = {}
    if base:
        merged.update(base)
    if override:
        merged.update(override)
    return merged


def merge_root_options(
    options: RootOptions,
    *,
    timeout: float | None = None,
) -> RootOptions:
    if timeout is None:
        return options
    return replace(options, timeout=timeout)


def merge_dataclass_options(options: _T, **changes: object) -> _T:
    updates = {key: value for key, value in changes.items() if value is not None}
    if not updates:
        return options
    return cast(_T, replace(cast(Any, options), **updates))


__all__ = ["merge_mapping", "merge_root_options", "merge_dataclass_options"]
