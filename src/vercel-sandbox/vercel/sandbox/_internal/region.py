"""Explicit opt-in to the current Vercel Function's region."""

import os
from typing import Final, NewType, TypeAlias, final


@final
class CurrentRegion:
    """Use the current function's region when available; otherwise omit it."""

    __slots__ = ()


CURRENT_REGION: Final = CurrentRegion()
RegionInput: TypeAlias = str | CurrentRegion | None
ResolvedRegion = NewType("ResolvedRegion", str)


def resolve_region(region: RegionInput, *, fallback: RegionInput = None) -> ResolvedRegion | None:
    """Select and resolve public inputs before passing them to the service."""
    if region is None:
        region = fallback
    if isinstance(region, CurrentRegion):
        region = os.environ.get("VERCEL_REGION")
    return None if region is None else ResolvedRegion(region)
