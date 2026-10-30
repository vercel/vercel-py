"""Explicit opt-in to the current Vercel Function's region."""

import os
from typing import Final, TypeAlias, final


@final
class CurrentRegion:
    """Use the current function's region when available; otherwise omit it."""

    __slots__ = ()


CURRENT_REGION: Final = CurrentRegion()
RegionInput: TypeAlias = str | CurrentRegion | None


def resolve_region(region: RegionInput, *, fallback: RegionInput = None) -> str | None:
    """Select and resolve public inputs before passing them to the service."""
    if region is None:
        region = fallback
    if isinstance(region, CurrentRegion):
        return os.environ.get("VERCEL_REGION")
    return region
