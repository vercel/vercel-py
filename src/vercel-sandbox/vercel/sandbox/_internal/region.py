"""Explicit opt-in to the current Vercel Function's region."""

import os
from typing import Final, TypeAlias, final


@final
class CurrentRegion:
    """Use the current function's region when available; otherwise omit it."""

    __slots__ = ()


CURRENT_REGION: Final = CurrentRegion()
RegionInput: TypeAlias = str | CurrentRegion | None


def resolve_region(region: RegionInput) -> str | None:
    """Resolve the opt-in at request time without changing explicit strings."""
    if isinstance(region, CurrentRegion):
        return os.environ.get("VERCEL_REGION") or None
    return region
