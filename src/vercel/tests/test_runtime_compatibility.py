from datetime import timezone

import vercel.workflow
from vercel._internal.polyfills import UTC, Self, StrEnum


def test_public_modules_import_under_supported_floor() -> None:
    assert vercel.workflow is not None


def test_internal_polyfills_expose_compatibility_surface() -> None:
    assert Self is not None
    assert issubclass(StrEnum, str)
    assert UTC is timezone.utc
