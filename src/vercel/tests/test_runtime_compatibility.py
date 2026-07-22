from datetime import timezone

import vercel.sandbox
import vercel.workflow
from vercel._internal.polyfills import UTC, Self, StrEnum
from vercel.sandbox import SandboxStatus


def test_public_modules_import_under_supported_floor() -> None:
    assert vercel.sandbox is not None
    assert vercel.workflow is not None


def test_sandbox_status_preserves_string_behavior() -> None:
    assert isinstance(SandboxStatus.RUNNING, str)
    assert str(SandboxStatus.RUNNING) == "running"


def test_internal_polyfills_expose_compatibility_surface() -> None:
    assert Self is not None
    assert issubclass(StrEnum, str)
    assert UTC is timezone.utc
