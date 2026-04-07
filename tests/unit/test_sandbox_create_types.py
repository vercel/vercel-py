from dataclasses import is_dataclass
from typing import Any

from vercel.sandbox import (
    GitSource,
    Resources,
    SandboxValidationError,
    SandboxValidationIssue,
    SnapshotSource,
    TarballSource,
)


def test_public_create_types_are_importable() -> None:
    assert is_dataclass(GitSource)
    assert is_dataclass(TarballSource)
    assert is_dataclass(SnapshotSource)
    assert is_dataclass(Resources)
    assert issubclass(SandboxValidationError, Exception)
    assert is_dataclass(SandboxValidationIssue)


def test_public_create_types_are_frozen() -> None:
    assert _is_frozen_dataclass(GitSource)
    assert _is_frozen_dataclass(TarballSource)
    assert _is_frozen_dataclass(SnapshotSource)
    assert _is_frozen_dataclass(Resources)


def _is_frozen_dataclass(value: type[Any]) -> bool:
    return bool(getattr(getattr(value, "__dataclass_params__", None), "frozen", False))
