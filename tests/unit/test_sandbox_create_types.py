from dataclasses import FrozenInstanceError, is_dataclass

import pytest

from vercel.sandbox import (
    GitSource,
    SandboxResources,
    SandboxValidationError,
    SandboxValidationIssue,
    SnapshotSource,
    TarballSource,
)


def test_public_create_types_are_importable() -> None:
    assert is_dataclass(GitSource)
    assert is_dataclass(TarballSource)
    assert is_dataclass(SnapshotSource)
    assert is_dataclass(SandboxResources)
    assert issubclass(SandboxValidationError, Exception)
    assert is_dataclass(SandboxValidationIssue)


def test_public_create_types_are_frozen() -> None:
    source = GitSource(url="https://github.com/vercel/vercel-py")

    with pytest.raises(FrozenInstanceError):
        object.__setattr__(source, "url", "https://github.com/vercel/next.js")
