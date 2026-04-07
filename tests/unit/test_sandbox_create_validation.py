import pytest

from vercel._internal.sandbox.models import GitSource, parse_resources, parse_source
from vercel.sandbox import SandboxResources, SandboxValidationError


def test_parse_source_accumulates_issues() -> None:
    with pytest.raises(SandboxValidationError) as exc_info:
        parse_source(
            {
                "type": "git",
                "depth": 0,
                "username": "scott",
                "unexpected": True,
            }
        )

    issues = {(issue.path, issue.message) for issue in exc_info.value.issues}
    assert ("source.url", "is required") in issues
    assert ("source.depth", "must be a positive integer") in issues
    assert ("source", "git username and password must be provided together") in issues


def test_parse_source_drops_unknown_keys() -> None:
    source = parse_source(
        {
            "type": "git",
            "url": "https://github.com/vercel/vercel-py",
            "revision": "main",
            "unexpected": True,
        }
    )

    assert source is not None
    assert source.to_payload() == {
        "type": "git",
        "url": "https://github.com/vercel/vercel-py",
        "revision": "main",
    }


def test_parse_source_accepts_camel_case_snapshot_id() -> None:
    source = parse_source({"type": "snapshot", "snapshotId": "snap_123"})
    assert source is not None
    assert source.to_payload() == {"type": "snapshot", "snapshot_id": "snap_123"}


def test_parse_source_validates_dataclass_input() -> None:
    with pytest.raises(SandboxValidationError) as exc_info:
        parse_source(GitSource(url="https://github.com/vercel/vercel-py", username="user"))

    assert [(issue.path, issue.message) for issue in exc_info.value.issues] == [
        ("source", "git username and password must be provided together")
    ]


def test_parse_resources_accumulates_issues() -> None:
    with pytest.raises(SandboxValidationError) as exc_info:
        parse_resources({"vcpus": 3, "memory": 4096, "extra": "nope"})

    issues = {(issue.path, issue.message) for issue in exc_info.value.issues}
    assert ("resources.vcpus", "must be even") in issues
    assert ("resources.memory", "must equal resources.vcpus * 2048 (6144)") in issues


def test_parse_resources_accepts_dataclass() -> None:
    resources = parse_resources(SandboxResources(vcpus=2, memory=4096))
    assert resources == SandboxResources(vcpus=2, memory=4096)


def test_parse_resources_drops_unknown_keys() -> None:
    resources = parse_resources({"vcpus": 2, "memory": 4096, "extra": "nope"})

    assert resources == SandboxResources(vcpus=2, memory=4096)
