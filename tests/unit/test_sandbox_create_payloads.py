from vercel._internal.payload import RawPayload, marshal_payload
from vercel._internal.sandbox.core import _build_create_sandbox_payload
from vercel.sandbox import GitSource, SandboxResources


def test_marshal_payload_recursively_camel_cases_without_mutating() -> None:
    source = {
        "snapshot_id": "snap_123",
        "nested_value": {"child_key": 1},
        "items": [{"inner_name": "value"}],
        "__interactive": True,
    }

    payload = marshal_payload(source)

    assert payload == {
        "snapshotId": "snap_123",
        "nestedValue": {"childKey": 1},
        "items": [{"innerName": "value"}],
        "__interactive": True,
    }
    assert source["nested_value"] == {"child_key": 1}
    assert source["items"] == [{"inner_name": "value"}]


def test_marshal_payload_respects_raw_payload_subtrees() -> None:
    payload = marshal_payload(
        {
            "env": RawPayload({"NODE_ENV": "test"}),
            "outer_key": {"inner_key": 1},
        }
    )

    assert payload == {
        "env": {"NODE_ENV": "test"},
        "outerKey": {"innerKey": 1},
    }


def test_build_create_payload_uses_dataclass_to_payload() -> None:
    payload = _build_create_sandbox_payload(
        project_id="prj_123",
        source=GitSource(
            url="https://github.com/vercel/vercel-py",
            revision="main",
        ),
        resources=SandboxResources(vcpus=2, memory=4096),
        env={"NODE_ENV": "test"},
        interactive=True,
    )

    assert payload == {
        "projectId": "prj_123",
        "source": {
            "type": "git",
            "url": "https://github.com/vercel/vercel-py",
            "revision": "main",
        },
        "resources": {"vcpus": 2, "memory": 4096},
        "env": {"NODE_ENV": "test"},
        "__interactive": True,
    }
