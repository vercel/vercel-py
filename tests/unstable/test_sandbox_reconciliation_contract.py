from __future__ import annotations

import pytest

from tests.unstable.fake_sandbox_api import FakeSandboxAPI
from vercel.unstable import Session, SyncSession
from vercel.unstable.auth import (
    AccessTokenCredentials,
    StaticCredentialProvider,
    SyncStaticCredentialProvider,
)
from vercel.unstable.sandbox import SandboxCreateParams, SandboxOptions


@pytest.mark.xfail(
    reason=("Named-sandbox retrieval boundary not yet implemented; reserved for future slice"),
    strict=True,
)
async def test_future_get_name_shape(fake_sandbox_api: FakeSandboxAPI) -> None:
    """Document the expected future `get(name=..., resume=...)` API shape.

    This test exists to prevent later slices from inventing inconsistent
    reconciliation semantics. `get` is the retrieval/resume boundary for
    named sandboxes; `resume` controls whether to restore the latest snapshot
    or start fresh.
    """
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_123",
                    team_id="team_123",
                )
            )
        )
    )

    # Expected shape: get(name=..., resume=bool)
    sandbox = await accessor.get(  # type: ignore[attr-defined]
        name="preview-db", resume=True
    )
    assert sandbox.name == "preview-db"
    assert sandbox.current_session is not None


@pytest.mark.xfail(
    reason=(
        "Named-sandbox at-least-once reconciliation helper not yet "
        "implemented; reserved for future slice"
    ),
    strict=True,
)
async def test_future_get_or_create_shape(fake_sandbox_api: FakeSandboxAPI) -> None:
    """Document the expected future `get_or_create(...)` reconciliation shape.

    This absorbs the case where a cancelled `create(name=...)` may have
    committed server-side, allowing callers to reach a known sandbox state
    without re-implementing upsert logic.
    """
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_123",
                    team_id="team_123",
                )
            )
        )
    )

    # Expected shape: get_or_create accepts SandboxCreateParams and returns
    # a Sandbox handle, preferring an existing named sandbox if it exists.
    sandbox = await accessor.get_or_create(  # type: ignore[attr-defined]
        SandboxCreateParams(name="preview-db", runtime="python3.12")
    )
    assert sandbox.name == "preview-db"
    assert sandbox.current_session is not None


@pytest.mark.xfail(
    reason=("Sync named-sandbox retrieval boundary not yet implemented; reserved for future slice"),
    strict=True,
)
def test_future_sync_get_name_shape(fake_sandbox_api: FakeSandboxAPI) -> None:
    """Document the expected future sync `get(name=..., resume=...)` API shape."""
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=SyncStaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_123",
                    team_id="team_123",
                )
            )
        )
    )

    sandbox = accessor.get(  # type: ignore[attr-defined]
        name="preview-db", resume=True
    )
    assert sandbox.name == "preview-db"
    assert sandbox.current_session is not None


@pytest.mark.xfail(
    reason=(
        "Sync named-sandbox reconciliation helper not yet implemented; reserved for future slice"
    ),
    strict=True,
)
def test_future_sync_get_or_create_shape(fake_sandbox_api: FakeSandboxAPI) -> None:
    """Document the expected future sync `get_or_create(...)` reconciliation shape."""
    session = SyncSession()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=SyncStaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_123",
                    team_id="team_123",
                )
            )
        )
    )

    sandbox = accessor.get_or_create(  # type: ignore[attr-defined]
        SandboxCreateParams(name="preview-db", runtime="python3.12")
    )
    assert sandbox.name == "preview-db"
    assert sandbox.current_session is not None


async def test_create_handle_preserves_snapshot_identifiers(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    """Create handles must preserve snapshot identifiers for future snapshot workflows.

    This is a live test of the current create implementation to confirm the
    snapshot-boundary contract is already satisfied by Slice 10 handle model
    corrections.
    """
    fake_sandbox_api.script_response(
        status_code=201,
        json={
            "sandbox": {
                "name": "my-sandbox",
                "persistent": True,
                "currentSnapshotId": "snap_current_123",
            },
            "session": {
                "id": "sbx_test123",
                "memory": 1024,
                "vcpus": 2,
                "region": "iad1",
                "runtime": "python3.12",
                "timeout": 300000,
                "status": "running",
                "requestedAt": 1,
                "startedAt": 2,
                "cwd": "/vercel/sandbox",
                "sourceSnapshotId": "snap_source_456",
            },
            "routes": [],
        },
    )
    session = Session()
    session._sandbox_transport = fake_sandbox_api
    accessor = session.sandbox.with_options(
        SandboxOptions(
            credential_provider=StaticCredentialProvider(
                AccessTokenCredentials(
                    token="token",
                    project_id="project_123",
                    team_id="team_123",
                )
            )
        )
    )

    sandbox = await accessor.create(SandboxCreateParams(runtime="python3.12"))

    assert sandbox.name == "my-sandbox"
    assert sandbox.current_session is not None
    # Snapshot identifiers must be preserved for future snapshot operations.
    assert sandbox.current_snapshot_id == "snap_current_123"
    assert sandbox.current_session.source_snapshot_id == "snap_source_456"
    # Raw context must preserve the full v2 response for future operations.
    assert sandbox._raw is not None
