"""Live API test for the unstable Sandbox create boundary."""

from __future__ import annotations

from datetime import timedelta

import pytest

from .conftest import requires_sandbox_credentials


@requires_sandbox_credentials
@pytest.mark.live
async def test_unstable_sandbox_create_live(unique_test_name: str) -> None:
    from vercel.unstable import Session
    from vercel.unstable.sandbox import SandboxAPIError, SandboxCreateParams, SandboxStatus

    # Unstable Sandbox does not expose stop/delete yet. Keep the sandbox
    # nonpersistent and short-lived so server-side lifetime handles cleanup.
    async with Session() as session:
        try:
            sandbox = await session.sandbox.create(
                SandboxCreateParams(
                    runtime="python3.13",
                    name=unique_test_name,
                    persistent=False,
                    timeout=timedelta(seconds=60),
                ),
                wait=True,
                timeout=timedelta(seconds=120),
            )
        except SandboxAPIError as exc:
            if exc.status_code == 403:
                pytest.skip("configured live credentials cannot create unstable sandboxes")
            raise

    assert sandbox.name == unique_test_name
    assert sandbox.current_session is not None
    assert sandbox.current_session.id
    assert sandbox.current_session.status is SandboxStatus.RUNNING
