"""Sandbox callbacks share the application's AnyIO task state."""

import anyio
import pytest

from vercel.sandbox._internal import async_runtime


@pytest.mark.anyio
async def test_sandbox_task_group_shares_user_cancel_scope() -> None:
    async def user_callback() -> None:
        with anyio.move_on_after(0) as scope:
            await anyio.sleep_forever()
        assert scope.cancelled_caught

    async with async_runtime.anyio.create_task_group() as tasks:
        tasks.start_soon(user_callback)

    assert async_runtime.anyio is anyio
