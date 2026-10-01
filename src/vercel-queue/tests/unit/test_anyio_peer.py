"""SDK task groups must use the same AnyIO state and errors as user code."""

import anyio
import pytest

from vercel.queue._internal import embedded, lease


@pytest.mark.anyio
async def test_sdk_task_group_runs_user_cancel_scope() -> None:
    finished = anyio.Event()

    async def user_callback() -> None:
        with anyio.move_on_after(0) as scope:
            await anyio.sleep_forever()
        assert scope.cancelled_caught
        finished.set()

    async with embedded.anyio.create_task_group() as tasks:
        tasks.start_soon(user_callback)
        await finished.wait()

    assert embedded.anyio is anyio
    assert lease.anyio is anyio


@pytest.mark.anyio
async def test_user_code_catches_sdk_stream_exception() -> None:
    send, receive = embedded.anyio.create_memory_object_stream[bytes]()
    await send.aclose()
    with pytest.raises(anyio.EndOfStream):
        await receive.receive()
    await receive.aclose()
