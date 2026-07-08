"""Embedded local queue server for in-process Queue API development.

This module provides a dependency-light Queue API implementation for embedded
local runtime code. It preserves the same client and push-delivery path as the
hosted service: callers send through ``QueueClient``, messages are stored
in a local server, visible messages are leased, and a dispatcher invokes
subscribers through ``accept_and_handle`` with synthetic Vercel Queue push
headers. Successful handlers are acknowledged by the normal client follow-up
path, and failures retry through lease expiry or explicit visibility changes.

Use this for embedded, single-process local mode and tests.

Run an embedded queue server in a test and use a client that sends to
it directly:

    import anyio
    import vercel.queue as vq
    import vercel.queue.embedded as vq_embedded

    @vq.subscribe(topic="jobs*", consumer_group="just-testing")
    async def handle_job(message: vq.Message[dict[str, object]]) -> None:
        print(message.payload)

    async def main() -> None:
        async with vq_embedded.embedded_queue_service() as service:
            client = service.get_async_client()
            await client.send("email", {"subject": "hi"})

    anyio.run(main)


Enable the pytest fixture and use it in tests:

    pytest_plugins = ["vercel.queue.testing.pytest"]

    async def test_queue(embedded_queue_server):
        client = embedded_queue_server.get_async_client()
        message_id = await client.send("emails", {"subject": "hi"}, retention=60)
"""

from ._internal.embedded import (
    create_embedded_queue_app,
    embedded_queue_service,
)

# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "create_embedded_queue_app",
    "embedded_queue_service",
)
