#!/usr/bin/env python3
"""Connect the official MCP client to a stdio MCP server inside a sandbox.

The server is the MCP project's reference "everything" server. Instead of
spawning it locally, the client talks to it through the stdin and stdout of a
sandbox process, using a small transport in the shape of the MCP SDK's own
``stdio_client``.
"""

import asyncio
import subprocess
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import uuid4

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from dotenv import load_dotenv
from mcp import Client
from mcp.shared.message import SessionMessage
from mcp.types import TextContent, jsonrpc_message_adapter

from vercel import sandbox
from vercel.api import session

load_dotenv()

SERVER = "@modelcontextprotocol/server-everything@1.0.0"


@asynccontextmanager
async def sandbox_stdio(
    process: sandbox.Process,
) -> AsyncIterator[
    tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
    ]
]:
    """An MCP transport over the stdin and stdout of a sandbox process."""
    stdin, stdout = process.stdin, process.stdout
    assert stdin is not None and stdout is not None
    read_writer, read_stream = anyio.create_memory_object_stream[SessionMessage | Exception](0)
    write_stream, write_reader = anyio.create_memory_object_stream[SessionMessage](0)

    async def read_stdout() -> None:
        async with read_writer:
            async for line in stdout:
                message: SessionMessage | Exception
                try:
                    message = SessionMessage(
                        jsonrpc_message_adapter.validate_json(line, by_name=False)
                    )
                except ValueError as error:
                    message = error
                try:
                    await read_writer.send(message)
                except anyio.BrokenResourceError:
                    return  # The client has disconnected.

    async def write_stdin() -> None:
        # Closing stdin once the client disconnects tells the server to exit.
        async with write_reader, stdin:
            async for message in write_reader:
                line = message.message.model_dump_json(by_alias=True, exclude_unset=True) + "\n"
                await stdin.send(line.encode())

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(read_stdout)
        tasks.start_soon(write_stdin)
        try:
            yield read_stream, write_stream
        finally:
            await write_stream.aclose()
            await read_stream.aclose()


async def main() -> None:
    async with session():
        await _main()


async def _main() -> None:
    name = f"vercel-py-mcp-{uuid4().hex[:12]}"

    async with sandbox.create_sandbox(
        name=name,
        execution_time_limit=timedelta(minutes=5),
    ) as box:
        # The server only logs to stderr, so drop it.
        server = await box.create_process(
            "npx", ["-y", SERVER, "stdio"], stdin=subprocess.PIPE, stderr=subprocess.DEVNULL
        )

        async with Client(sandbox_stdio(server)) as client:
            assert client.server_info is not None
            print(f"connected to {client.server_info.name} {client.server_info.version}")

            tools = [tool.name for tool in (await client.list_tools()).tools]
            print(f"tools: {', '.join(tools)}")

            echo = await client.call_tool("echo", {"message": "héllo from a sandbox"})
            assert echo.content == [TextContent(type="text", text="Echo: héllo from a sandbox")]
            print("echo: answered")

            # Progress notifications arrive while the call is still waiting
            # for its result.
            steps: list[float] = []

            async def on_progress(
                progress: float, total: float | None, message: str | None
            ) -> None:
                steps.append(progress)

            result = await client.call_tool(
                "trigger-long-running-operation",
                {"duration": 2, "steps": 4},
                progress_callback=on_progress,
            )
            assert not result.is_error, result
            assert steps == [1, 2, 3, 4], steps
            print(f"long-running operation reported progress {steps}")

            prompt = await client.get_prompt("args-prompt", {"city": "Paris", "state": "Texas"})
            content = prompt.messages[0].content
            assert isinstance(content, TextContent), content
            print(f"prompt: {content.text}")

        returncode = await server.wait()
        assert returncode == 0, returncode
        print(f"server exited with {returncode} after stdin closed")


if __name__ == "__main__":
    asyncio.run(main())
