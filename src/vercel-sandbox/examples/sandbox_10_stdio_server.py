#!/usr/bin/env python3
"""Talk to a stdio JSON-RPC server, such as an MCP server, inside a sandbox.

The server reads newline-delimited requests on stdin and answers each on
stdout. The client sends requests from one task while another matches
responses to them, which is how stdio MCP clients drive their servers.
"""

import asyncio
import json
import subprocess
from datetime import timedelta
from uuid import uuid4

import anyio
from dotenv import load_dotenv

from vercel import sandbox
from vercel.api import session

load_dotenv()

SERVER = """\
import json, sys
for line in sys.stdin:
    request = json.loads(line)
    result = {"tool": request["method"], "args": request["params"]}
    print(json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}), flush=True)
"""


async def main() -> None:
    async with session():
        await _main()


async def _main() -> None:
    name = f"vercel-py-stdio-{uuid4().hex[:12]}"

    async with sandbox.create_sandbox(
        name=name,
        execution_time_limit=timedelta(minutes=2),
    ) as box:
        server = await box.create_process("python", ["-u", "-c", SERVER], stdin=subprocess.PIPE)
        stdin, stdout = server.stdin, server.stdout
        assert stdin is not None and stdout is not None
        requests = [("add", [1, 2]), ("upper", ["héllo"]), ("echo", ["x" * 10_000])]
        responses: dict[int, object] = {}

        async def send_requests() -> None:
            # Closing stdin tells the server there is nothing more to read.
            async with stdin:
                for request_id, (method, params) in enumerate(requests):
                    request = {"jsonrpc": "2.0", "id": request_id, "method": method}
                    line = json.dumps({**request, "params": params}) + "\n"
                    await stdin.send(line.encode())

        async def read_responses() -> None:
            async for line in stdout:
                response = json.loads(line)
                responses[response["id"]] = response["result"]

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(send_requests)
            tasks.start_soon(read_responses)

        for request_id, (method, params) in enumerate(requests):
            assert responses[request_id] == {"tool": method, "args": params}
            print(f"{method}: answered")

        returncode = await server.wait()
        assert returncode == 0, returncode
        print(f"server exited with {returncode} after stdin closed")


if __name__ == "__main__":
    asyncio.run(main())
