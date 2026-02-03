import asyncio
import contextlib
import os
import webbrowser

import httpx
from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


FASTAPI_APP = b"""
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI in Vercel Sandbox!"}
"""


async def main() -> None:
    # TODO: Figure out why python3.13 was causing issues with the runtime initialization.
    runtime = os.getenv("SANDBOX_RUNTIME") or "python3.13"

    async with await Sandbox.create(ports=[8000], timeout=600_000, runtime=runtime) as sandbox:
        # Write the FastAPI application to the sandbox working directory
        await sandbox.write_files(
            [
                {"path": "main.py", "content": FASTAPI_APP},
            ]
        )

        print("Installing FastAPI and Uvicorn...")
        install_cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    "PYBIN=$(command -v python3 || command -v python); "
                    "if [ -z \"$PYBIN\" ]; then echo 'python not found in sandbox'; exit 1; fi; "
                    "$PYBIN -m ensurepip --upgrade || true; "
                    "$PYBIN -m pip install --upgrade pip; "
                    "$PYBIN -m pip install --no-cache-dir fastapi uvicorn"
                ),
            ],
        )

        async for line in install_cmd.logs():
            print(line.data, end="")
        install_done = await install_cmd.wait()
        if install_done.exit_code != 0:
            raise SystemExit("Dependency installation failed")

        print("Starting FastAPI server...")
        cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                (
                    f"cd {sandbox.sandbox.cwd} && "
                    "PYBIN=$(command -v python3 || command -v python) && "
                    "$PYBIN -m uvicorn main:app --host 0.0.0.0 --port 8000"
                ),
            ],
        )

        # Stream logs and open browser once server is ready. Ctrl+C will stop the script.
        ready = asyncio.Event()

        async def logs_and_detect_ready():
            async for line in cmd.logs():
                print(line.data, end="")
                if not ready.is_set() and (
                    "Application startup complete" in line.data or "Uvicorn running on" in line.data
                ):
                    ready.set()

        logs_task = asyncio.create_task(logs_and_detect_ready())
        try:
            await asyncio.wait_for(ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass

        url = sandbox.domain(8000)
        print("Open:", url)

        # In CI, don't try to open a browser. Optionally probe the endpoint once.
        if not os.getenv("CI"):
            with contextlib.suppress(Exception):
                webbrowser.open(url)

        # Probe the server once to ensure it's reachable, then tear down quickly.
        with contextlib.suppress(Exception):
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url)
                print("GET / status:", resp.status_code)

        # Stop streaming logs and terminate the server so the example exits promptly.
        logs_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await logs_task
        await cmd.kill()


if __name__ == "__main__":
    asyncio.run(main())
