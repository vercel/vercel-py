import asyncio
import contextlib
import os
import webbrowser

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


async def main() -> None:
    async with await Sandbox.create(
        source={
            "type": "git",
            "url": "https://github.com/vercel/sandbox-example-next.git",
        },
        resources={"vcpus": 4},
        ports=[3000],
        timeout=600_000,
        runtime="node22",
    ) as sandbox:
        print("Installing dependencies...")
        install = await sandbox.run_command("npm", ["install", "--loglevel", "info"])
        if install.exit_code != 0:
            raise SystemExit("install failed")

        print("Starting the development server...")
        cmd = await sandbox.run_command_detached("npm", ["run", "dev"])  # logs stream below

        # Stream logs and open browser once ready. Ctrl+C is handled by the async context manger.
        ready = asyncio.Event()

        async def logs_and_detect_ready():
            async for line in cmd.logs():
                print(line.data, end="")
                if not ready.is_set() and ("Ready" in line.data or "Local:" in line.data):
                    ready.set()

        logs_task = asyncio.create_task(logs_and_detect_ready())
        try:
            await asyncio.wait_for(ready.wait(), timeout=60)
        except asyncio.TimeoutError:
            pass

        url = sandbox.domain(3000)
        if url:
            print("Open:", url)
            # In CI, avoid opening a browser.
            if not os.getenv("CI"):
                with contextlib.suppress(Exception):
                    webbrowser.open(url)

        # Stop streaming logs and terminate the server so the example exits promptly.
        logs_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await logs_task
        await cmd.kill()


if __name__ == "__main__":
    asyncio.run(main())
