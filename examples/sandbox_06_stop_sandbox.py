import asyncio
import os

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


async def main() -> None:
    # TODO: Figure out why python3.13 was causing issues with the runtime initialization.
    runtime = os.getenv("SANDBOX_RUNTIME") or "python3.13"

    # Create sandbox (not using context manager so we can control lifecycle)
    sandbox = await Sandbox.create(timeout=120_000, runtime=runtime)
    try:
        print("Sandbox started:", sandbox.sandbox_id)

        # Start a long-running Python process (60s sleep)
        await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                "PYBIN=$(command -v python3 || command -v python) && $PYBIN -c 'import time; time.sleep(60)'",
            ],
        )

        # Retrieve the same sandbox by ID before stopping
        fetched = await Sandbox.get(sandbox_id=sandbox.sandbox_id)
        try:
            print("Status before stop:", fetched.status)

            # Stop the sandbox
            print("Stopping sandbox...")
            await fetched.stop()
        finally:
            await fetched.client.aclose()

        # Retrieve final status
        refreshed = await Sandbox.get(sandbox_id=sandbox.sandbox_id)
        try:
            print("Status after stop:", refreshed.status)
        finally:
            await refreshed.client.aclose()
    finally:
        await sandbox.client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
