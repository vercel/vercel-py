import asyncio
import os

from dotenv import load_dotenv

from vercel.sandbox import AsyncSandbox as Sandbox

load_dotenv()


PY_CODE = b"""
import os
import sys

print("Hello from Python inside Vercel Sandbox!")
print("Python version:", sys.version)
print("ENV SAMPLE:", os.getenv("SAMPLE_ENV", "<missing>"))
"""


async def main() -> None:
    runtime = os.getenv("SANDBOX_RUNTIME") or None  # e.g., "python311"

    async with await Sandbox.create(timeout=120_000, runtime=runtime) as sandbox:
        # Write a Python file to the sandbox working directory
        await sandbox.write_files(
            [
                {"path": "main.py", "content": PY_CODE},
            ]
        )

        # Try python3 first, then fall back to python
        cmd = await sandbox.run_command_detached(
            "bash",
            [
                "-lc",
                f"cd {sandbox.sandbox.cwd} && (python3 main.py || python main.py)",
            ],
            env={"SAMPLE_ENV": "works"},
        )

        out = await cmd.stdout()
        done = await cmd.wait()
        print("========= OUTPUT =========")
        print(out, end="")
        print("==========================")
        print("exit:", done.exit_code)


if __name__ == "__main__":
    asyncio.run(main())
