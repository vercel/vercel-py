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
print("SAMPLE_ENV:", os.getenv("SAMPLE_ENV", "<missing>"))
print("EXTRA_ENV:", os.getenv("EXTRA_ENV", "<missing>"))
"""


async def main() -> None:
    runtime = os.getenv("SANDBOX_RUNTIME") or None  # e.g., "python311"

    # Default env vars are inherited by all commands in the sandbox
    async with await Sandbox.create(
        timeout=120_000, runtime=runtime, env={"SAMPLE_ENV": "default"}
    ) as sandbox:
        await sandbox.write_files(
            [
                {"path": "main.py", "content": PY_CODE},
            ]
        )

        run_py = f"cd {sandbox.sandbox.cwd} && (python3 main.py || python main.py)"

        # Run 1: add EXTRA_ENV per-command; SAMPLE_ENV keeps its default
        cmd1 = await sandbox.run_command_detached(
            "bash",
            ["-lc", run_py],
            env={"EXTRA_ENV": "added"},
        )
        done1 = await cmd1.wait()
        print("=== Run 1: add EXTRA_ENV, keep default SAMPLE_ENV ===")
        print(await cmd1.stdout(), end="")
        print(f"exit: {done1.exit_code}\n")

        # Run 2: override SAMPLE_ENV per-command
        cmd2 = await sandbox.run_command_detached(
            "bash",
            ["-lc", run_py],
            env={"SAMPLE_ENV": "overridden", "EXTRA_ENV": "added"},
        )
        done2 = await cmd2.wait()
        print("=== Run 2: override SAMPLE_ENV, add EXTRA_ENV ===")
        print(await cmd2.stdout(), end="")
        print(f"exit: {done2.exit_code}")


if __name__ == "__main__":
    asyncio.run(main())
