#!/usr/bin/env python3
"""Run a small async code-review workflow in an unstable Sandbox."""

import asyncio
import sys
from datetime import timedelta

from dotenv import load_dotenv

from vercel.unstable import sandbox
from vercel.unstable.sandbox import SandboxCommandLogStream, WriteFile

load_dotenv()


async def review_code(files: list[WriteFile], review_agent: str) -> str:
    # `async with sandbox.create_sandbox(...)` gives you automatic cleanup:
    # leaving the block destroys the sandbox, even if the workflow raises.
    #
    # If you want a persistent sandbox instead, use:
    #
    #     box = await sandbox.create_sandbox(name="foo-box", ...)
    async with sandbox.create_sandbox(
        runtime="python3.13",
        execution_time_limit=timedelta(minutes=1),
    ) as box:
        # The sandbox returned by `create_sandbox` already has a current runtime
        # session. Most workflows can call commands on `box` and filesystem
        # methods on `box.fs`; creating an explicit `box.session()` is an advanced
        # operation for separate runtime-session lifecycles.
        await box.fs.mkdir("workspace")
        await box.fs.write_files(
            [
                *files,
                WriteFile(path="workspace/review_agent.py", content=review_agent),
            ]
        )

        # Start the review command so the example can stream logs while the
        # process runs. `run_command` is simpler when you only need the result.
        cmd = await box.start_command("python", ["workspace/review_agent.py", "workspace"])

        # Sandbox log events preserve the original stream
        async for event in cmd.logs():
            match event.stream:
                case SandboxCommandLogStream.STDOUT:
                    sys.stdout.write(event.data)
                    sys.stdout.flush()
                case SandboxCommandLogStream.STDERR:
                    sys.stderr.write(event.data)
                    sys.stderr.flush()

        finished = await cmd.wait()
        if finished.exit_code != 0:
            raise RuntimeError(f"review failed with exit code {finished.exit_code}")

        return await box.fs.read_text("workspace/review.md")


async def main() -> None:
    # The runner data lives below the reusable workflow so the example first
    # shows the SDK shape, then the concrete files used for this demo.
    report = await review_code(
        [
            WriteFile(
                path="workspace/app.py",
                content=(
                    "def greet(name: str) -> str:\n"
                    "    # TODO: support localized greetings.\n"
                    "    return f'hello, {name}'\n"
                ),
            ),
            WriteFile(
                path="workspace/test_app.py",
                content=(
                    "from app import greet\n\n"
                    "\n"
                    "def test_greet() -> None:\n"
                    "    assert greet('Vercel') == 'hello, Vercel'\n"
                ),
            ),
        ],
        review_agent="""\
import sys
from pathlib import Path


def main() -> int:
    workspace = Path(sys.argv[1])
    python_files = sorted(
        path for path in workspace.glob("*.py") if path.name != "review_agent.py"
    )

    todo_count = 0
    total_lines = 0
    report_lines = ["# Sandbox Code Review", ""]

    for path in python_files:
        content = path.read_text()
        lines = content.splitlines()
        total_lines += len(lines)
        file_todos = sum(1 for line in lines if "TODO" in line)
        todo_count += file_todos
        report_lines.append(f"- {path.name}: {len(lines)} line(s), {file_todos} TODO(s)")

    report_lines.extend(
        [
            "",
            f"Reviewed {len(python_files)} Python file(s) with {total_lines} total line(s).",
        ]
    )

    if todo_count:
        print(f"found {todo_count} TODO comment(s)", file=sys.stderr)
        report_lines.append("Status: needs follow-up.")
    else:
        print("no follow-up items found")
        report_lines.append("Status: clean.")

    (workspace / "review.md").write_text("\\n".join(report_lines) + "\\n")
    print("wrote workspace/review.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
""",
    )
    print(report, end="")


if __name__ == "__main__":
    asyncio.run(main())
