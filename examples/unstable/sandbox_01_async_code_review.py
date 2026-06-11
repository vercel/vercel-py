#!/usr/bin/env python3
"""Run a small async code-review workflow in an unstable Sandbox."""

import asyncio
import os
from datetime import timedelta

from dotenv import load_dotenv

from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    NetworkPolicy,
    NetworkPolicyRule,
    NetworkPolicyTransform,
)

load_dotenv()


def github_network_policy(github_token: str | None) -> NetworkPolicy:
    rules: tuple[NetworkPolicyRule, ...] = ()
    if github_token:
        rules = (
            NetworkPolicyRule(
                transform=[
                    NetworkPolicyTransform(
                        headers={
                            "Authorization": f"Bearer {github_token}",
                            "X-GitHub-Api-Version": "2022-11-28",
                        }
                    )
                ]
            ),
        )

    return NetworkPolicy.custom(allow={"api.github.com": rules})


async def review_code(
    files: list[tuple[str, str]],
    review_agent: str,
    github_token: str | None = None,
) -> str:
    # `async with sandbox.create_sandbox(...)` gives you automatic cleanup:
    # leaving the block stops and destroys the sandbox, even if the workflow
    # raises.
    #
    # If you want a persistent sandbox instead, use:
    #
    #     box = await sandbox.create_sandbox(name="foo-box", ...)
    async with sandbox.create_sandbox(
        runtime="python3.13",
        execution_time_limit=timedelta(minutes=1),
        # The token is injected into requests to api.github.com by the network
        # policy. It is never exposed to the sandbox process or filesystem.
        network_policy=github_network_policy(github_token),
    ) as box:
        # The sandbox returned by `create_sandbox` already has a current runtime
        # session. Most workflows can call commands on `box` and filesystem
        # methods on `box.fs`. Use `sandbox.resume_sandbox(...)` when reopening
        # a stopped persistent sandbox.
        await box.fs.mkdir("workspace")
        async with box.fs.batch() as batch:
            for path, content in files:
                batch.write_text(path, content)
            batch.write_text("workspace/review_agent.py", review_agent)

        # `run_process` streams output to this process by default and waits for
        # completion. Use `create_process` when a live process handle is needed.
        await box.run_process(
            "python",
            ["workspace/review_agent.py", "workspace"],
            kill_after=timedelta(seconds=30),
            check=True,
        )
        return await box.fs.read_text("workspace/review.md")


async def main() -> None:
    # The runner data lives below the reusable workflow so the example first
    # shows the SDK shape, then the concrete files used for this demo.
    report = await review_code(
        [
            (
                "workspace/app.py",
                (
                    "def greet(name: str) -> str:\n"
                    "    # TODO: support localized greetings.\n"
                    "    return f'hello, {name}'\n"
                ),
            ),
            (
                "workspace/test_app.py",
                (
                    "from app import greet\n\n"
                    "\n"
                    "def test_greet() -> None:\n"
                    "    assert greet('Vercel') == 'hello, Vercel'\n"
                ),
            ),
        ],
        github_token=os.getenv("GITHUB_TOKEN"),
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
