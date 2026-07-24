#!/usr/bin/env python3
"""Run a small sync script-wrapper workflow in a Sandbox."""

from datetime import timedelta
from uuid import uuid4

from dotenv import load_dotenv

from vercel.api import session
from vercel.sandbox import sync as sandbox

load_dotenv()


def run_script(input_text: str, script: str) -> str:
    name = f"vercel-py-script-{uuid4().hex[:12]}"

    # The sync API mirrors the async API for scripts and CLIs that do not run an
    # event loop. Using the sandbox as a context manager stops and destroys it
    # on exit.
    #
    # To keep a sandbox around across calls, create it without `with`:
    #
    #     box = sandbox.create_sandbox(...)
    #
    # and call `box.destroy()` once the longer-lived workflow is complete.
    with (
        session(),
        sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=timedelta(minutes=1),
        ) as box,
    ):
        # `box` already points at the sandbox's current runtime session. Commands
        # live on the handle and workspace operations live on `box.fs`.
        box.fs.mkdir("workspace")
        with box.fs.batch() as batch:
            batch.write_text("workspace/tool.py", script)
            batch.write_text("workspace/input.txt", input_text)

        # `run_process` streams output to this process while waiting. Use
        # `create_process` when signaling or separate readers are needed.
        box.run_process(
            "python",
            [
                "workspace/tool.py",
                "--input",
                "workspace/input.txt",
                "--output",
                "workspace/output.json",
                "--uppercase",
            ],
            check=True,
        )

        return box.fs.read_text("workspace/output.json")


def main() -> None:
    # The concrete input is intentionally kept below the reusable wrapper so the
    # important SDK flow remains the first thing to read.
    output = run_script(
        "ship async example\nship sync example\nkeep logs visible\n",
        script="""\
import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--uppercase", action="store_true")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    print(f"reading {input_path}", file=sys.stderr)
    lines = input_path.read_text().splitlines()
    if args.uppercase:
        lines = [line.upper() for line in lines]

    result = {
        "line_count": len(lines),
        "preview": lines[:3],
    }
    output_path.write_text(json.dumps(result, indent=2) + "\\n")
    print(f"wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
""",
    )
    print(output, end="")


if __name__ == "__main__":
    main()
