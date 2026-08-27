#!/usr/bin/env python3
"""Start Sandbox processes with inherited and captured output."""

import io
import subprocess
from contextlib import redirect_stdout
from datetime import timedelta
from uuid import uuid4

from dotenv import load_dotenv

from vercel.api import session
from vercel.sandbox import sync as sandbox

load_dotenv()


def main() -> None:
    name = f"vercel-py-process-{uuid4().hex[:12]}"

    with (
        session(),
        sandbox.create_sandbox(
            name=name,
            execution_time_limit=timedelta(minutes=1),
        ) as box,
    ):
        inherited = io.StringIO()
        with redirect_stdout(inherited):
            process = box.create_process("printf", ["inherited output\\n"])
            assert process.wait() == 0
        assert inherited.getvalue() == "inherited output\n"

        process = box.create_process(
            "printf",
            ["captured output\\n"],
            stdout=subprocess.PIPE,
        )
        stdout, stderr = process.communicate()
        assert stdout == "captured output\n"
        assert stderr is None

    print("create_process inherited stdout and captured explicit PIPE output")


if __name__ == "__main__":
    main()
