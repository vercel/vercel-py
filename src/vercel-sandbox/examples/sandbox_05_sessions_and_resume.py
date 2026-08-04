#!/usr/bin/env python3
"""Demonstrate lazy sandbox resume and exact runtime-session scopes."""

import asyncio
from datetime import timedelta
from uuid import uuid4

from dotenv import load_dotenv

from vercel import sandbox
from vercel.api import session
from vercel.sandbox import SandboxStatus

load_dotenv()


async def main() -> None:
    async with session():
        await _main()


async def _main() -> None:
    name = f"vercel-py-sessions-{uuid4().hex[:12]}"

    # Direct acquisition leaves the sandbox running. That is useful here
    # because this example deliberately stops and resumes the same persistent
    # sandbox several times before destroying it.
    box = await sandbox.create_sandbox(
        name=name,
        runtime="python3.13",
        persistent=True,
        execution_time_limit=timedelta(minutes=2),
    )

    try:
        await box.fs.write_text("workspace/message.txt", "hello from persistent storage\n")
        stopped_session_id = box.current_session_id
        await box.stop()

        # Retrieval is passive: this lookup returns the stopped sandbox without
        # starting a replacement runtime session.
        box = await sandbox.get_sandbox(name=name)
        assert box.current_session_id == stopped_session_id
        assert box.current_session is not None
        assert box.current_session.status is SandboxStatus.STOPPED
        print(f"retrieved stopped session {stopped_session_id}")

        # Sandbox-level process and filesystem operations resume lazily. The
        # same box object adopts the replacement session before replaying this
        # command. Use resume_sandbox(name=...) instead when startup should be
        # explicit before the first operation.
        result = await box.run_process(
            "python",
            [
                "-c",
                (
                    "from pathlib import Path; "
                    "print(Path('workspace/message.txt').read_text(), end='')"
                ),
            ],
            capture_output=True,
            check=True,
        )
        resumed_session_id = box.current_session_id
        assert resumed_session_id != stopped_session_id
        print(f"auto-resumed as {resumed_session_id}: {result.stdout}", end="")

        # box.session() makes the session boundary explicit. Managed use stops
        # exactly the session yielded here, even if box later adopts a
        # different session. active = await box.session() is the direct form
        # when the acquired session should remain running.
        async with box.session() as exact_session:
            assert exact_session is box.current_session
            exact_session_id = exact_session.id
            exact_result = await exact_session.run_process(
                "python",
                ["-c", "print('running through an identity-bound session')"],
                capture_output=True,
                check=True,
            )
            print(f"{exact_session_id}: {exact_result.stdout}", end="")

        assert exact_session.status is SandboxStatus.STOPPED
        print(f"managed session {exact_session_id} stopped")

        # The explicit session remains stopped and identity-bound. A later
        # sandbox-level operation can resume box again and adopt a new current
        # session.
        await box.fs.write_text("workspace/after-session.txt", "replacement is active\n")
        assert box.current_session_id != exact_session_id
        print(f"sandbox resumed again as {box.current_session_id}")
    finally:
        await box.destroy()
        print(f"destroyed sandbox {name}")


if __name__ == "__main__":
    asyncio.run(main())
