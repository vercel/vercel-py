#!/usr/bin/env python3
"""Use one generic consumer on a sandbox, runtime session, and isolated users."""

from datetime import timedelta
from uuid import uuid4

import anyio
from dotenv import load_dotenv

from vercel import sandbox
from vercel.api import session
from vercel.sandbox import SandboxExecution, SandboxFilesystemError
from vercel.sandbox.sync import SyncSandboxExecution

load_dotenv()


async def greet(executor: SandboxExecution) -> str:
    """A consumer only needs command and file capabilities."""
    await executor.fs.write_text("greeting.txt", "hello from a generic consumer\n")
    result = await executor.run_process("cat", ["greeting.txt"], capture_output=True, check=True)
    assert result.stdout == await executor.fs.read_text("greeting.txt")
    return result.stdout or ""


def greet_sync(executor: SyncSandboxExecution) -> str:
    """The same capability contract is available to synchronous applications."""
    executor.fs.write_text("greeting.txt", "hello from a generic consumer\n")
    result = executor.run_process("cat", ["greeting.txt"], capture_output=True, check=True)
    assert result.stdout == executor.fs.read_text("greeting.txt")
    return result.stdout or ""


async def main() -> None:
    async with session():
        async with sandbox.create_sandbox(
            name=f"vercel-py-users-{uuid4().hex[:12]}",
            execution_time_limit=timedelta(minutes=3),
        ) as box:
            runtime = await box.session()
            alice = await box.create_user("alice")
            bob = await runtime.create_user("bob")
            for executor in (box, runtime, alice, bob):
                print((await greet(executor)).strip())

            await alice.fs.write_text("secret.txt", "alice only", mode=0o600)
            denied = await bob.run_process(
                "cat",
                ["/home/alice/secret.txt"],
                capture_output=True,
            )
            assert denied.returncode != 0
            try:
                await bob.fs.read_text("/home/alice/secret.txt")
            except SandboxFilesystemError:
                print("Bob cannot read Alice's private files")
            else:
                raise AssertionError("user filesystem isolation failed")

            identity = await alice.run_process("whoami", capture_output=True, check=True)
            assert identity.stdout == "alice\n"
            root = box.as_user("root")
            assert root.home_dir == "/root"
            privileged = await root.run_process("pwd", capture_output=True, check=True)
            assert privileged.stdout == "/root\n"
            print(f"{alice.username} works in {alice.home_dir}")

    # Sync APIs use the same example consumer.
    from vercel.sandbox import sync

    with session():
        with sync.create_sandbox(execution_time_limit=timedelta(minutes=2)) as box_sync:
            runtime_sync = box_sync.session()
            alice_sync = runtime_sync.create_user("alice")
            for executor_sync in (box_sync, runtime_sync, alice_sync):
                print(greet_sync(executor_sync).strip())


if __name__ == "__main__":
    anyio.run(main)
