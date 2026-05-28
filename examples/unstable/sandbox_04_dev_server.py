#!/usr/bin/env python3
"""Prepare and optionally run a dev server in a persistent Git-backed Sandbox."""

import argparse
import asyncio
import hashlib
import json
import sys
import time
from collections.abc import AsyncIterator

from dotenv import load_dotenv

from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    GitSource,
    Sandbox,
    SandboxApiError,
    SandboxCommand,
    SandboxCommandLog,
    SandboxCommandLogStream,
    WriteFile,
)

load_dotenv()

DEFAULT_REPO = "https://github.com/vercel/sandbox-example-next.git"
DEFAULT_CWD = "/vercel/sandbox"
DEFAULT_INSTALL = "npm install --loglevel info"
MARKER_PATH = ".vercel-py-dev-server/install.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--ref")
    parser.add_argument("--runtime", default="node22")
    parser.add_argument("--port", type=int, default=3000)
    parser.add_argument("--install", default=DEFAULT_INSTALL)
    parser.add_argument("--entrypoint")
    parser.add_argument("--name")
    parser.add_argument("--cwd", default=DEFAULT_CWD)
    parser.add_argument("--reinstall", action="store_true")
    parser.add_argument("--destroy", action="store_true")
    return parser.parse_args()


def sandbox_name(*, repo: str, ref: str | None, runtime: str, port: int) -> str:
    key = json.dumps(
        {
            "repo": repo,
            "ref": ref,
            "runtime": runtime,
            "port": port,
        },
        sort_keys=True,
    )
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return f"vercel-py-dev-{digest}"


async def get_or_create_sandbox(
    *,
    name: str,
    repo: str,
    ref: str | None,
    runtime: str,
    port: int,
) -> Sandbox:
    try:
        box = await sandbox.get_sandbox(name=name)
        print(f"resumed sandbox {box.name}")
        return box
    except SandboxApiError as error:
        if error.status_code != 404:
            raise

    source = GitSource(url=repo, revision=ref)
    box = await sandbox.create_sandbox(
        name=name,
        runtime=runtime,
        source=source,
        ports=[port],
        persistent=True,
        execution_time_limit=300_000,
        tags={
            "example": "dev-server",
            "sdk": "vercel-py",
            "runtime": runtime,
        },
    )
    print(f"created sandbox {box.name}")
    return box


async def should_install(
    box: Sandbox,
    *,
    repo: str,
    ref: str | None,
    runtime: str,
    install: str,
    cwd: str,
    reinstall: bool,
) -> bool:
    if reinstall:
        return True

    expected = marker_payload(
        repo=repo,
        ref=ref,
        runtime=runtime,
        install=install,
        cwd=cwd,
    )
    try:
        marker = await box.read_text(MARKER_PATH, cwd=cwd)
    except SandboxApiError as error:
        if error.status_code == 404:
            return True
        raise

    try:
        return json.loads(marker) != expected
    except json.JSONDecodeError:
        return True


def marker_payload(
    *,
    repo: str,
    ref: str | None,
    runtime: str,
    install: str,
    cwd: str,
) -> dict[str, object]:
    return {
        "repo": repo,
        "ref": ref,
        "runtime": runtime,
        "install": install,
        "cwd": cwd,
    }


async def run_shell(box: Sandbox, command: str, *, cwd: str) -> None:
    result = await box.run_command("sh", ["-lc", command], cwd=cwd)
    if result.exit_code != 0:
        raise RuntimeError(f"{command!r} failed with exit code {result.exit_code}")


async def install_dependencies(
    box: Sandbox,
    *,
    repo: str,
    ref: str | None,
    runtime: str,
    install: str,
    cwd: str,
    reinstall: bool,
) -> None:
    if not await should_install(
        box,
        repo=repo,
        ref=ref,
        runtime=runtime,
        install=install,
        cwd=cwd,
        reinstall=reinstall,
    ):
        print("dependencies already installed")
        return

    print(f"running install: {install}")
    await run_shell(box, install, cwd=cwd)
    await box.mkdir(".vercel-py-dev-server", cwd=cwd)
    await box.write_files(
        [
            WriteFile(
                path=MARKER_PATH,
                content=json.dumps(
                    marker_payload(
                        repo=repo,
                        ref=ref,
                        runtime=runtime,
                        install=install,
                        cwd=cwd,
                    ),
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
            )
        ],
        cwd=cwd,
    )
    print("wrote install marker")


async def stream_logs_for(command: SandboxCommand, *, seconds: float) -> None:
    events = command.logs()
    deadline = time.monotonic() + seconds

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return

        try:
            event = await asyncio.wait_for(anext_event(events), timeout=remaining)
        except TimeoutError:
            return
        except StopAsyncIteration:
            return

        write_log_event(event)


async def anext_event(events: AsyncIterator[SandboxCommandLog]) -> SandboxCommandLog:
    return await anext(events)


def write_log_event(event: SandboxCommandLog) -> None:
    if event.stream is SandboxCommandLogStream.STDOUT:
        sys.stdout.write(event.data)
        sys.stdout.flush()
    elif event.stream is SandboxCommandLogStream.STDERR:
        sys.stderr.write(event.data)
        sys.stderr.flush()


def route_url(box: Sandbox, port: int) -> str | None:
    for route in box.routes:
        if route.port == port:
            return route.url
    return None


async def run_entrypoint(
    box: Sandbox,
    *,
    entrypoint: str | None,
    cwd: str,
    port: int,
) -> None:
    if entrypoint is None:
        print("prepared sandbox; pass --entrypoint to start a dev server")
        return

    command = await box.start_command("sh", ["-lc", entrypoint], cwd=cwd)
    print(f"started command {command.id}")

    url = route_url(box, port)
    if url is not None:
        print(f"port {port}: {url}")

    try:
        await stream_logs_for(command, seconds=10)
    finally:
        try:
            await command.kill()
        except SandboxApiError as error:
            if error.status_code not in {404, 409}:
                raise
        print(f"stopped command {command.id}")


async def main() -> None:
    args = parse_args()
    repo: str = args.repo
    ref: str | None = args.ref
    runtime: str = args.runtime
    port: int = args.port
    install: str = args.install
    entrypoint: str | None = args.entrypoint
    name: str | None = args.name
    cwd: str = args.cwd
    reinstall: bool = args.reinstall
    destroy: bool = args.destroy

    sandbox_id = name or sandbox_name(
        repo=repo,
        ref=ref,
        runtime=runtime,
        port=port,
    )
    box = await get_or_create_sandbox(
        name=sandbox_id,
        repo=repo,
        ref=ref,
        runtime=runtime,
        port=port,
    )

    try:
        await install_dependencies(
            box,
            repo=repo,
            ref=ref,
            runtime=runtime,
            install=install,
            cwd=cwd,
            reinstall=reinstall,
        )
        await run_entrypoint(
            box,
            entrypoint=entrypoint,
            cwd=cwd,
            port=port,
        )
    finally:
        if destroy:
            await box.destroy()
            print(f"destroyed sandbox {box.name}")


if __name__ == "__main__":
    asyncio.run(main())
