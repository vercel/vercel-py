"""Shared live scenarios for the Sandbox public API."""

import asyncio
import hashlib
import subprocess
import tempfile
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import vercel
from vercel import sandbox
from vercel.sandbox import (
    NetworkPolicy,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkPolicyTransform,
    SandboxApiError,
    SandboxFilesystemWriteError,
    SandboxPathNotFoundError,
    SandboxQueryByName,
    SandboxStatus,
    SnapshotSource,
    TagFilter,
    sync as sandbox_sync,
)

_SESSION_STOP_TIMEOUT_SECONDS = 60
_SESSION_STOP_POLL_INTERVAL_SECONDS = 0.5


@dataclass(frozen=True, slots=True)
class WorkspaceObservation:
    stdout: str
    stderr: str
    output: str
    exit_code: int | None
    context_cleaned_up: bool


@dataclass(frozen=True, slots=True)
class PersistentObservation:
    discovered: bool
    tags_preserved: bool
    routes_preserved: bool
    project_id_preserved: bool
    current_session_preserved: bool
    routes_cleared: bool
    snapshot_fetched: bool
    snapshot_listed: bool
    restored_content: str
    session_output: str
    session_exit_code: int | None
    session_cleaned_up: bool
    resources_cleaned_up: bool


@dataclass(frozen=True, slots=True)
class ProcessFilesystemObservation:
    stdout: str
    stderr: str
    returncode: int
    terminated_returncode: int
    timed_out_returncode: int
    missing_executable_failed: bool
    text: str
    binary: bytes
    missing_read_failed: bool
    invalid_write_failed: bool


@dataclass(frozen=True, slots=True)
class StreamingTransferObservation:
    digest_matches: bool
    empty_matches: bool
    explicit_mode: str
    missing_download_failed: bool


@dataclass(frozen=True, slots=True)
class NetworkPolicyObservation:
    allow_all_created: bool
    custom_returned: bool
    header_names_redacted: bool
    deny_all_returned: bool
    resources_cleaned_up: bool


class _ScenarioDriver:
    @asynccontextmanager
    async def session(self) -> AsyncIterator[None]:
        raise NotImplementedError
        yield

    @asynccontextmanager
    async def ephemeral_sandbox(self, name: str) -> AsyncIterator[Any]:
        raise NotImplementedError
        yield

    async def create_persistent(self, name: str, tags: dict[str, str]) -> Any:
        raise NotImplementedError

    async def create_with_network_policy(self, name: str, network_policy: NetworkPolicy) -> Any:
        raise NotImplementedError

    async def update_network_policy(self, box: Any, network_policy: NetworkPolicy) -> Any:
        raise NotImplementedError

    async def update(
        self,
        box: Any,
        *,
        ports: list[int] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        raise NotImplementedError

    async def restore(self, name: str, snapshot_id: str) -> Any:
        raise NotImplementedError

    async def query_sandboxes(self, name_prefix: str, tag: TagFilter) -> list[Any]:
        raise NotImplementedError

    async def get_sandbox(self, name: str) -> Any:
        raise NotImplementedError

    async def query_snapshots(self, name: str) -> list[Any]:
        raise NotImplementedError

    async def get_snapshot(self, snapshot_id: str) -> Any:
        raise NotImplementedError

    async def mkdir(self, box: Any, path: str) -> None:
        raise NotImplementedError

    async def write_files(
        self, box: Any, files: list[tuple[str, str]], *, cwd: str | None = None
    ) -> None:
        raise NotImplementedError

    async def read_text(self, box: Any, path: str) -> str:
        raise NotImplementedError

    async def write_text(self, box: Any, path: str, text: str) -> None:
        raise NotImplementedError

    async def read_bytes(self, box: Any, path: str) -> bytes:
        raise NotImplementedError

    async def write_bytes(self, box: Any, path: str, data: bytes) -> None:
        raise NotImplementedError

    async def write_path(
        self, box: Any, remote: str, local: Path, *, mode: int | None = None
    ) -> None:
        raise NotImplementedError

    async def read_path(self, box: Any, remote: str, local: Path) -> int:
        raise NotImplementedError

    async def exists(self, box: Any, path: str) -> bool:
        raise NotImplementedError

    async def is_file(self, box: Any, path: str) -> bool:
        raise NotImplementedError

    async def is_dir(self, box: Any, path: str) -> bool:
        raise NotImplementedError

    async def listdir(self, box: Any, path: str) -> list[Any]:
        raise NotImplementedError

    async def rename(self, box: Any, source: str, destination: str) -> None:
        raise NotImplementedError

    async def remove(self, box: Any, path: str) -> None:
        raise NotImplementedError

    async def create_process(
        self, box: Any, command: str, args: list[str], *, kill_after: float | None = None
    ) -> Any:
        raise NotImplementedError

    async def wait(self, command: Any) -> int | None:
        raise NotImplementedError

    async def read_process_streams(self, process: Any) -> tuple[str, str]:
        raise NotImplementedError

    async def terminate(self, process: Any) -> None:
        raise NotImplementedError

    async def missing_read_failed(self, box: Any) -> bool:
        raise NotImplementedError

    async def invalid_write_failed(self, box: Any) -> bool:
        raise NotImplementedError

    async def missing_executable_failed(self, box: Any) -> bool:
        raise NotImplementedError

    async def snapshot(self, box: Any) -> Any:
        raise NotImplementedError

    async def run_independent_session(self, box: Any) -> tuple[str, int | None, bool]:
        raise NotImplementedError

    async def delete_snapshot(self, snapshot: Any) -> None:
        raise NotImplementedError

    async def destroy(self, box: Any) -> None:
        raise NotImplementedError


class AsyncDriver(_ScenarioDriver):
    @asynccontextmanager
    async def session(self) -> AsyncIterator[None]:
        async with vercel.session():
            yield

    @asynccontextmanager
    async def ephemeral_sandbox(self, name: str) -> AsyncIterator[Any]:
        async with sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=timedelta(minutes=2),
        ) as box:
            yield box

    async def create_persistent(self, name: str, tags: dict[str, str]) -> Any:
        return await sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            persistent=True,
            ports=[3000],
            execution_time_limit=timedelta(minutes=2),
            tags=tags,
        )

    async def create_with_network_policy(self, name: str, network_policy: NetworkPolicy) -> Any:
        return await sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=timedelta(minutes=2),
            network_policy=network_policy,
        )

    async def update_network_policy(self, box: Any, network_policy: NetworkPolicy) -> Any:
        return await box.update_network_policy(network_policy)

    async def update(
        self,
        box: Any,
        *,
        ports: list[int] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        await box.update(ports=ports, tags=tags)

    async def restore(self, name: str, snapshot_id: str) -> Any:
        return await sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            source=SnapshotSource(snapshot_id=snapshot_id),
        )

    async def query_sandboxes(self, name_prefix: str, tag: TagFilter) -> list[Any]:
        return [
            item
            async for item in sandbox.query_sandboxes(
                query=SandboxQueryByName(name_prefix=name_prefix, tag=tag)
            )
        ]

    async def get_sandbox(self, name: str) -> Any:
        return await sandbox.get_sandbox(name=name)

    async def query_snapshots(self, name: str) -> list[Any]:
        return [item async for item in sandbox.query_snapshots(name=name)]

    async def get_snapshot(self, snapshot_id: str) -> Any:
        return await sandbox.get_snapshot(snapshot_id=snapshot_id)

    async def mkdir(self, box: Any, path: str) -> None:
        await box.fs.mkdir(path)

    async def write_files(
        self, box: Any, files: list[tuple[str, str]], *, cwd: str | None = None
    ) -> None:
        async with box.fs.batch(cwd=cwd) as batch:
            for path, content in files:
                batch.write_text(path, content)

    async def read_text(self, box: Any, path: str) -> str:
        return await box.fs.read_text(path)

    async def write_text(self, box: Any, path: str, text: str) -> None:
        async with box.fs.open(path, "w") as target:
            await target.write(text)

    async def read_bytes(self, box: Any, path: str) -> bytes:
        return await box.fs.read_bytes(path)

    async def write_bytes(self, box: Any, path: str, data: bytes) -> None:
        await box.fs.write_bytes(path, data)

    async def write_path(
        self, box: Any, remote: str, local: Path, *, mode: int | None = None
    ) -> None:
        import anyio

        async with (
            await anyio.open_file(local, "rb") as source,
            box.fs.open(remote, "wb", permissions=mode) as target,
        ):
            while chunk := await source.read(64 * 1024):
                await target.write(chunk)

    async def read_path(self, box: Any, remote: str, local: Path) -> int:
        import anyio

        copied = 0
        async with (
            box.fs.open(remote, "rb") as source,
            await anyio.open_file(local, "wb") as target,
        ):
            while chunk := await source.read(64 * 1024):
                await target.write(chunk)
                copied += len(chunk)
        return copied

    async def exists(self, box: Any, path: str) -> bool:
        return await box.fs.exists(path)

    async def is_file(self, box: Any, path: str) -> bool:
        return await box.fs.is_file(path)

    async def is_dir(self, box: Any, path: str) -> bool:
        return await box.fs.is_dir(path)

    async def listdir(self, box: Any, path: str) -> list[Any]:
        return await box.fs.listdir(path)

    async def rename(self, box: Any, source: str, destination: str) -> None:
        await box.fs.rename(source, destination)

    async def remove(self, box: Any, path: str) -> None:
        await box.fs.remove(path)

    async def create_process(
        self, box: Any, command: str, args: list[str], *, kill_after: float | None = None
    ) -> Any:
        return await box.create_process(command, args, kill_after=kill_after)

    async def wait(self, command: Any) -> int | None:
        return await command.wait()

    async def read_process_streams(self, process: Any) -> tuple[str, str]:
        return await process.communicate()

    async def terminate(self, process: Any) -> None:
        await process.terminate()

    async def missing_read_failed(self, box: Any) -> bool:
        try:
            await box.fs.read_bytes("missing")
        except SandboxPathNotFoundError:
            return True
        return False

    async def invalid_write_failed(self, box: Any) -> bool:
        try:
            await box.fs.write_text("/", "invalid")
        except SandboxFilesystemWriteError:
            return True
        return False

    async def missing_executable_failed(self, box: Any) -> bool:
        try:
            await box.create_process("vercel-py-missing-executable")
        except SandboxApiError as error:
            return error.code == "executable_not_found"
        return False

    async def snapshot(self, box: Any) -> Any:
        return await box.snapshot()

    async def run_independent_session(self, box: Any) -> tuple[str, int | None, bool]:
        async with sandbox.resume_sandbox(name=box.name, project_id=box.project_id) as resumed:
            command = await resumed.run_process(
                "printf", ["session follow-up\n"], stdout=subprocess.PIPE
            )
            assert command.stdout is not None
            output = command.stdout
            exit_code = command.returncode
            runtime_session = resumed.current_session
            assert runtime_session is not None
        deadline = time.monotonic() + _SESSION_STOP_TIMEOUT_SECONDS
        while runtime_session.status is not SandboxStatus.STOPPED:
            if time.monotonic() >= deadline:
                return output, exit_code, False
            await asyncio.sleep(_SESSION_STOP_POLL_INTERVAL_SECONDS)
            await runtime_session.refresh()
        return output, exit_code, True

    async def delete_snapshot(self, snapshot: Any) -> None:
        await snapshot.delete()

    async def destroy(self, box: Any) -> None:
        await box.destroy()


class SyncDriver(_ScenarioDriver):
    @asynccontextmanager
    async def session(self) -> AsyncIterator[None]:
        with vercel.session():
            yield

    @asynccontextmanager
    async def ephemeral_sandbox(self, name: str) -> AsyncIterator[Any]:
        with sandbox_sync.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=timedelta(minutes=2),
        ) as box:
            yield box

    async def create_persistent(self, name: str, tags: dict[str, str]) -> Any:
        return sandbox_sync.create_sandbox(
            name=name,
            runtime="python3.13",
            persistent=True,
            ports=[3000],
            execution_time_limit=timedelta(minutes=2),
            tags=tags,
        )

    async def create_with_network_policy(self, name: str, network_policy: NetworkPolicy) -> Any:
        return sandbox_sync.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=timedelta(minutes=2),
            network_policy=network_policy,
        )

    async def update_network_policy(self, box: Any, network_policy: NetworkPolicy) -> Any:
        return box.update_network_policy(network_policy)

    async def update(
        self,
        box: Any,
        *,
        ports: list[int] | None = None,
        tags: dict[str, str] | None = None,
    ) -> None:
        box.update(ports=ports, tags=tags)

    async def restore(self, name: str, snapshot_id: str) -> Any:
        return sandbox_sync.create_sandbox(
            name=name,
            runtime="python3.13",
            source=sandbox_sync.SnapshotSource(snapshot_id=snapshot_id),
        )

    async def query_sandboxes(self, name_prefix: str, tag: TagFilter) -> list[Any]:
        return list(
            sandbox_sync.query_sandboxes(
                query=sandbox_sync.SandboxQueryByName(
                    name_prefix=name_prefix,
                    tag=sandbox_sync.TagFilter(key=tag.key, value=tag.value),
                ),
            )
        )

    async def get_sandbox(self, name: str) -> Any:
        return sandbox_sync.get_sandbox(name=name)

    async def query_snapshots(self, name: str) -> list[Any]:
        return list(sandbox_sync.query_snapshots(name=name))

    async def get_snapshot(self, snapshot_id: str) -> Any:
        return sandbox_sync.get_snapshot(snapshot_id=snapshot_id)

    async def mkdir(self, box: Any, path: str) -> None:
        box.fs.mkdir(path)

    async def write_files(
        self, box: Any, files: list[tuple[str, str]], *, cwd: str | None = None
    ) -> None:
        with box.fs.batch(cwd=cwd) as batch:
            for path, content in files:
                batch.write_text(path, content)

    async def read_text(self, box: Any, path: str) -> str:
        return box.fs.read_text(path)

    async def write_text(self, box: Any, path: str, text: str) -> None:
        with box.fs.open(path, "w") as target:
            target.write(text)

    async def read_bytes(self, box: Any, path: str) -> bytes:
        return box.fs.read_bytes(path)

    async def write_bytes(self, box: Any, path: str, data: bytes) -> None:
        box.fs.write_bytes(path, data)

    async def write_path(
        self, box: Any, remote: str, local: Path, *, mode: int | None = None
    ) -> None:
        with (
            local.open("rb") as source,
            box.fs.open(remote, "wb", permissions=mode) as target,
        ):
            while chunk := source.read(64 * 1024):
                target.write(chunk)

    async def read_path(self, box: Any, remote: str, local: Path) -> int:
        copied = 0
        with box.fs.open(remote, "rb") as source, local.open("wb") as target:
            while chunk := source.read(64 * 1024):
                target.write(chunk)
                copied += len(chunk)
        return copied

    async def exists(self, box: Any, path: str) -> bool:
        return box.fs.exists(path)

    async def is_file(self, box: Any, path: str) -> bool:
        return box.fs.is_file(path)

    async def is_dir(self, box: Any, path: str) -> bool:
        return box.fs.is_dir(path)

    async def listdir(self, box: Any, path: str) -> list[Any]:
        return box.fs.listdir(path)

    async def rename(self, box: Any, source: str, destination: str) -> None:
        box.fs.rename(source, destination)

    async def remove(self, box: Any, path: str) -> None:
        box.fs.remove(path)

    async def create_process(
        self, box: Any, command: str, args: list[str], *, kill_after: float | None = None
    ) -> Any:
        return box.create_process(command, args, kill_after=kill_after)

    async def wait(self, command: Any) -> int | None:
        return command.wait()

    async def read_process_streams(self, process: Any) -> tuple[str, str]:
        return process.communicate()

    async def terminate(self, process: Any) -> None:
        process.terminate()

    async def missing_read_failed(self, box: Any) -> bool:
        try:
            box.fs.read_bytes("missing")
        except SandboxPathNotFoundError:
            return True
        return False

    async def invalid_write_failed(self, box: Any) -> bool:
        try:
            box.fs.write_text("/", "invalid")
        except SandboxFilesystemWriteError:
            return True
        return False

    async def missing_executable_failed(self, box: Any) -> bool:
        try:
            box.create_process("vercel-py-missing-executable")
        except SandboxApiError as error:
            return error.code == "executable_not_found"
        return False

    async def snapshot(self, box: Any) -> Any:
        return box.snapshot()

    async def run_independent_session(self, box: Any) -> tuple[str, int | None, bool]:
        with sandbox_sync.resume_sandbox(name=box.name, project_id=box.project_id) as resumed:
            command = resumed.run_process("printf", ["session follow-up\n"], stdout=subprocess.PIPE)
            assert command.stdout is not None
            output = command.stdout
            exit_code = command.returncode
            runtime_session = resumed.current_session
            assert runtime_session is not None
        deadline = time.monotonic() + _SESSION_STOP_TIMEOUT_SECONDS
        while runtime_session.status is not SandboxStatus.STOPPED:
            if time.monotonic() >= deadline:
                return output, exit_code, False
            await asyncio.sleep(_SESSION_STOP_POLL_INTERVAL_SECONDS)
            runtime_session.refresh()
        return output, exit_code, True

    async def delete_snapshot(self, snapshot: Any) -> None:
        snapshot.delete()

    async def destroy(self, box: Any) -> None:
        box.destroy()


async def workspace_command_flow(driver: _ScenarioDriver, name: str) -> WorkspaceObservation:
    context_cleaned_up = False
    async with driver.session():
        async with driver.ephemeral_sandbox(name) as box:
            await driver.mkdir(box, "workspace")
            await driver.write_files(
                box,
                [
                    (
                        "tool.py",
                        "from pathlib import Path\n"
                        "import sys\n"
                        "value = Path('workspace/input.txt').read_text().strip()\n"
                        "Path('workspace/output.txt').write_text(value.upper() + '\\n')\n"
                        "print('stdout:' + value)\n"
                        "print('stderr:' + value, file=sys.stderr)\n",
                    ),
                    ("input.txt", "scenario input\n"),
                    ("remove-me.txt", "temporary\n"),
                ],
                cwd="workspace",
            )
            assert await driver.exists(box, "workspace/remove-me.txt")
            assert await driver.is_file(box, "workspace/remove-me.txt")
            assert await driver.is_dir(box, "workspace")
            assert any(
                entry.path == "remove-me.txt" and entry.kind == "file"
                for entry in await driver.listdir(box, "workspace")
            )
            await driver.rename(box, "workspace/remove-me.txt", "workspace/renamed.txt")
            assert await driver.exists(box, "workspace/renamed.txt")
            await driver.remove(box, "workspace/renamed.txt")
            assert not await driver.exists(box, "workspace/renamed.txt")
            command = await driver.create_process(box, "python", ["workspace/tool.py"])
            stdout, stderr = await driver.read_process_streams(command)
            exit_code = await driver.wait(command)
            output = await driver.read_text(box, "workspace/output.txt")
        try:
            await driver.get_sandbox(name)
        except SandboxApiError as error:
            context_cleaned_up = error.status_code == 404

    return WorkspaceObservation(
        stdout=stdout,
        stderr=stderr,
        output=output,
        exit_code=exit_code,
        context_cleaned_up=context_cleaned_up,
    )


async def process_filesystem_flow(
    driver: _ScenarioDriver, name: str
) -> ProcessFilesystemObservation:
    async with driver.session():
        async with driver.ephemeral_sandbox(name) as box:
            await driver.write_text(box, "message.txt", "hello\n")
            await driver.write_bytes(box, "data.bin", b"\x00\xff")

            process = await driver.create_process(
                box,
                "python",
                [
                    "-c",
                    "import sys; print('stdout line'); print('stderr line', file=sys.stderr); "
                    "raise SystemExit(3)",
                ],
            )
            stdout, stderr = await driver.read_process_streams(process)
            returncode = await driver.wait(process)

            sleeper = await driver.create_process(box, "sleep", ["60"])
            await driver.terminate(sleeper)
            terminated_returncode = await driver.wait(sleeper)

            timed = await driver.create_process(box, "sleep", ["60"], kill_after=2.5)
            timed_out_returncode = await driver.wait(timed)

            text = await driver.read_text(box, "message.txt")
            binary = await driver.read_bytes(box, "data.bin")
            missing_read_failed = await driver.missing_read_failed(box)
            invalid_write_failed = await driver.invalid_write_failed(box)
            missing_executable_failed = await driver.missing_executable_failed(box)

    assert returncode is not None
    assert terminated_returncode is not None
    assert timed_out_returncode is not None
    return ProcessFilesystemObservation(
        stdout=stdout,
        stderr=stderr,
        returncode=returncode,
        terminated_returncode=terminated_returncode,
        timed_out_returncode=timed_out_returncode,
        missing_executable_failed=missing_executable_failed,
        text=text,
        binary=binary,
        missing_read_failed=missing_read_failed,
        invalid_write_failed=invalid_write_failed,
    )


async def streaming_transfer_flow(
    driver: _ScenarioDriver, name: str
) -> StreamingTransferObservation:
    payload = bytes(range(256)) * 1025
    expected_digest = hashlib.sha256(payload).digest()
    remote_paths = ("large.bin", "empty.bin")

    with tempfile.TemporaryDirectory() as directory:
        source = Path(directory) / "source.bin"
        empty_source = Path(directory) / "empty.bin"
        target = Path(directory) / "target.bin"
        empty_target = Path(directory) / "empty-target.bin"
        missing_target = Path(directory) / "missing.bin"
        source.write_bytes(payload)
        empty_source.write_bytes(b"")

        async with driver.session():
            async with driver.ephemeral_sandbox(name) as box:
                try:
                    await driver.write_path(box, remote_paths[0], source, mode=0o600)
                    await driver.write_path(box, remote_paths[1], empty_source)
                    copied = await driver.read_path(box, remote_paths[0], target)
                    empty_copied = await driver.read_path(box, remote_paths[1], empty_target)

                    command = await driver.create_process(
                        box,
                        "python",
                        [
                            "-c",
                            "import os; print(oct(os.stat('large.bin').st_mode & 0o777))",
                        ],
                    )
                    stdout, _ = await driver.read_process_streams(command)
                    await driver.wait(command)

                    try:
                        await driver.read_path(box, "missing.bin", missing_target)
                    except SandboxPathNotFoundError:
                        missing_download_failed = True
                    else:
                        missing_download_failed = False
                finally:
                    for remote_path in remote_paths:
                        try:
                            await driver.remove(box, remote_path)
                        except SandboxPathNotFoundError:
                            pass

        return StreamingTransferObservation(
            digest_matches=(
                copied == len(payload)
                and hashlib.sha256(target.read_bytes()).digest() == expected_digest
            ),
            empty_matches=empty_copied == 0 and empty_target.read_bytes() == b"",
            explicit_mode=stdout.strip(),
            missing_download_failed=missing_download_failed,
        )


async def network_policy_flow(driver: _ScenarioDriver, name: str) -> NetworkPolicyObservation:
    box = None
    allow_all_created = False
    custom_returned = False
    header_names_redacted = False
    deny_all_returned = False
    cleanup_complete = False
    custom = NetworkPolicy.custom(
        allow={
            "example.com": (),
            "api.github.com": [
                NetworkPolicyRule(
                    transform=[NetworkPolicyTransform(headers={"X-Sandbox-Live": "configured"})]
                )
            ],
        },
        subnets=NetworkPolicySubnets(
            allow=["1.1.1.1/32"],
            deny=["192.0.2.0/24"],
        ),
    )

    async with driver.session():
        try:
            box = await driver.create_with_network_policy(name, NetworkPolicy.allow_all())
            allow_all_created = box.network_policy == NetworkPolicy.allow_all()

            session = await driver.update_network_policy(box, custom)
            returned = session.network_policy
            custom_returned = (
                returned is not None
                and returned.mode == "custom"
                and tuple(returned.allow) == ("example.com", "api.github.com")
                and returned.subnets
                == NetworkPolicySubnets(
                    allow=["1.1.1.1/32"],
                    deny=["192.0.2.0/24"],
                )
            )
            transform = (
                None if returned is None else returned.allow["api.github.com"][0].transform[0]
            )
            header_names_redacted = (
                transform is not None
                and transform.headers is None
                and transform.header_names == ("X-Sandbox-Live",)
            )

            session = await driver.update_network_policy(box, NetworkPolicy.deny_all())
            deny_all_returned = session.network_policy == NetworkPolicy.deny_all()
        finally:
            if box is not None:
                await driver.destroy(box)
            cleanup_complete = True

    return NetworkPolicyObservation(
        allow_all_created=allow_all_created,
        custom_returned=custom_returned,
        header_names_redacted=header_names_redacted,
        deny_all_returned=deny_all_returned,
        resources_cleaned_up=cleanup_complete,
    )


async def persistent_snapshot_flow(driver: _ScenarioDriver, name: str) -> PersistentObservation:
    base = None
    restored = None
    snapshot = None
    cleanup_complete = False
    tags = {"scenario": "standalone-live"}
    updated_tags = {**tags, "updated": "true"}

    async with driver.session():
        try:
            base = await driver.create_persistent(name, tags)
            routes = base.routes
            project_id = base.project_id
            current_session = base.current_session
            assert routes
            assert project_id is not None
            assert current_session is not None
            await driver.update(base, tags=updated_tags)
            routes_preserved = base.routes == routes
            project_id_preserved = base.project_id == project_id
            current_session_preserved = base.current_session is current_session
            await driver.update(base, ports=[])
            routes_cleared = base.routes == ()
            await driver.write_files(base, [("state/message.txt", "restored state\n")])
            discovered = await driver.query_sandboxes(
                name, TagFilter(key="scenario", value="standalone-live")
            )
            found = next((item for item in discovered if item.name == name), None)

            snapshot = await driver.snapshot(base)
            fetched = await driver.get_snapshot(snapshot.id)
            listed = await driver.query_snapshots(name)

            restored = await driver.restore(f"{name}-restored", snapshot.id)
            restored_content = await driver.read_text(restored, "state/message.txt")
            (
                session_output,
                session_exit_code,
                session_cleaned_up,
            ) = await driver.run_independent_session(base)
        finally:
            try:
                if snapshot is not None:
                    await driver.delete_snapshot(snapshot)
            finally:
                try:
                    if restored is not None:
                        await driver.destroy(restored)
                finally:
                    if base is not None:
                        await driver.destroy(base)
            cleanup_complete = True

    return PersistentObservation(
        discovered=found is not None,
        tags_preserved=found is not None and found.tags == updated_tags,
        routes_preserved=routes_preserved,
        project_id_preserved=project_id_preserved,
        current_session_preserved=current_session_preserved,
        routes_cleared=routes_cleared,
        snapshot_fetched=fetched.id == snapshot.id,
        snapshot_listed=any(item.id == snapshot.id for item in listed),
        restored_content=restored_content,
        session_output=session_output,
        session_exit_code=session_exit_code,
        session_cleaned_up=session_cleaned_up,
        resources_cleaned_up=cleanup_complete,
    )
