"""Shared live scenarios for the experimental Sandbox public API."""

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from vercel import unstable as vercel
from vercel.unstable import sandbox
from vercel.unstable.sandbox import (
    SandboxApiError,
    SandboxQueryByName,
    SandboxStatus,
    SnapshotSource,
    TagFilter,
    WriteFile as AsyncWriteFile,
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


class _ScenarioDriver:
    write_file_type: type[Any]

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

    async def start_command(self, box: Any, command: str, args: list[str]) -> Any:
        raise NotImplementedError

    async def logs(self, command: Any) -> list[tuple[str, str]]:
        raise NotImplementedError

    async def wait(self, command: Any) -> int | None:
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
    write_file_type = AsyncWriteFile

    @asynccontextmanager
    async def session(self) -> AsyncIterator[None]:
        async with vercel.session():
            yield

    @asynccontextmanager
    async def ephemeral_sandbox(self, name: str) -> AsyncIterator[Any]:
        async with sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=120_000,
        ) as box:
            yield box

    async def create_persistent(self, name: str, tags: dict[str, str]) -> Any:
        return await sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            persistent=True,
            ports=[3000],
            execution_time_limit=120_000,
            tags=tags,
        )

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
        return await sandbox.get_sandbox(name=name, resume=False)

    async def query_snapshots(self, name: str) -> list[Any]:
        return [item async for item in sandbox.query_snapshots(name=name)]

    async def get_snapshot(self, snapshot_id: str) -> Any:
        return await sandbox.get_snapshot(snapshot_id=snapshot_id)

    async def mkdir(self, box: Any, path: str) -> None:
        await box.fs.mkdir(path)

    async def write_files(
        self, box: Any, files: list[tuple[str, str]], *, cwd: str | None = None
    ) -> None:
        await box.fs.write_files(
            [self.write_file_type(path=path, content=content) for path, content in files],
            cwd=cwd,
        )

    async def read_text(self, box: Any, path: str) -> str:
        return await box.fs.read_text(path)

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

    async def start_command(self, box: Any, command: str, args: list[str]) -> Any:
        return await box.start_command(command, args)

    async def logs(self, command: Any) -> list[tuple[str, str]]:
        return [(event.stream, event.data) async for event in command.logs()]

    async def wait(self, command: Any) -> int | None:
        return (await command.wait()).exit_code

    async def snapshot(self, box: Any) -> Any:
        return await box.snapshot()

    async def run_independent_session(self, box: Any) -> tuple[str, int | None, bool]:
        async with box.session() as runtime_session:
            command = await runtime_session.run_command("printf", ["session follow-up\n"])
            output = await command.stdout()
            exit_code = command.exit_code
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
    write_file_type = sandbox_sync.WriteFile

    @asynccontextmanager
    async def session(self) -> AsyncIterator[None]:
        with vercel.session():
            yield

    @asynccontextmanager
    async def ephemeral_sandbox(self, name: str) -> AsyncIterator[Any]:
        with sandbox_sync.create_sandbox(
            name=name,
            runtime="python3.13",
            execution_time_limit=120_000,
        ) as box:
            yield box

    async def create_persistent(self, name: str, tags: dict[str, str]) -> Any:
        return sandbox_sync.create_sandbox(
            name=name,
            runtime="python3.13",
            persistent=True,
            ports=[3000],
            execution_time_limit=120_000,
            tags=tags,
        )

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
        return sandbox_sync.get_sandbox(name=name, resume=False)

    async def query_snapshots(self, name: str) -> list[Any]:
        return list(sandbox_sync.query_snapshots(name=name))

    async def get_snapshot(self, snapshot_id: str) -> Any:
        return sandbox_sync.get_snapshot(snapshot_id=snapshot_id)

    async def mkdir(self, box: Any, path: str) -> None:
        box.fs.mkdir(path)

    async def write_files(
        self, box: Any, files: list[tuple[str, str]], *, cwd: str | None = None
    ) -> None:
        box.fs.write_files(
            [self.write_file_type(path=path, content=content) for path, content in files],
            cwd=cwd,
        )

    async def read_text(self, box: Any, path: str) -> str:
        return box.fs.read_text(path)

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

    async def start_command(self, box: Any, command: str, args: list[str]) -> Any:
        return box.start_command(command, args)

    async def logs(self, command: Any) -> list[tuple[str, str]]:
        return [(event.stream, event.data) for event in command.logs()]

    async def wait(self, command: Any) -> int | None:
        return command.wait().exit_code

    async def snapshot(self, box: Any) -> Any:
        return box.snapshot()

    async def run_independent_session(self, box: Any) -> tuple[str, int | None, bool]:
        with box.session() as runtime_session:
            command = runtime_session.run_command("printf", ["session follow-up\n"])
            output = command.stdout()
            exit_code = command.exit_code
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
    logs: list[tuple[str, str]]
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
            command = await driver.start_command(box, "python", ["workspace/tool.py"])
            logs = await driver.logs(command)
            exit_code = await driver.wait(command)
            output = await driver.read_text(box, "workspace/output.txt")
        try:
            await driver.get_sandbox(name)
        except SandboxApiError as error:
            context_cleaned_up = error.status_code == 404

    return WorkspaceObservation(
        stdout="".join(data for stream, data in logs if stream == "stdout"),
        stderr="".join(data for stream, data in logs if stream == "stderr"),
        output=output,
        exit_code=exit_code,
        context_cleaned_up=context_cleaned_up,
    )


async def persistent_snapshot_flow(driver: _ScenarioDriver, name: str) -> PersistentObservation:
    base = None
    restored = None
    snapshot = None
    cleanup_complete = False
    tags = {"scenario": "unstable-live"}
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
                name, TagFilter(key="scenario", value="unstable-live")
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
