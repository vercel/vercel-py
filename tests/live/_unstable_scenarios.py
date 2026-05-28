"""Shared live scenarios for the experimental Sandbox public API."""

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

    async def write_files(self, box: Any, files: list[tuple[str, str]]) -> None:
        raise NotImplementedError

    async def read_text(self, box: Any, path: str) -> str:
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
            execution_time_limit=60_000,
        ) as box:
            yield box

    async def create_persistent(self, name: str, tags: dict[str, str]) -> Any:
        return await sandbox.create_sandbox(
            name=name,
            runtime="python3.13",
            persistent=True,
            execution_time_limit=60_000,
            tags=tags,
        )

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
        await box.mkdir(path)

    async def write_files(self, box: Any, files: list[tuple[str, str]]) -> None:
        await box.write_files(
            [self.write_file_type(path=path, content=content) for path, content in files]
        )

    async def read_text(self, box: Any, path: str) -> str:
        return await box.read_text(path)

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
        return output, exit_code, runtime_session.status is SandboxStatus.STOPPED

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
            execution_time_limit=60_000,
        ) as box:
            yield box

    async def create_persistent(self, name: str, tags: dict[str, str]) -> Any:
        return sandbox_sync.create_sandbox(
            name=name,
            runtime="python3.13",
            persistent=True,
            execution_time_limit=60_000,
            tags=tags,
        )

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
        box.mkdir(path)

    async def write_files(self, box: Any, files: list[tuple[str, str]]) -> None:
        box.write_files(
            [self.write_file_type(path=path, content=content) for path, content in files]
        )

    async def read_text(self, box: Any, path: str) -> str:
        return box.read_text(path)

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
        return output, exit_code, runtime_session.status is SandboxStatus.STOPPED

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
                        "workspace/tool.py",
                        "from pathlib import Path\n"
                        "import sys\n"
                        "value = Path('workspace/input.txt').read_text().strip()\n"
                        "Path('workspace/output.txt').write_text(value.upper() + '\\n')\n"
                        "print('stdout:' + value)\n"
                        "print('stderr:' + value, file=sys.stderr)\n",
                    ),
                    ("workspace/input.txt", "scenario input\n"),
                ],
            )
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

    async with driver.session():
        try:
            base = await driver.create_persistent(name, tags)
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
        tags_preserved=found is not None and found.tags == tags,
        snapshot_fetched=fetched.id == snapshot.id,
        snapshot_listed=any(item.id == snapshot.id for item in listed),
        restored_content=restored_content,
        session_output=session_output,
        session_exit_code=session_exit_code,
        session_cleaned_up=session_cleaned_up,
        resources_cleaned_up=cleanup_complete,
    )
