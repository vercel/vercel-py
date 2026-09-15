"""Live semantic parity scenarios for `vercel.sandbox`."""

import asyncio
from uuid import uuid4

import anyio
import pytest

from vercel import sandbox
from vercel.api import session
from vercel.sandbox import SandboxCleanupError, SandboxStatus, SandboxTerminalStateError

from ._sandbox_scenarios import (
    AsyncDriver,
    NetworkPolicyObservation,
    PersistentObservation,
    ProcessFilesystemObservation,
    StreamingTransferObservation,
    SyncDriver,
    WorkspaceObservation,
    network_policy_flow,
    persistent_snapshot_flow,
    process_filesystem_flow,
    reconcile_sandbox_drive_cleanup,
    streaming_transfer_flow,
    workspace_command_flow,
)
from .conftest import requires_sandbox_credentials


def _name(scenario: str, mode: str) -> str:
    return f"vercel-py-sandbox-{scenario}-{mode}-{uuid4().hex[:10]}"


def _exception_notes(error: BaseException) -> tuple[str, ...]:
    if not hasattr(BaseException, "add_note"):
        return ()
    return tuple(getattr(error, "__notes__", ()))


def _assert_workspace(result: WorkspaceObservation) -> None:
    assert result == WorkspaceObservation(
        stdout="stdout:scenario input\n",
        stderr="stderr:scenario input\n",
        output="SCENARIO INPUT\n",
        exit_code=0,
        context_cleaned_up=True,
    )


def _assert_persistent(result: PersistentObservation) -> None:
    assert result == PersistentObservation(
        discovered=True,
        tags_preserved=True,
        routes_preserved=True,
        project_id_preserved=True,
        current_session_preserved=True,
        routes_cleared=True,
        snapshot_fetched=True,
        snapshot_listed=True,
        restored_content="restored state\n",
        session_output="session follow-up\n",
        session_exit_code=0,
        session_cleaned_up=True,
        resources_cleaned_up=True,
    )


def _assert_process_filesystem(result: ProcessFilesystemObservation) -> None:
    assert result.stdout == "stdout line\n"
    assert result.stderr == "stderr line\n"
    assert result.returncode == 3
    assert result.terminated_returncode != 0
    assert result.timed_out_returncode != 0
    assert result.missing_executable_failed
    assert result.text == "hello\n"
    assert result.binary == b"\x00\xff"
    assert result.missing_read_failed
    assert result.invalid_write_failed


def _assert_network_policy(result: NetworkPolicyObservation) -> None:
    assert result == NetworkPolicyObservation(
        allow_all_created=True,
        custom_returned=True,
        header_names_redacted=True,
        deny_all_returned=True,
        resources_cleaned_up=True,
    )


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_workspace_command_flow_has_sync_async_semantic_parity() -> None:
    async_result = await workspace_command_flow(AsyncDriver(), _name("workspace", "async"))
    sync_result = await workspace_command_flow(SyncDriver(), _name("workspace", "sync"))

    _assert_workspace(async_result)
    _assert_workspace(sync_result)
    assert async_result == sync_result


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_process_filesystem_flow_has_sync_async_semantic_parity() -> None:
    async_result = await process_filesystem_flow(AsyncDriver(), _name("process-fs", "async"))
    sync_result = await process_filesystem_flow(SyncDriver(), _name("process-fs", "sync"))

    _assert_process_filesystem(async_result)
    _assert_process_filesystem(sync_result)


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_streaming_transfer_flow() -> None:
    expected = StreamingTransferObservation(
        digest_matches=True,
        empty_matches=True,
        explicit_mode="0o600",
        missing_download_failed=True,
    )
    async_result = await streaming_transfer_flow(AsyncDriver(), _name("transfer", "async"))
    sync_result = await streaming_transfer_flow(SyncDriver(), _name("transfer", "sync"))
    assert async_result == expected
    assert sync_result == expected


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_network_policy_flow_has_sync_async_semantic_parity() -> None:
    async_result = await network_policy_flow(AsyncDriver(), _name("network-policy", "async"))
    sync_result = await network_policy_flow(SyncDriver(), _name("network-policy", "sync"))

    _assert_network_policy(async_result)
    _assert_network_policy(sync_result)
    assert async_result == sync_result


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_persistent_snapshot_flow_has_sync_async_semantic_parity() -> None:
    async_result = await persistent_snapshot_flow(AsyncDriver(), _name("persist", "async"))
    sync_result = await persistent_snapshot_flow(SyncDriver(), _name("persist", "sync"))

    _assert_persistent(async_result)
    _assert_persistent(sync_result)
    assert async_result == sync_result


class _CleanupSandbox:
    def __init__(self, calls: list[str], *, stall_destroy: bool = False) -> None:
        self.calls = calls
        self.stall_destroy = stall_destroy

    async def stop(self) -> None:
        self.calls.append("stop")

    async def destroy(self) -> None:
        self.calls.append("destroy")
        if self.stall_destroy:
            await anyio.sleep_forever()


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_drive_persists_files_for_a_mounted_sandbox() -> None:
    suffix = uuid4().hex[:10]
    drive_name = f"vercel-py-drive-{suffix}"
    first_sandbox_name = f"vercel-py-drive-sandbox-first-{suffix}"
    second_sandbox_name = f"vercel-py-drive-sandbox-second-{suffix}"
    probe_path = f"/mnt/cache/probe-{suffix}"
    probe_content = f"drive-live-{suffix}"
    known_sandboxes: dict[str, object] = {}
    original_error: BaseException | None = None
    creating_name: str | None = None
    drive: sandbox.Drive | None = None

    async with session():
        try:
            drive = await sandbox.get_or_create_drive(
                name=drive_name,
                region="iad1",
                max_size_bytes=1024**3,
            )
            assert drive.id
            assert drive.region == "iad1"
            assert drive.project_id

            creating_name = first_sandbox_name
            first_box = await sandbox.create_sandbox(
                name=first_sandbox_name,
                region=drive.region,
                mounts={"/mnt/cache": drive},
            )
            creating_name = None
            known_sandboxes[first_sandbox_name] = first_box
            result = await first_box.run_process(
                "sh",
                ["-lc", f"printf %s {probe_content} > {probe_path}"],
                capture_output=True,
            )
            assert result.returncode == 0
            await first_box.stop()
            await first_box.destroy()
            known_sandboxes.pop(first_sandbox_name)

            creating_name = second_sandbox_name
            second_box = await sandbox.create_sandbox(
                name=second_sandbox_name,
                region=drive.region,
                mounts={"/mnt/cache": drive},
            )
            creating_name = None
            known_sandboxes[second_sandbox_name] = second_box
            result = await second_box.run_process(
                "cat",
                [probe_path],
                capture_output=True,
            )
            assert result.returncode == 0
            assert result.stdout == probe_content
            await second_box.stop()
            await second_box.destroy()
            known_sandboxes.pop(second_sandbox_name)

            matches = [
                item
                async for item in sandbox.query_drives(
                    query=sandbox.DriveQueryByName(name_prefix=drive_name),
                )
                if item.name == drive_name
            ]
            assert [item.id for item in matches] == [drive.id]
        except SandboxTerminalStateError as error:
            if creating_name is not None and error.sandbox is not None:
                known_sandboxes[creating_name] = error.sandbox
            original_error = error
            raise
        except BaseException as error:
            original_error = error
            raise
        finally:
            await reconcile_sandbox_drive_cleanup(
                names=(first_sandbox_name, second_sandbox_name),
                known_sandboxes=known_sandboxes,
                drive_name=drive_name,
                project_id=drive.project_id if drive is not None else None,
                original_error=original_error,
            )


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_fork_does_not_inherit_a_running_source_drive() -> None:
    suffix = uuid4().hex[:10]
    drive_name = f"vercel-py-fork-drive-{suffix}"
    source_name = f"vercel-py-drive-fork-source-{suffix}"
    fork_name = f"vercel-py-drive-fork-target-{suffix}"
    known_sandboxes: dict[str, object] = {}
    original_error: BaseException | None = None
    creating_name: str | None = None
    drive: sandbox.Drive | None = None

    async with session():
        try:
            drive = await sandbox.get_or_create_drive(name=drive_name, region="iad1")
            creating_name = source_name
            source = await sandbox.create_sandbox(
                name=source_name,
                region=drive.region,
                mounts={"/mnt/cache": drive},
            )
            creating_name = None
            known_sandboxes[source_name] = source
            await source.fs.write_text("/mnt/cache/source.txt", "source drive\n")

            creating_name = fork_name
            fork = await sandbox.fork_sandbox(
                source_sandbox=source_name,
                name=fork_name,
                region=drive.region,
            )
            creating_name = None
            known_sandboxes[fork_name] = fork

            assert not fork.mounts
            result = await fork.run_process(
                "sh",
                ["-lc", "test ! -e /mnt/cache"],
                capture_output=True,
            )
            assert result.returncode == 0
        except SandboxTerminalStateError as error:
            if creating_name is not None and error.sandbox is not None:
                known_sandboxes[creating_name] = error.sandbox
            original_error = error
            raise
        except BaseException as error:
            original_error = error
            raise
        finally:
            await reconcile_sandbox_drive_cleanup(
                names=(source_name, fork_name),
                known_sandboxes=known_sandboxes,
                drive_name=drive_name,
                project_id=drive.project_id if drive is not None else None,
                original_error=original_error,
            )


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
async def test_snapshot_mount_and_next_session_mount_updates() -> None:
    suffix = uuid4().hex[:10]
    drive_name = f"vercel-py-snapshot-update-drive-{suffix}"
    initializer_name = f"vercel-py-drive-initialize-{suffix}"
    snapshot_name = f"vercel-py-drive-snapshot-{suffix}"
    update_name = f"vercel-py-drive-update-{suffix}"
    probe_path = "/mnt/cache/probe.txt"
    known_sandboxes: dict[str, object] = {}
    original_error: BaseException | None = None
    creating_name: str | None = None
    drive: sandbox.Drive | None = None

    async with session():
        try:
            drive = await sandbox.get_or_create_drive(name=drive_name, region="iad1")

            creating_name = initializer_name
            initializer = await sandbox.create_sandbox(
                name=initializer_name,
                region=drive.region,
                mounts={"/mnt/cache": drive},
            )
            creating_name = None
            known_sandboxes[initializer_name] = initializer
            await initializer.fs.write_text(probe_path, "persisted\n")
            await initializer.stop()
            await initializer.destroy()
            known_sandboxes.pop(initializer_name)

            creating_name = snapshot_name
            snapshot_reader = await sandbox.create_sandbox(
                name=snapshot_name,
                region=drive.region,
                mounts={"/mnt/cache": drive.snapshot()},
            )
            creating_name = None
            known_sandboxes[snapshot_name] = snapshot_reader
            assert await snapshot_reader.fs.read_text(probe_path) == "persisted\n"
            write_result = await snapshot_reader.run_process(
                "sh",
                ["-lc", f"printf blocked > {probe_path}"],
                capture_output=True,
            )
            assert write_result.returncode != 0
            await snapshot_reader.stop()
            await snapshot_reader.destroy()
            known_sandboxes.pop(snapshot_name)

            creating_name = update_name
            updated = await sandbox.create_sandbox(
                name=update_name,
                region=drive.region,
                persistent=True,
            )
            creating_name = None
            known_sandboxes[update_name] = updated
            await updated.update(mounts={"/mnt/cache": drive})
            result = await updated.run_process(
                "sh", ["-lc", "test ! -e /mnt/cache"], capture_output=True
            )
            assert result.returncode == 0
            await updated.stop()

            resumed = await sandbox.resume_sandbox(name=update_name)
            known_sandboxes[update_name] = resumed
            assert await resumed.fs.read_text(probe_path) == "persisted\n"
            await resumed.update(mounts={})
            assert await resumed.fs.read_text(probe_path) == "persisted\n"
            await resumed.stop()

            unmounted = await sandbox.resume_sandbox(name=update_name)
            known_sandboxes[update_name] = unmounted
            assert not unmounted.mounts
            result = await unmounted.run_process(
                "sh", ["-lc", f"test ! -e {probe_path}"], capture_output=True
            )
            assert result.returncode == 0
        except SandboxTerminalStateError as error:
            if creating_name is not None and error.sandbox is not None:
                known_sandboxes[creating_name] = error.sandbox
            original_error = error
            raise
        except BaseException as error:
            original_error = error
            raise
        finally:
            await reconcile_sandbox_drive_cleanup(
                names=(initializer_name, snapshot_name, update_name),
                known_sandboxes=known_sandboxes,
                drive_name=drive_name,
                project_id=drive.project_id if drive is not None else None,
                original_error=original_error,
            )
