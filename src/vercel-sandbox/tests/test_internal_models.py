"""Contracts for internal data models and their runtime references."""

import dataclasses
import importlib
import io
import pkgutil
import subprocess
from pathlib import PurePosixPath
from types import SimpleNamespace

import anyio
import pytest
from pydantic import BaseModel, ValidationError

from vercel import sandbox
from vercel._internal.core.byte_stream import ReadableByteStream, SyncByteStreamRuntime
from vercel.sandbox._internal import state
from vercel.sandbox._internal.api_client import _PreparedSandboxCreation
from vercel.sandbox._internal.async_runtime import (
    _AsyncRecoveryAttempt,
    _CreateSandboxParams,
    _ForkSandboxParams,
    _ResumeSandboxParams,
)
from vercel.sandbox._internal.models import NO_PRIVATE_PARAMETERS
from vercel.sandbox._internal.pagination import (
    QueryDrivesPage,
    QueryDrivesParams,
    QuerySandboxesPage,
    QuerySandboxesParams,
    QuerySessionsPage,
    QuerySessionsParams,
    QuerySnapshotsPage,
    QuerySnapshotsParams,
)
from vercel.sandbox._internal.process_output import ProcessOutputRouter, _OutputTarget
from vercel.sandbox._internal.runtime_common import _UploadFileEntry
from vercel.sandbox._internal.sync_runtime import _SyncRecoveryAttempt


def test_production_package_contains_no_dataclasses() -> None:
    for info in pkgutil.walk_packages(sandbox.__path__, prefix=f"{sandbox.__name__}."):
        module = importlib.import_module(info.name)
        for name, value in vars(module).items():
            if isinstance(value, type) and value.__module__ == info.name:
                assert not dataclasses.is_dataclass(value), f"{info.name}.{name}"


@pytest.mark.parametrize("module", [sandbox, sandbox.sync])
@pytest.mark.parametrize(
    ("handle_name", "payload", "identity"),
    [
        (
            "Process",
            state.ProcessState(
                id="cmd_1",
                name="echo",
                args=(),
                cwd="/",
                session_id="sbx_1",
                returncode=None,
                started_at=1,
            ),
            "id",
        ),
        (
            "Snapshot",
            state.SnapshotState(
                id="snap_1",
                source_session_id="sbx_1",
                region="sfo1",
                regions=("sfo1",),
                status="created",
                size_bytes=0,
            ),
            "id",
        ),
        (
            "Drive",
            state.DriveState(
                id="drive_1",
                name="data",
                project_id="prj_1",
                region="sfo1",
                max_size_bytes=1024,
                current_session_id=None,
                current_sandbox_name=None,
                created_at=1,
                updated_at=1,
            ),
            "name",
        ),
        ("SandboxRuntimeSession", state.SandboxRuntimeSessionState(id="sbx_1"), "id"),
        ("Sandbox", state.SandboxState(name="preview", current_session_id="sbx_1"), "name"),
    ],
)
def test_response_errors_expose_models(module, handle_name, payload, identity) -> None:
    prefix = "Sync" if module is sandbox.sync else ""
    handle = getattr(module, prefix + handle_name)(payload=payload, service=SimpleNamespace())
    unexpected = payload.model_copy(update={identity: "other"})
    with pytest.raises(sandbox.SandboxResponseError) as caught:
        handle._apply_payload(unexpected)
    assert caught.value.data is unexpected
    assert isinstance(caught.value.data, BaseModel)
    assert not dataclasses.is_dataclass(caught.value.data)
    dumped = caught.value.data.model_dump_json()
    assert type(payload).model_validate_json(dumped) == unexpected


@pytest.mark.parametrize(
    ("model", "field", "handle_name"),
    [
        (QuerySandboxesPage, "sandboxes", "Sandbox"),
        (QuerySessionsPage, "sessions", "SandboxRuntimeSession"),
        (QuerySnapshotsPage, "snapshots", "Snapshot"),
        (QueryDrivesPage, "drives", "Drive"),
    ],
)
def test_generic_pages_preserve_live_handles(model, field, handle_name) -> None:
    # Exercise the same concrete generic types as the lazy public iterators.
    handle_type = getattr(sandbox, handle_name)
    handle = object.__new__(handle_type)
    page = model[handle_type](**{field: [handle], "next_cursor": "next"})
    assert getattr(page, field)[0] is handle
    with pytest.raises(ValidationError, match="frozen"):
        page.next_cursor = None


@pytest.mark.parametrize(
    "model", [QuerySandboxesParams, QuerySessionsParams, QuerySnapshotsParams, QueryDrivesParams]
)
def test_pagination_validates_model_input_and_preserves_size(model) -> None:
    first = model.model_validate({"page_size": 50})
    following = first.with_cursor("next")
    assert following.page_size == 50
    assert following.cursor == "next"
    assert first.cursor is None
    for size in [0, 51]:
        with pytest.raises(ValidationError, match="page_size must be between 1 and 50"):
            model.model_validate({"page_size": size})


def test_upload_models_preserve_stream_and_binary_body_identity() -> None:
    reader = io.BytesIO(b"\xff\x00payload")
    source = SyncByteStreamRuntime.reader(reader)
    entry = _UploadFileEntry[ReadableByteStream](path="file", size=9, source=source)
    assert entry.source is source
    copied = entry.model_copy(update={"archive_path": "archive/file"})
    assert copied.source is source
    assert reader.tell() == 0
    credentials = sandbox.SandboxCredentials("token", "team", "project")
    body = b"\xff\x00encoded request"
    request = _PreparedSandboxCreation(credentials=credentials, project_id="project", body=body)
    assert request.credentials is credentials
    assert request.body is body


@pytest.mark.parametrize("model", [_CreateSandboxParams, _ForkSandboxParams, _ResumeSandboxParams])
def test_deferred_parameters_preserve_defaults_and_mount_handles(model) -> None:
    required = {"source_sandbox": "preview"} if model is _ForkSandboxParams else {}
    if model is _ResumeSandboxParams:
        required = {"name": "preview"}
    params = model(**required)
    assert params.private_parameters is NO_PRIVATE_PARAMETERS
    with pytest.raises(ValidationError, match="frozen"):
        params.name = "changed"
    if model is _ResumeSandboxParams:
        return
    drive = SimpleNamespace(name="data", project_id="project", region="sfo1")
    path = PurePosixPath("/data")
    params = model(**required, mounts={path: sandbox.DriveMount(drive)})
    assert params.mounts[path].drive is drive


async def test_recovery_models_preserve_mutability_and_shared_coordination() -> None:
    event = anyio.Event()
    attempt = _AsyncRecoveryAttempt(completed=event)
    error = RuntimeError("resume failed")
    recoveries: list[_AsyncRecoveryAttempt | _SyncRecoveryAttempt] = [
        attempt,
        _SyncRecoveryAttempt(),
    ]
    for recovery in recoveries:
        assert isinstance(recovery, BaseModel)
        recovery.status = "failed"
        recovery.error = error
        assert recovery.error is error
        assert recovery.status == "failed"
    assert attempt.completed is event
    attempt.completed.set()
    assert event.is_set()


def test_mutable_output_models_preserve_sinks_and_merged_capture() -> None:
    sink = io.StringIO()
    target = _OutputTarget(stream=sink)
    assert target.stream is sink
    target.write("first")
    target.chunks = []
    target.write("second")
    assert target.captured() == "second"
    assert sink.getvalue() == "first"
    router = ProcessOutputRouter(
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, capture_output=False
    )
    assert isinstance(router._stdout, BaseModel)
    assert router._stderr is router._stdout
    router._stdout.write("out")
    router._stderr.write("err")
    assert router.captured() == ("outerr", None)


def test_sparse_attachment_flags_survive_nested_validation_and_copy() -> None:
    payload = state.SandboxState.model_validate(
        {
            "name": "preview",
            "current_session_id": "sbx_1",
            "mounts_attached": False,
            "routes_attached": False,
            "current_session_attached": False,
            "current_session": {"id": "sbx_1"},
        }
    )
    assert payload.current_session is not None
    stopped = state.RuntimeSessionStopState(
        session=payload.current_session,
        sandbox=payload,
        sandbox_attached=True,
    )
    restored = state.RuntimeSessionStopState.model_validate_json(stopped.model_dump_json())
    assert restored.sandbox_attached
    assert restored.sandbox is not None
    assert not restored.sandbox.mounts_attached
    assert not restored.sandbox.routes_attached
    assert not restored.sandbox.current_session_attached
    assert isinstance(restored.sandbox.current_session, state.SandboxRuntimeSessionState)
    updated = payload.model_copy(update={"routes_attached": True})
    assert updated.current_session is payload.current_session
    assert updated.routes_attached
    assert not payload.routes_attached
