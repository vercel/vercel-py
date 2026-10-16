"""Creating, finding, forking, updating, and cleaning up sandboxes."""

import asyncio
import gc
from datetime import timedelta
from typing import Any, cast

import httpx as legacy_httpx
import httpx2 as httpx
import pytest
from fake_sandbox import FakeSandboxApi, RecordedRequest, wire
from sandbox_modes import Mode

from vercel import sandbox
from vercel._internal.core.session import get_active_session
from vercel.errors import VercelSessionClosedError
from vercel.sandbox import (
    GitSource,
    NetworkPolicy,
    SandboxApiError,
    SandboxCleanupError,
    SandboxResources,
    SandboxResponseError,
    SandboxSource,
    SandboxStatus,
    SandboxTerminalStateError,
    SnapshotExpiration,
    SnapshotRetention,
    SnapshotRetentionState,
    SnapshotSource,
    TarballSource,
    sync as sandbox_sync,
)
from vercel.sandbox._internal.service import get_sandbox_service
from vercel.sandbox._internal.state import SandboxRuntimeSessionState, SandboxState


def _resumes(request: RecordedRequest) -> bool:
    return request.params.get("resume") == "true"


async def test_private_parameters_reach_their_request_but_not_polls(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.startup_seconds = 1
    fake_api.seed("fetched")
    fake_api.seed("resumed")

    async with mode.session():
        await mode(mode.sandbox.create_sandbox(name="created", __networkId="network_123"))
        await mode(
            mode.sandbox.fork_sandbox(
                source_sandbox="created", name="forked", __networkId="network_123"
            )
        )
        await mode(mode.sandbox.get_sandbox(name="fetched", __includeSystemRoutes=True))
        await mode(mode.sandbox.resume_sandbox(name="resumed", __includeSystemRoutes=True))

    assert fake_api.requests_to("sandboxes.create")[-1].json()["__networkId"] == "network_123"
    assert fake_api.requests_to("sandboxes.fork")[-1].json()["__networkId"] == "network_123"
    polls = fake_api.requests_to(
        "sandboxes.get", when=lambda request: request.path_params["name"] in {"created", "forked"}
    )
    assert polls
    assert all("__networkId" not in poll.params for poll in polls)
    fetched, resumed = fake_api.requests_to(
        "sandboxes.get", when=lambda request: request.path_params["name"] in {"fetched", "resumed"}
    )
    assert fetched.params["__includeSystemRoutes"] == "true"
    assert resumed.params["__includeSystemRoutes"] == "true"


@pytest.mark.parametrize("sync", [False, True])
async def test_sandbox_supports_legacy_httpx_session_client(
    mock_env_clear: None, sync: bool
) -> None:
    fake = FakeSandboxApi()
    requests: list[legacy_httpx.Request] = []
    body = wire.sandbox_response(sandbox=wire.sandbox_payload(name="legacy"))

    def handler(request: legacy_httpx.Request) -> legacy_httpx.Response:
        requests.append(request)
        return legacy_httpx.Response(200, json=body)

    client: Any
    if sync:
        client = legacy_httpx.Client(transport=legacy_httpx.MockTransport(handler))
        with _legacy_session(fake, client, sync=True):
            instance: Any = sandbox_sync.get_sandbox(name="legacy")
    else:

        async def async_handler(request: legacy_httpx.Request) -> legacy_httpx.Response:
            return handler(request)

        client = legacy_httpx.AsyncClient(transport=legacy_httpx.MockTransport(async_handler))
        async with _legacy_session(fake, client, sync=False):
            instance = await sandbox.get_sandbox(name="legacy")

    assert instance.name == "legacy"
    assert requests[0].url.path == "/v2/sandboxes/legacy"
    assert requests[0].url.params["teamId"] == "team_123"
    assert client.is_closed


def _legacy_session(fake: FakeSandboxApi, client: object, *, sync: bool) -> Any:
    from vercel.api import session

    return session(
        service_options=fake.service_options(sync=sync),
        httpx_client_factory=cast(Any, lambda: client),
    )


async def test_create_encodes_options_and_update_keeps_the_current_session(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.project_id = "prj_other"

    async with fake_api.session():
        handle = await sandbox.create_sandbox(
            name="preview",
            source=GitSource(url="https://github.com/vercel/vercel-py", revision="main"),
            ports=[3000],
            execution_time_limit=120,
            resources=SandboxResources(vcpus=2, memory=4096),
            snapshot_expiration=SnapshotExpiration(timedelta(days=1)),
            snapshot_retention=SnapshotRetention(
                count=3, expiration=timedelta(days=2), delete_evicted=False
            ),
            tags={"env": "test"},
            network_id="network_123",
            region="iad1",
            failover_regions=("sfo1", "cle1"),
        )
        assert handle.image == "vercel/sandbox/universal:latest"
        assert handle.region == "iad1"
        assert handle.network_id == "network_123"
        assert handle.failover_regions == ("sfo1", "cle1")
        assert handle.execution_time_limit == timedelta(minutes=2)
        assert handle.snapshot_retention == SnapshotRetentionState(
            count=3, expiration=timedelta(days=2), delete_evicted=False
        )
        assert not hasattr(handle, "runtime")
        assert handle.current_session is not None
        assert not hasattr(handle.current_session, "runtime")
        with pytest.raises(AttributeError):
            handle.status = SandboxStatus.STOPPED  # type: ignore[misc]
        assert handle.tags is not None
        handle.tags["env"] = "mutated"
        retained_session = handle.current_session

        await handle.update(
            network_id="network_456",
            tags={"env": "updated"},
            execution_time_limit=4.5,
            snapshot_expiration=0,
            snapshot_retention=SnapshotRetention(count=1, expiration=0),
            region="sfo1",
            failover_regions=(),
        )
        assert handle.tags == {"env": "updated"}
        assert handle.region == "sfo1"
        assert handle.failover_regions == ()
        assert handle.current_session is retained_session
        assert handle.current_session.region == "iad1"
        assert handle.routes[0].url == "https://preview-3000.vercel.run"
        assert handle.project_id == "prj_other"

        await handle.update(tags={}, ports=[], network_id=None)
        # The API leaves an empty tag set out of its response.
        assert handle.tags is None
        assert handle.routes == ()
        assert handle.network_id is None
        assert handle.current_session is retained_session

        await handle.update(snapshot_retention=None)
        assert handle.snapshot_retention is None

    (create,) = fake_api.requests_to("sandboxes.create")
    assert dict(create.params) == {"teamId": "team_123"}
    assert create.headers["authorization"] == "Bearer token"
    assert create.headers["user-agent"].startswith("vercel-sandbox/")
    assert "Python/" in create.headers["user-agent"]
    assert create.json() == {
        "projectId": "prj_other",
        "name": "preview",
        "source": {"type": "git", "url": "https://github.com/vercel/vercel-py", "revision": "main"},
        "ports": [3000],
        "timeout": 120000,
        "resources": {"vcpus": 2, "memory": 4096},
        "snapshotExpiration": 86400000,
        "keepLastSnapshots": {"count": 3, "expiration": 172800000, "deleteEvicted": False},
        "tags": {"env": "test"},
        "networkId": "network_123",
        "region": "iad1",
        "failoverRegions": ["sfo1", "cle1"],
    }
    updates = fake_api.requests_to("sandboxes.update")
    assert [dict(request.params) for request in updates] == [
        {"teamId": "team_123", "projectId": "prj_other"}
    ] * 3
    assert [request.json() for request in updates] == [
        {
            "timeout": 4500,
            "snapshotExpiration": 0,
            "keepLastSnapshots": {"count": 1, "expiration": 0, "deleteEvicted": True},
            "tags": {"env": "updated"},
            "networkId": "network_456",
            "region": "sfo1",
            "failoverRegions": [],
        },
        {"ports": [], "tags": {}, "networkId": None},
        {"keepLastSnapshots": None},
    ]
    assert not hasattr(handle, "model_dump")


async def test_fork_encodes_overrides_polls_and_stops_on_exit(fake_api: FakeSandboxApi) -> None:
    fake_api.project_id = "prj_other"
    fake_api.seed("source")
    fake_api.startup_seconds = 1

    async with fake_api.session():
        async with sandbox.fork_sandbox(
            source_sandbox="source",
            name="forked",
            ports=[],
            execution_time_limit=12.5,
            resources=SandboxResources(vcpus=2, memory=4096),
            image="team/project/image:v1",
            persistent=False,
            network_policy=NetworkPolicy.deny_all(),
            network_id="network_123",
            env={},
            tags={},
            snapshot_expiration=0,
            region="iad1",
            failover_regions=("sfo1",),
            snapshot_retention=SnapshotRetention(
                count=2, expiration=timedelta(days=1), delete_evicted=False
            ),
            destroy=False,
        ) as forked:
            assert forked.name == "forked"

    (fork,) = fake_api.requests_to("sandboxes.fork")
    assert dict(fork.params) == {"teamId": "team_123", "projectId": "prj_other"}
    assert fork.json() == {
        "name": "forked",
        "ports": [],
        "region": "iad1",
        "failoverRegions": ["sfo1"],
        "timeout": 12500,
        "resources": {"vcpus": 2, "memory": 4096},
        "image": "team/project/image:v1",
        "persistent": False,
        "networkPolicy": {"mode": "deny-all"},
        "networkId": "network_123",
        "env": {},
        "tags": {},
        "mounts": {},
        "snapshotExpiration": 0,
        "keepLastSnapshots": {"count": 2, "expiration": 86400000, "deleteEvicted": False},
    }
    poll = fake_api.requests_to("sandboxes.get")[-1]
    assert dict(poll.params) == {"teamId": "team_123", "projectId": "prj_other", "resume": "false"}
    assert fake_api.sandbox("forked").current_session_id
    assert fake_api.current_session("forked").status == "stopped"
    assert not fake_api.requests_to("sandboxes.delete")


async def test_fork_without_overrides_inherits_from_the_source(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed("source", tags={"team": "a"}, ports=[3000])

    async with mode.session():
        forked = await mode(mode.sandbox.fork_sandbox(source_sandbox="source"))

    assert isinstance(forked, mode.handle_types("Sandbox"))
    assert forked.tags == {"team": "a"}
    assert [route.port for route in forked.routes] == [3000]
    (fork,) = fake_api.requests_to("sandboxes.fork")
    assert dict(fork.params) == {"teamId": "team_123", "projectId": "prj_123"}
    assert fork.json() == {"mounts": {}}


async def test_service_region_defaults_placements_and_allows_overrides(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.region = "iad1"
    fake_api.seed("source")

    async with fake_api.session():
        created = await sandbox.create_sandbox(name="created")
        await sandbox.fork_sandbox(source_sandbox="source", region="sfo1")
        await created.update(tags={"updated": "true"})

    assert fake_api.requests_to("sandboxes.create")[-1].json() == {
        "projectId": "prj_123",
        "name": "created",
        "region": "iad1",
    }
    assert fake_api.requests_to("sandboxes.fork")[-1].json() == {"mounts": {}, "region": "sfo1"}
    assert fake_api.requests_to("sandboxes.update")[-1].json() == {
        "tags": {"updated": "true"},
        "region": "iad1",
    }


async def test_service_region_defaults_to_the_environment(
    mode: Mode, fake_api: FakeSandboxApi, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VERCEL_REGION", "cle1")
    fake_api.seed("source")

    async with mode.session():
        await mode(mode.sandbox.fork_sandbox(source_sandbox="source"))

    assert fake_api.requests_to("sandboxes.fork")[-1].json() == {"mounts": {}, "region": "cle1"}


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            GitSource(url="https://github.com/vercel/vercel-py"),
            {"type": "git", "url": "https://github.com/vercel/vercel-py"},
        ),
        (
            TarballSource(url="https://example.com/source.tar.gz"),
            {"type": "tarball", "url": "https://example.com/source.tar.gz"},
        ),
        (
            SnapshotSource(snapshot_id="snap_1"),
            {"type": "snapshot", "snapshotId": "snap_1"},
        ),
    ],
)
async def test_create_serializes_source_variants(
    fake_api: FakeSandboxApi, source: SandboxSource, expected: dict[str, str]
) -> None:
    fake_api.seed("origin", files={"state.txt": "kept\n"})
    assert fake_api.seed_snapshot("origin").id == "snap_1"

    async with fake_api.session():
        box = await sandbox.create_sandbox(name="preview", source=source)
        if isinstance(source, SnapshotSource):
            assert await box.fs.read_text("state.txt") == "kept\n"

    assert fake_api.requests_to("sandboxes.create")[-1].json()["source"] == expected


@pytest.mark.parametrize(
    "image",
    [
        "my-repository",
        "my-repository:latest",
        "my-repository@sha256:request-digest",
        "vcr.vercel.com/team-slug/project-slug/my-repository:latest",
    ],
)
async def test_create_sends_the_image_and_reports_the_resolved_reference(
    mode: Mode, fake_api: FakeSandboxApi, image: str
) -> None:
    fake_api.resolve_image = lambda _reference: "team/project/my-repository@sha256:resolved"

    async with mode.session():
        handle = await mode(mode.sandbox.create_sandbox(name="preview", image=image))

    assert fake_api.requests_to("sandboxes.create")[-1].json() == {
        "projectId": "prj_123",
        "name": "preview",
        "image": image,
    }
    assert handle.image == "team/project/my-repository@sha256:resolved"
    assert not hasattr(handle, "runtime")
    assert handle.current_session is not None
    assert not hasattr(handle.current_session, "runtime")


async def test_create_rejects_a_malformed_success_response(fake_api: FakeSandboxApi) -> None:
    fake_api.respond("sandboxes.create", httpx.Response(200, json={}))

    async with fake_api.session():
        with pytest.raises(SandboxResponseError):
            await sandbox.create_sandbox(name="preview")


async def test_snapshot_expiration_is_validated_before_any_request(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed()

    async with fake_api.session():
        with pytest.raises(ValueError):
            sandbox.create_sandbox(snapshot_expiration=1)
        handle = await sandbox.get_sandbox(name="preview")
        with pytest.raises(ValueError):
            await handle.update(snapshot_expiration=1)
        with pytest.raises(ValueError):
            await handle.snapshot(expiration=1)

    assert fake_api.routes() == ["sandboxes.get"]


async def test_create_rejects_a_terminal_initial_state(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.respond(
        "sandboxes.create",
        httpx.Response(
            200,
            json=wire.sandbox_response(
                sandbox=wire.sandbox_payload(status="stopped"),
                session=wire.session_payload(status="stopped"),
            ),
        ),
    )

    async with mode.session():
        with pytest.raises(SandboxTerminalStateError) as exc_info:
            await mode(mode.sandbox.create_sandbox(name="preview"))

    assert exc_info.value.status is SandboxStatus.STOPPED
    assert isinstance(exc_info.value.sandbox, mode.handle_types("Sandbox"))
    assert exc_info.value.sandbox.name == "preview"


async def test_create_polls_a_pending_session_until_it_runs(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.startup_seconds = 1.2

    async with mode.session():
        box = await mode(mode.sandbox.create_sandbox(name="preview"))

    assert box.status is SandboxStatus.RUNNING
    assert fake_api.routes() == ["sandboxes.create"] + ["sandboxes.get"] * 3
    assert fake_api.clock.sleeps == [0.5, 0.5, 0.5]


async def test_service_returns_neutral_state_that_runtimes_bind_to_handles(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed(
        snapshotExpiration=0,
        keepLastSnapshots={"count": 2, "expiration": 86400000, "deleteEvicted": False},
    )

    async with fake_api.session():
        service = get_sandbox_service(get_active_session())
        state = await service.get_sandbox(name="preview")
        assert isinstance(state, SandboxState)
        assert isinstance(state.current_session, SandboxRuntimeSessionState)
        assert state.execution_time_limit == timedelta(minutes=5)
        assert state.snapshot_expiration == timedelta(0)
        assert state.snapshot_retention == SnapshotRetentionState(
            count=2, expiration=timedelta(days=1), delete_evicted=False
        )
        assert state.raw is not None
        assert state.raw["timeout"] == 300000
        assert state.raw["snapshotExpiration"] == 0
        assert state.raw["keepLastSnapshots"] == {
            "count": 2,
            "expiration": 86400000,
            "deleteEvicted": False,
        }
        page_state = await service.query_sandboxes_page()
        assert isinstance(page_state.sandboxes[0], SandboxState)


async def test_runtimes_bind_their_own_handles(mode: Mode, fake_api: FakeSandboxApi) -> None:
    fake_api.seed()
    sandbox_type, session_type, process_type, snapshot_type = mode.handle_types(
        "Sandbox", "SandboxRuntimeSession", "Process", "Snapshot"
    )
    assert not hasattr(sandbox_sync, "Process")

    async with mode.session():
        handle = await mode(mode.sandbox.get_sandbox(name="preview"))
        assert isinstance(handle, sandbox_type)
        assert isinstance(handle.current_session, session_type)
        assert isinstance(await mode(handle.create_process("true")), process_type)
        updated_session = await mode(handle.extend_execution_time_limit(2.5))
        assert isinstance(updated_session, session_type)
        assert updated_session.execution_time_limit == timedelta(minutes=5, seconds=2.5)
        await mode(handle.update(tags={}))
        assert isinstance(await mode(handle.snapshot(expiration=86400.5)), snapshot_type)
        page = await mode.collect(mode.sandbox.query_sandboxes())
        assert isinstance(page[0], sandbox_type)

    assert fake_api.requests_to("sessions.extend_timeout")[-1].json() == {"duration": 2500}
    assert fake_api.requests_to("sessions.snapshot")[-1].json() == {"expiration": 86400500}
    assert fake_api.requests_to("sandboxes.update")[-1].json() == {"tags": {}}


async def test_kill_after_is_sent_as_a_command_timeout(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed()

    async with mode.session():
        handle = await mode(mode.sandbox.get_sandbox(name="preview"))
        completed = await mode(handle.run_process("sleep", ["60"], kill_after=2.5))
        await mode(handle.create_process("sleep", ["60"], kill_after=timedelta(seconds=3.25)))
        await mode(handle.current_session.create_process("sleep", ["60"], kill_after=4))
        await mode(handle.create_process("echo", ["hello"]))

    assert completed.returncode == 137
    assert [request.json() for request in fake_api.requests_to("commands.create")] == [
        {
            "command": "sleep",
            "args": ["60"],
            "sudo": False,
            "wait": True,
            "logs": True,
            "timeout": 2500,
        },
        {"command": "sleep", "args": ["60"], "sudo": False, "timeout": 3250},
        {"command": "sleep", "args": ["60"], "sudo": False, "timeout": 4000},
        {"command": "echo", "args": ["hello"], "sudo": False},
    ]


async def test_closing_the_session_while_create_polls_rejects_the_create(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.startup_seconds = 3600

    async def create() -> object:
        return await sandbox.create_sandbox(name="preview")

    async with fake_api.session():
        operation = asyncio.create_task(create())
        await fake_api.wait_for_requests("sandboxes.get")

    with pytest.raises(VercelSessionClosedError):
        await operation


async def test_create_and_resume_operations_are_single_use(fake_api: FakeSandboxApi) -> None:
    async with fake_api.session():
        operation = sandbox.create_sandbox(name="preview")
        created = await operation
        assert created.current_session is not None
        with pytest.raises(RuntimeError, match="can only be used once"):
            await operation

        resume_operation = sandbox.resume_sandbox(name="preview")
        resumed = await resume_operation
        assert resumed.current_session is not None
        with pytest.raises(RuntimeError, match="can only be used once"):
            await resume_operation

    assert fake_api.requests_to("sandboxes.get")[-1].params["resume"] == "true"
    assert not fake_api.requests_to("sessions.stop")
    assert not fake_api.requests_to("sandboxes.delete")

    async with fake_api.session():
        captured = sandbox.create_sandbox(name="preview")
        captured_resume = sandbox.resume_sandbox(name="preview")

    with pytest.raises(VercelSessionClosedError):
        await captured
    with pytest.raises(VercelSessionClosedError):
        await captured_resume

    with pytest.warns(RuntimeWarning, match="never awaited or entered"):
        unconsumed = sandbox.create_sandbox(name="preview")
        del unconsumed
        gc.collect()
    with pytest.warns(RuntimeWarning, match="never awaited or entered"):
        unconsumed_resume = sandbox.resume_sandbox(name="preview")
        del unconsumed_resume
        gc.collect()


async def test_get_or_create_returns_an_existing_sandbox_or_creates_one(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.project_id = "prj_other"
    fake_api.seed("existing", image="existing-repository@sha256:resolved")

    async with mode.session():
        existing, existing_created = await mode(
            mode.sandbox.get_or_create_sandbox(name="existing", image="ignored-repository:latest")
        )
        created, was_created = await mode(
            mode.sandbox.get_or_create_sandbox(
                name="missing", image="requested:latest", persistent=True, tags={"purpose": "test"}
            )
        )

    assert (existing.name, existing_created) == ("existing", False)
    assert existing.image == "existing-repository@sha256:resolved"
    assert (created.name, was_created) == ("missing", True)
    assert created.image == "requested:latest"
    assert not hasattr(existing, "__enter__")
    assert not hasattr(created, "__enter__")
    assert [request.params["resume"] for request in fake_api.requests_to("sandboxes.get")] == [
        "true",
        "true",
    ]
    (create,) = fake_api.requests_to("sandboxes.create")
    assert create.json() == {
        "projectId": "prj_other",
        "name": "missing",
        "image": "requested:latest",
        "persistent": True,
        "tags": {"purpose": "test"},
    }


async def test_get_or_create_honors_resume_false(fake_api: FakeSandboxApi) -> None:
    fake_api.seed("existing")
    fake_api.stop("existing")

    async with fake_api.session():
        existing, created = await sandbox.get_or_create_sandbox(name="existing", resume=False)

    assert existing.name == "existing"
    assert existing.status is SandboxStatus.STOPPED
    assert created is False
    assert fake_api.requests_to("sandboxes.get")[-1].params["resume"] == "false"


async def test_get_or_create_recreates_a_sandbox_whose_snapshot_is_gone(
    fake_api: FakeSandboxApi,
) -> None:
    # The SDK recreates on 410 snapshot_not_found. The API service currently
    # answers 400 for a missing snapshot, so the fake is told to send 410.
    fake_api.fail("sandboxes.get", status=410, code="snapshot_not_found", when=_resumes)

    async with fake_api.session():
        recreated, created = await sandbox.get_or_create_sandbox(
            name="stale", image="stale-repository:latest", __networkId="network_123"
        )

    assert (recreated.name, created) == ("stale", True)
    assert recreated.image == "stale-repository:latest"
    assert fake_api.routes() == ["sandboxes.get", "sandboxes.delete", "sandboxes.create"]
    get, delete, create = fake_api.requests
    assert get.params["resume"] == "true"
    assert get.params["__networkId"] == "network_123"
    assert "__networkId" not in delete.params
    assert create.json() == {
        "projectId": "prj_123",
        "name": "stale",
        "image": "stale-repository:latest",
        "__networkId": "network_123",
    }


@pytest.mark.xfail(
    strict=True,
    raises=SandboxApiError,
    reason="The API answers a resume without a snapshot with 400, not the 410 "
    "the SDK recreates on.",
)
async def test_get_or_create_recreates_when_the_api_cannot_resume(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed("stale", persistent=False)
    fake_api.stop("stale")

    async with fake_api.session():
        recreated, created = await sandbox.get_or_create_sandbox(name="stale")

    assert (recreated.name, created) == ("stale", True)


async def test_get_or_create_propagates_other_lookup_errors(fake_api: FakeSandboxApi) -> None:
    fake_api.fail("sandboxes.get", status=403, code="forbidden", message="nope")

    async with fake_api.session():
        with pytest.raises(SandboxApiError) as exc_info:
            await sandbox.get_or_create_sandbox(name="forbidden")

    assert exc_info.value.status_code == 403
    assert fake_api.routes() == ["sandboxes.get"]


async def test_get_fetches_as_is_and_resume_starts_a_session(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.project_id = "prj_other"
    fake_api.seed()
    stopped = fake_api.stop("preview")

    async with mode.session():
        fetched = await mode(mode.sandbox.get_sandbox(name="preview", include_system_routes=True))
        resumed = await mode(
            mode.sandbox.resume_sandbox(name="preview", include_system_routes=True)
        )

    assert fetched.current_session is not None
    assert fetched.current_session.status is SandboxStatus.STOPPED
    assert resumed.current_session is not None
    assert resumed.current_session.id != stopped.id
    assert resumed.current_session.id == fake_api.sandbox("preview").current_session_id
    assert [dict(request.params) for request in fake_api.requests_to("sandboxes.get")] == [
        {
            "teamId": "team_123",
            "projectId": "prj_other",
            "resume": "false",
            "__includeSystemRoutes": "true",
        },
        {
            "teamId": "team_123",
            "projectId": "prj_other",
            "resume": "true",
            "__includeSystemRoutes": "true",
        },
    ]


def test_sync_get_is_a_plain_handle_and_create_and_resume_are_managed(
    fake_api: FakeSandboxApi,
) -> None:
    with fake_api.session(sync=True):
        created = sandbox_sync.create_sandbox(name="preview")
        fetched = sandbox_sync.get_sandbox(name="preview")
        resumed = sandbox_sync.resume_sandbox(name="preview")

    assert not hasattr(fetched, "__enter__")
    assert hasattr(created, "__enter__")
    assert hasattr(resumed, "__enter__")
    assert created.image == "vercel/sandbox/universal:latest"
    assert [request.params["resume"] for request in fake_api.requests_to("sandboxes.get")] == [
        "false",
        "true",
    ]
    assert not fake_api.requests_to("sessions.stop")
    assert not fake_api.requests_to("sandboxes.delete")


async def test_a_closed_session_rejects_handles_and_lazy_readers(
    fake_api: FakeSandboxApi,
) -> None:
    async with fake_api.session():
        service = get_sandbox_service(get_active_session())
        handle = await sandbox.create_sandbox(name="preview")
        resumed = await sandbox.resume_sandbox(name="preview")
        assert resumed.current_session is not None
        runtime_session = resumed.current_session
        command = await handle.create_process("sleep", ["30"])
        snapshot = await handle.snapshot()
        assert snapshot.region == "iad1"
        assert snapshot.regions == ("iad1",)

    with pytest.raises(VercelSessionClosedError):
        await handle.create_process("true")
    with pytest.raises(VercelSessionClosedError):
        await runtime_session.create_process("true")
    with pytest.raises(VercelSessionClosedError):
        await command.refresh()
    assert command.stdout is not None
    with pytest.raises(VercelSessionClosedError):
        # The reader opens its log response lazily, so the first read observes
        # the closed session.
        await command.stdout.read()
    with pytest.raises(VercelSessionClosedError):
        await snapshot.delete()
    with pytest.raises(VercelSessionClosedError):
        await service.get_sandbox(name="preview")


async def test_managed_sandboxes_stop_and_destroy_as_configured(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.seed("resumed")

    async with mode.session():
        async with mode.enter(mode.sandbox.create_sandbox(name="default")) as default:
            pass
        async with mode.enter(
            mode.sandbox.create_sandbox(name="retained", destroy=False)
        ) as retained:
            pass
        async with mode.enter(mode.sandbox.resume_sandbox(name="resumed")) as resumed:
            pass

    events = [
        f"stop:{fake_api.sessions[request.path_params['session_id']].sandbox_name}"
        if request.route == "sessions.stop"
        else f"destroy:{request.path_params['name']}"
        for request in fake_api.requests
        if request.route in {"sessions.stop", "sandboxes.delete"}
    ]
    assert events == ["stop:default", "destroy:default", "stop:retained", "stop:resumed"]
    assert fake_api.requests_to("sandboxes.delete")[0].params["deleteOrphanSnapshots"] == "true"
    # The API's destroy response carries the sandbox but no session.
    assert default.current_session is None
    assert default.status is SandboxStatus.STOPPED
    for handle in (retained, resumed):
        assert handle.current_session is not None
        assert handle.current_session.status is SandboxStatus.STOPPED
    assert ("prj_123", "default") not in fake_api.sandboxes
    assert fake_api.sandbox("retained").name == "retained"


async def test_managed_cleanup_wraps_a_destroy_failure(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.fail("sandboxes.delete", status=500, code="sandbox_failed", message="delete failed")

    async with mode.session():
        with pytest.raises(SandboxCleanupError) as exc_info:
            async with mode.enter(mode.sandbox.create_sandbox(name="preview")):
                pass

    assert exc_info.value.resource_type == "sandbox"
    assert exc_info.value.resource_id == "preview"
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "sandbox_failed"


async def test_managed_cleanup_still_destroys_after_a_stop_failure(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.fail("sessions.stop", status=500, code="stop_failed", message="stop failed")

    async with mode.session():
        with pytest.raises(SandboxCleanupError) as exc_info:
            async with mode.enter(mode.sandbox.create_sandbox(name="preview")):
                pass

    assert fake_api.requests_to("sandboxes.delete")
    assert isinstance(exc_info.value.cause, SandboxApiError)
    assert exc_info.value.cause.code == "stop_failed"


async def test_handles_reject_a_response_about_a_different_resource(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.respond(
        "sandboxes.update",
        httpx.Response(200, json={"sandbox": wire.sandbox_payload(name="other")}),
    )
    fake_api.respond(
        "commands.get", httpx.Response(200, json={"command": wire.command_payload(id="other")})
    )
    fake_api.respond(
        "snapshots.delete",
        httpx.Response(200, json={"snapshot": wire.snapshot_payload(id="other")}),
    )

    async with fake_api.session():
        handle = await sandbox.create_sandbox(name="preview")
        with pytest.raises(SandboxResponseError):
            await handle.update(ports=[3001])

        command = await handle.create_process("python", ["--version"])
        with pytest.raises(SandboxResponseError):
            await command.refresh()

        snapshot = await handle.snapshot()
        with pytest.raises(SandboxResponseError):
            await snapshot.delete()
