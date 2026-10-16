"""Drives: getting, querying, deleting, and mounting them into sandboxes."""

from pathlib import PurePosixPath
from typing import Any, cast

import httpx2 as httpx
import pytest
from fake_sandbox import FakeSandboxApi, wire
from sandbox_modes import Mode

from vercel import sandbox
from vercel.sandbox import (
    DriveMount,
    DriveQuery,
    DriveQueryByCreatedAt,
    DriveQueryByUpdatedAt,
    SandboxApiError,
    SandboxMount,
)
from vercel.sandbox.client import SandboxClient
from vercel.sandbox.sync.client import SyncSandboxClient


async def test_get_or_create_returns_the_same_drive_every_time(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.project_id = "prj_other"

    async with mode.session():
        drive, created = await mode(
            mode.sandbox.get_or_create_drive(name="cache", region="sfo1", max_size_bytes=1024**3)
        )
        again, created_again = await mode(mode.sandbox.get_or_create_drive(name="cache"))

    assert (created, created_again) == (True, False)
    assert isinstance(drive, mode.handle_types("Drive"))
    assert (drive.id, again.id) == ("drive_1", "drive_1")
    assert drive.name == "cache"
    assert drive.project_id == "prj_other"
    assert drive.region == "sfo1"
    assert drive.max_size_bytes == 1024**3
    assert drive.current_session_id is None
    assert drive.snapshot() == DriveMount(drive, mode="snapshot")
    with pytest.raises(AttributeError):
        drive.name = "other"  # type: ignore[misc]
    first, second = fake_api.requests_to("drives.get_or_create")
    assert first.json() == {"projectId": "prj_other", "maxSizeBytes": 1024**3, "region": "sfo1"}
    assert second.json() == {"projectId": "prj_other"}


@pytest.mark.parametrize(
    ("arguments", "code"),
    [({"max_size_bytes": 2 * 1024**3}, "conflict"), ({"region": "cle1"}, "conflict")],
)
async def test_get_or_create_rejects_a_conflicting_existing_drive(
    fake_api: FakeSandboxApi, arguments: dict[str, Any], code: str
) -> None:
    async with fake_api.session():
        await sandbox.get_or_create_drive(name="cache", region="sfo1", max_size_bytes=1024**3)
        with pytest.raises(SandboxApiError) as exc_info:
            await sandbox.get_or_create_drive(name="cache", **arguments)

    assert exc_info.value.status_code == 409
    assert exc_info.value.code == code


def test_drive_mount_modes_are_validated() -> None:
    with pytest.raises(ValueError, match="snapshot.*read-write"):
        DriveMount("cache", mode=cast(Any, "read-only"))


async def test_queries_filter_order_and_follow_cursors(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    async with mode.session():
        for name in ("cache-2", "other", "cache-1"):
            await mode(mode.sandbox.get_or_create_drive(name=name))
            fake_api.clock.advance(1)
        query = mode.sandbox.DriveQueryByName(name_prefix="cache-", sort_order="asc")
        drives = await mode.collect(mode.sandbox.query_drives(query=query, page_size=1))

    assert [drive.name for drive in drives] == ["cache-1", "cache-2"]
    assert all(isinstance(drive, mode.handle_types("Drive")) for drive in drives)
    first, second = (dict(request.params) for request in fake_api.requests_to("drives.list"))
    assert first == {
        "teamId": "team_123",
        "projectId": "prj_123",
        "limit": "1",
        "sortBy": "name",
        "sortOrder": "asc",
        "namePrefix": "cache-",
    }
    assert second == first | {"cursor": "1"}


@pytest.mark.parametrize(
    ("query", "sort_by", "names"),
    [
        (DriveQueryByCreatedAt(sort_order="asc"), "createdAt", ["first", "second"]),
        (DriveQueryByUpdatedAt(sort_order="asc"), "updatedAt", ["first", "second"]),
    ],
)
async def test_queries_encode_each_supported_ordering(
    fake_api: FakeSandboxApi, query: DriveQuery, sort_by: str, names: list[str]
) -> None:
    async with fake_api.session():
        for name in ("first", "second"):
            await sandbox.get_or_create_drive(name=name)
            fake_api.clock.advance(1)
        drives = [drive async for drive in sandbox.query_drives(query=query)]

    assert [drive.name for drive in drives] == names
    assert dict(fake_api.requests_to("drives.list")[0].params) == {
        "teamId": "team_123",
        "projectId": "prj_123",
        "sortBy": sort_by,
        "sortOrder": "asc",
    }


async def test_mounted_drives_attach_to_the_session_and_cannot_be_deleted(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    async with mode.session():
        drive, _ = await mode(mode.sandbox.get_or_create_drive(name="cache", region="sfo1"))
        for name in ("source", "scratch"):
            await mode(mode.sandbox.get_or_create_drive(name=name, region="sfo1"))
        async with mode.enter(
            mode.sandbox.create_sandbox(
                name="preview",
                region="sfo1",
                mounts={
                    "/cache//./": drive,
                    "/readonly": DriveMount("source", mode="snapshot"),
                    "/scratch": "scratch",
                },
            )
        ) as box:
            assert box.mounts == {
                "/cache": SandboxMount(drive="cache", mode="read-write"),
                "/readonly": SandboxMount(drive="source", mode="snapshot"),
                "/scratch": SandboxMount(drive="scratch", mode="read-write"),
            }
            attached = await mode(mode.sandbox.get_drive(name_or_id="cache"))
            assert attached.current_session_id == box.current_session_id
            assert attached.current_sandbox_name == "preview"
            with pytest.raises(SandboxApiError) as exc_info:
                await mode(attached.delete())
            assert exc_info.value.status_code == 409
            assert exc_info.value.code == "drive_attached"

        deleted = await mode(attached.delete())

    assert deleted is attached
    assert attached.current_session_id is None
    assert attached.current_sandbox_name is None
    assert fake_api.requests_to("sandboxes.create")[0].json()["mounts"] == {
        "/cache": {"drive": "cache", "mode": "read-write"},
        "/readonly": {"drive": "source", "mode": "snapshot"},
        "/scratch": {"drive": "scratch", "mode": "read-write"},
    }
    assert dict(fake_api.requests_to("drives.delete")[-1].params) == {
        "teamId": "team_123",
        "projectId": "prj_123",
    }


async def test_mounts_are_checked_against_the_drive_before_any_request(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.project_id = "project-name"
    fake_api.project_names = {"project-name": "prj_123"}
    fake_api.region = "iad1"
    fake_api.seed("source", region="sfo1")

    async with fake_api.session():
        drive, _ = await sandbox.get_or_create_drive(name="cache", region="sfo1")
    assert drive.project_id == "prj_123"

    fake_api.project_id = "prj_other"
    async with fake_api.session():
        with pytest.raises(ValueError, match="project"):
            await sandbox.create_sandbox(region="sfo1", mounts={"/cache": drive})

    # The credentials name the project, so the drive's ID cannot be compared
    # and the API is left to decide.
    fake_api.project_id = "project-name"
    async with fake_api.session():
        requests_before = len(fake_api.requests)
        with pytest.raises(ValueError, match="region"):
            await sandbox.create_sandbox(region="cle1", mounts={"/cache": drive})
        with pytest.raises(ValueError, match="region"):
            await sandbox.get_or_create_sandbox(name="with-drive", mounts={"/cache": drive})
        with pytest.raises(ValueError, match="region"):
            await sandbox.fork_sandbox(source_sandbox="source", mounts={"/cache": drive})
        assert [request.route for request in fake_api.requests[requests_before:]] == [
            "sandboxes.get"
        ]

        created = await sandbox.create_sandbox(region="sfo1", mounts={"/cache": drive})

    assert created.mounts == {"/cache": SandboxMount(drive="cache", mode="read-write")}
    assert not fake_api.requests_to("sandboxes.fork")


async def test_mounts_serialize_alongside_failover_regions(fake_api: FakeSandboxApi) -> None:
    async with fake_api.session():
        drive, _ = await sandbox.get_or_create_drive(name="cache", region="sfo1")
        await sandbox.create_sandbox(
            region="sfo1", failover_regions=["cle1"], mounts={"/cache": drive}
        )

    body = fake_api.requests_to("sandboxes.create")[0].json()
    assert body["region"] == "sfo1"
    assert body["failoverRegions"] == ["cle1"]
    assert body["mounts"] == {"/cache": {"drive": "cache", "mode": "read-write"}}


async def test_mount_paths_may_be_pure_posix_paths(fake_api: FakeSandboxApi) -> None:
    async with fake_api.session():
        await sandbox.get_or_create_drive(name="cache")
        await sandbox.create_sandbox(mounts={PurePosixPath("/mnt") / "cache": "cache"})

    assert fake_api.requests_to("sandboxes.create")[0].json()["mounts"] == {
        "/mnt/cache": {"drive": "cache", "mode": "read-write"}
    }


@pytest.mark.parametrize(
    ("mounts", "message"),
    [
        ({"relative": "cache"}, "absolute"),
        ({"/data/../cache": "cache"}, r"\.\."),
        ({"/cache\x00suffix": "cache"}, "NUL"),
        ({"/": "cache"}, "root"),
        ({"/run/cell": "cache"}, "reserved"),
        ({"/run/vercel/share": "cache"}, "reserved"),
        ({"/" + "x" * 256: "cache"}, "256"),
        ({f"/drive-{index}": f"drive-{index}" for index in range(5)}, "at most 4"),
        ({"/data": "cache", "/data/nested": "other"}, "overlap"),
        ({"/data": "cache", "/data/": "other"}, "overlap"),
        ({"/one": "cache", "/two": "cache"}, "only once"),
    ],
)
async def test_invalid_mounts_are_rejected_before_any_request(
    fake_api: FakeSandboxApi, mounts: dict[str, Any], message: str
) -> None:
    async with fake_api.session():
        with pytest.raises(ValueError, match=message):
            await sandbox.create_sandbox(mounts=mounts)

    assert not fake_api.requests


async def test_get_or_create_validates_mounts_only_when_it_creates(
    fake_api: FakeSandboxApi,
) -> None:
    fake_api.seed("existing")
    fake_api.seed("stale")
    # See test_get_or_create_recreates_a_sandbox_whose_snapshot_is_gone for
    # why this is a 410 rather than the API's 400.
    fake_api.fail(
        "sandboxes.get",
        status=410,
        code="snapshot_not_found",
        when=lambda request: request.path_params["name"] == "stale",
    )

    async with fake_api.session():
        box, created = await sandbox.get_or_create_sandbox(
            name="existing", mounts={"/data/../cache": "cache"}
        )
        assert (box.name, created) == ("existing", False)
        with pytest.raises(ValueError, match="NUL"):
            await sandbox.get_or_create_sandbox(name="stale", mounts={"/cache\x00suffix": "cache"})

    assert fake_api.routes() == ["sandboxes.get", "sandboxes.get"]


async def test_forks_never_inherit_mounts(fake_api: FakeSandboxApi) -> None:
    fake_api.project_id = "prj_other"
    fake_api.region = "iad1"

    async with fake_api.session():
        drive, _ = await sandbox.get_or_create_drive(name="cache", region="sfo1")
        await sandbox.create_sandbox(
            name="source", region="sfo1", mounts={"/cache": drive.snapshot()}
        )
        forks = [
            await sandbox.fork_sandbox(source_sandbox="source", name="omitted"),
            await sandbox.fork_sandbox(source_sandbox="source", name="empty", mounts={}),
            await sandbox.fork_sandbox(
                source_sandbox="source", name="string", region="sfo1", mounts={"/cache": "cache"}
            ),
            await sandbox.fork_sandbox(
                source_sandbox="source", name="handle", region="sfo1", mounts={"/cache": drive}
            ),
        ]

    read_write = {"/cache": SandboxMount(drive="cache", mode="read-write")}
    assert [fork.mounts for fork in forks] == [None, None, read_write, read_write]
    requests = fake_api.requests_to("sandboxes.fork")
    assert [request.json()["mounts"] for request in requests] == [
        {},
        {},
        {"/cache": {"drive": "cache", "mode": "read-write"}},
        {"/cache": {"drive": "cache", "mode": "read-write"}},
    ]
    assert all(request.params["projectId"] == "prj_other" for request in requests)


async def test_update_omits_clears_or_replaces_mounts(fake_api: FakeSandboxApi) -> None:
    async with fake_api.session():
        for name in ("existing", "cache"):
            await sandbox.get_or_create_drive(name=name)
        box = await sandbox.create_sandbox(name="mounted", mounts={"/existing": "existing"})
        await box.update(mounts=None)
        assert box.mounts == {"/existing": SandboxMount(drive="existing", mode="read-write")}
        await box.update(mounts={})
        await box.update(mounts={"/cache//.": DriveMount("cache", mode="snapshot")})

    bodies = [request.json() for request in fake_api.requests_to("sandboxes.update")]
    assert "mounts" not in bodies[0]
    assert bodies[1]["mounts"] == {}
    assert bodies[2]["mounts"] == {"/cache": {"drive": "cache", "mode": "snapshot"}}
    assert box.mounts == {"/cache": SandboxMount(drive="cache", mode="snapshot")}


@pytest.mark.xfail(
    strict=True,
    reason="The API omits cleared mounts from the update response, and the SDK "
    "keeps the mounts it last saw when a sparse response omits them.",
)
async def test_clearing_mounts_is_reflected_on_the_handle(fake_api: FakeSandboxApi) -> None:
    async with fake_api.session():
        await sandbox.get_or_create_drive(name="cache")
        box = await sandbox.create_sandbox(name="mounted", mounts={"/cache": "cache"})
        await box.update(mounts={})

    assert fake_api.sandbox("mounted").mounts is None
    assert box.mounts is None


@pytest.mark.parametrize("mount_mode", ["snapshot", "read-write", None])
async def test_mount_readback_defaults_to_read_write_and_is_a_copy(
    fake_api: FakeSandboxApi, mount_mode: str | None
) -> None:
    mount = {"drive": "cache"} if mount_mode is None else {"drive": "cache", "mode": mount_mode}
    fake_api.respond(
        "sandboxes.get",
        httpx.Response(
            200,
            json=wire.sandbox_response(sandbox=wire.sandbox_payload(mounts={"/cache": mount})),
        ),
    )

    async with fake_api.session():
        box, created = await sandbox.get_or_create_sandbox(name="preview")

    assert created is False
    expected = "read-write" if mount_mode is None else mount_mode
    assert box.mounts == {"/cache": SandboxMount(drive="cache", mode=cast(Any, expected))}
    mounts = box.mounts
    assert mounts is not None
    mounts["/cache"] = SandboxMount(drive="other", mode="read-write")
    assert box.mounts["/cache"].drive == "cache"


@pytest.mark.parametrize("name_or_id", ["cache", "drive_1", "cache/with space"])
@pytest.mark.parametrize("standalone", [False, True])
async def test_get_drive_by_name_or_id(
    mode: Mode, fake_api: FakeSandboxApi, name_or_id: str, standalone: bool
) -> None:
    fake_api.project_id = "prj_other"
    async with fake_api.session():
        await sandbox.get_or_create_drive(name="cache/with space" if " " in name_or_id else "cache")
    fake_api.requests.clear()

    options: Any = fake_api.service_options(sync=mode.sync)[0]
    factory = fake_api.httpx_client_factory(sync=mode.sync)
    client: Any = (
        SyncSandboxClient.create(options=options, httpx_client_factory=factory)
        if mode.sync
        else SandboxClient.create(options=options, httpx_client_factory=factory)
    )
    try:
        async with mode.session():
            get_drive = client.get_drive if standalone else mode.sandbox.get_drive
            drive = await mode(get_drive(name_or_id=name_or_id))
            assert isinstance(drive, mode.handle_types("Drive"))
            assert drive.id == "drive_1"
            assert drive.snapshot() == DriveMount(drive, mode="snapshot")
            with pytest.raises(SandboxApiError) as exc_info:
                await mode(get_drive(name_or_id="missing"))
    finally:
        await mode(client.close() if mode.sync else client.aclose())

    assert exc_info.value.status_code == 404
    assert exc_info.value.code == "not_found"
    found, missing = fake_api.requests
    assert dict(found.params) == {"teamId": "team_123", "projectId": "prj_other"}
    assert found.path_params["name"] == name_or_id
    assert missing.path_params["name"] == "missing"


async def test_drive_delete_names_the_credentials_project(
    mode: Mode, fake_api: FakeSandboxApi
) -> None:
    fake_api.project_id = "project-name"
    fake_api.project_names = {"project-name": "prj_123"}

    async with mode.session():
        await mode(mode.sandbox.get_or_create_drive(name="cache"))
        drive = await mode(mode.sandbox.get_drive(name_or_id="cache"))
        assert drive.project_id == "prj_123"
        await mode(drive.delete())

    assert dict(fake_api.requests_to("drives.delete")[0].params) == {
        "teamId": "team_123",
        "projectId": "project-name",
    }
    assert not fake_api.drives
