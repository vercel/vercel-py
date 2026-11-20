"""The flow route handler, driven the way a web framework drives it.

`workflow_entrypoint()` returns one handler for one route, and everything it
needs about the request arrives through `HTTPRequest` -- which promises a plain
mapping of headers, not an httpx one. HTTP headers are case-insensitive, plain
mappings are not, and what case they arrive in is decided by the framework in
front of us: ASGI lowercases, `BaseHTTPRequestHandler` and WSGI do not. So the
handler has to be indifferent to it, and that is what this pins.

A queue delivery, not the health probe, because the delivery is the path that
reads headers at all -- and a probe payload is the one delivery that needs no
run to exist first, so a real `LocalWorld` can answer it.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from collections.abc import AsyncIterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import pytest

import vercel.workflow
from vercel.workflow._internal import core, runtime, world as w
from vercel.workflow._internal.worlds import local as local_mod

CID = "01K1CASEINSENSITIVE00000"
PROBE = json.dumps({"__healthCheck": True, "correlationId": CID}).encode()
HEALTH_QUEUE = "__wkf_workflow_health_check"


@pytest.mark.parametrize("supplied", [None, "wrong-key", "非ASCII"])
async def test_unsigned_or_forged_local_deliveries_are_refused(tmp_path, monkeypatch, supplied):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("WORKFLOW_QUEUE_AUTH", raising=False)
    world = local_mod.LocalWorld()
    callback = AsyncMock()
    handler = world.create_queue_handler("__wkf_workflow_", callback)

    class Untrusted(Delivery):
        async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
            raise AssertionError("unauthenticated bodies must not be read")
            yield b""  # pragma: no cover

    headers = {"x-vqs-queue-name": HEALTH_QUEUE}
    if supplied is not None:
        # Raw mappings can contain non-ASCII header values too.
        headers["x-workflow-auth"] = supplied
    response = await handler(Untrusted(headers))
    assert response.status == 401
    assert response.headers["x-workflow-auth-refused"] == "1"
    assert json.loads(response.body)["code"] == "QUEUE_AUTH_REFUSED"
    callback.assert_not_awaited()
    assert not (tmp_path / "runs").exists()


async def test_queue_auth_key_is_shared_only_by_the_same_data_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path / "app"))
    first = local_mod.LocalWorld()._queue_auth_key()
    assert len(first) == 43
    assert local_mod.LocalWorld()._queue_auth_key() == first
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path / "other-app"))
    assert local_mod.LocalWorld()._queue_auth_key() != first


def test_concurrent_worlds_publish_one_private_complete_queue_key(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    worlds = [local_mod.LocalWorld() for _ in range(8)]
    with ThreadPoolExecutor(max_workers=len(worlds)) as executor:
        keys = list(executor.map(lambda world: world._queue_auth_key(), worlds))
    assert len(set(keys)) == 1
    key_file = tmp_path / "queue-auth-key"
    assert key_file.read_text().strip() == keys[0]
    # Windows uses ACLs; its stat mode does not report POSIX permissions.
    if os.name == "posix":
        assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    assert not list(tmp_path.glob(".queue-auth-key-*"))


@pytest.mark.parametrize("existing_key", [False, True])
def test_queue_auth_key_is_ignored_by_git(tmp_path, monkeypatch, existing_key):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    data_dir = tmp_path / ".workflow-data"
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(data_dir))
    monkeypatch.delenv("WORKFLOW_QUEUE_AUTH", raising=False)
    if existing_key:
        data_dir.mkdir()
        (data_dir / "queue-auth-key").write_text("a" * 43 + "\n")
    w.set_world(local_mod.LocalWorld())

    vercel.workflow.Workflows()

    assert (data_dir / "queue-auth-key").is_file()
    # Verify Git's behavior, including the public API's eager key creation.
    subprocess.run(
        ["git", "check-ignore", "-q", ".workflow-data/queue-auth-key"],
        cwd=tmp_path,
        check=True,
    )
    if existing_key:
        assert (data_dir / "queue-auth-key").read_text().strip() == "a" * 43


def test_queue_auth_key_preserves_existing_gitignore(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    ignore_file = tmp_path / ".gitignore"
    contents = b"# Custom rules\r\nqueue-auth-key\r\n"
    ignore_file.write_bytes(contents)
    world = local_mod.LocalWorld()

    first = world._queue_auth_key()
    assert world._queue_auth_key() == first
    assert ignore_file.read_bytes() == contents


@pytest.mark.parametrize("invalid", ["", "truncated", "!" * 43])
def test_invalid_queue_auth_key_does_not_disable_authentication(tmp_path, monkeypatch, invalid):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("WORKFLOW_QUEUE_AUTH", raising=False)
    key_file = tmp_path / "queue-auth-key"
    key_file.write_text(invalid)
    world = local_mod.LocalWorld()
    with pytest.raises(ValueError, match="Invalid local queue authentication key"):
        world.create_queue_handler("__wkf_workflow_", AsyncMock())
    assert key_file.read_text() == invalid


async def test_unsigned_local_delivery_requires_explicit_trusted_transport(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("WORKFLOW_QUEUE_AUTH", "off")
    world = local_mod.LocalWorld()
    callback = AsyncMock(return_value=None)
    handler = world.create_queue_handler("__wkf_workflow_", callback)
    response = await handler(Delivery({"x-vqs-queue-name": HEALTH_QUEUE}))
    assert response.status == 200
    callback.assert_awaited_once()


class Delivery(w.HTTPRequest):
    """A queue delivery whose headers are spelled however *headers* spells them."""

    def __init__(self, headers: Mapping[str, str], body: bytes = PROBE) -> None:
        self._headers = headers
        self._body = body

    @property
    def method(self) -> str:
        return "POST"

    @property
    def url(self) -> str:
        return runtime.ENDPOINT_PATH

    @property
    def headers(self) -> Mapping[str, str]:
        return self._headers

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        yield self._body


@pytest.fixture(autouse=True)
def _reset_world(isolated_subscriptions):
    """`isolated_subscriptions` because these tests subscribe for real.

    Registrations are process-global and refuse to overlap, so a subscription
    another test left behind makes `workflow_entrypoint` here raise
    `DuplicateSubscriptionError` -- intermittently, depending on what shared the
    worker. Clearing on the way out is not enough; the fixture also clears on the
    way in, and puts back what it found.
    """
    yield
    w.set_world(None)


@pytest.mark.parametrize(
    "spelling",
    [
        {"x-vqs-queue-name": HEALTH_QUEUE, "content-type": "application/json"},
        {"X-VQS-Queue-Name": HEALTH_QUEUE, "Content-Type": "application/json"},
    ],
    ids=["lowercase", "mixed-case"],
)
async def test_a_delivery_is_dispatched_whatever_case_its_headers_use(
    spelling, tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)
    handler = runtime.workflow_entrypoint(core.Workflows(as_vercel_job=False))

    try:
        response = await handler(
            Delivery(dict(spelling) | {"X-Workflow-Auth": world._queue_auth_key()})
        )
    finally:
        await world.aclose()

    assert response.status == 200
    # Accepted *and* dispatched: the answer on disk is what the subscribed
    # handler wrote, so a 200 alone could not have produced it.
    assert json.loads(response.body) == {"ok": True}
    assert await world.streams_list(f"wrun_hc_{CID}") == [f"__health_check__{CID}"]


async def test_the_public_surface_serves_a_delivery(tmp_path, monkeypatch) -> None:
    """What a user has to reach for, reached for the way a user would.

    Nothing here imports `vercel._internal`: a registry, the path to mount it
    at, and an adapter built structurally -- `PlainRequest` inherits nothing,
    which is what `HTTPRequest` being a Protocol is for.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)

    app = vercel.workflow.Workflows()

    class PlainRequest:
        method = "POST"
        url = vercel.workflow.ENDPOINT_PATH
        headers = {
            "x-vqs-queue-name": HEALTH_QUEUE,
            "x-workflow-auth": world._queue_auth_key(),
        }

        async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
            yield PROBE

    try:
        response = await app.http_handler(PlainRequest())
    finally:
        await world.aclose()

    assert isinstance(response, vercel.workflow.HTTPResponse)
    assert response.status == 200
    assert await world.streams_list(f"wrun_hc_{CID}") == [f"__health_check__{CID}"]


def test_the_endpoint_path_is_the_one_the_tools_probe() -> None:
    """Hard-coded on both sides, so it is pinned rather than derived."""
    assert vercel.workflow.ENDPOINT_PATH == "/.well-known/workflow/v1/flow"


def test_a_registry_that_serves_nothing_says_so(tmp_path, monkeypatch) -> None:
    """`as_vercel_job=False` skips the subscription, and a handler without one
    would accept deliveries and dispatch them nowhere."""
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    w.set_world(local_mod.LocalWorld())

    app = vercel.workflow.Workflows(as_vercel_job=False)

    with pytest.raises(RuntimeError, match="no HTTP handler"):
        _ = app.http_handler
