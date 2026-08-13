"""The manifest endpoint: what the dashboard reads to name what it is showing.

A run carries ids (``workflow//<module>//<qualname>``), not names, so the
Workflows tab resolves them through a manifest. In TypeScript that document is a
build artifact; here the registry already holds the same facts, so it is built
per request -- which makes the ids in it the thing to pin, because they are what
a reader joins on.

Serving it is opt-in: it lists every workflow module and function in the app, so
without ``WORKFLOW_PUBLIC_MANIFEST=1`` the route is an empty 404, the same gate
`@workflow/nest` applies.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping

import pytest

import vercel.workflow
from vercel.workflow._internal import core, runtime, world as w

# What `_manifest_file` should make of this module's name, spelled differently:
# the manifest keys files, and a reader matches them against paths it knows.
THIS_FILE = __name__.replace(".", "/") + ".py"

registry = core.Workflows(as_vercel_job=False)


@registry.step
async def charge(amount: int) -> int:
    return amount


@registry.workflow
async def checkout(amount: int) -> int:
    return await charge(amount)


class Request(w.HTTPRequest):
    @property
    def method(self) -> str:
        return "GET"

    @property
    def url(self) -> str:
        return vercel.workflow.MANIFEST_PATH

    @property
    def headers(self) -> Mapping[str, str]:
        return {}

    async def aiter_bytes(self, chunk_size: int | None = None) -> AsyncIterator[bytes]:
        return
        yield b""  # pragma: no cover -- makes this a generator


@pytest.fixture
def published(monkeypatch) -> None:
    monkeypatch.setenv(runtime.PUBLIC_MANIFEST_ENV, "1")


def test_a_module_is_keyed_by_the_path_it_would_have() -> None:
    """Spelled out for a fixed input, because the assertions below derive the
    key the same way the implementation does and so cannot pin the transform.

    A reader matches these keys against the paths it knows the project by --
    `workflow`'s e2e suite matches by suffix with the extension stripped -- so
    both halves matter: dots become separators, and the file has an extension.
    """
    assert runtime._manifest_file("workflows.99_e2e") == "workflows/99_e2e.py"
    assert runtime._manifest_file("app") == "app.py"


async def test_it_is_not_served_until_it_is_published() -> None:
    """Mounting the route publishes nothing by itself."""
    response = await registry.manifest_handler(Request())

    assert response.status == 404
    assert response.body == b""


@pytest.mark.parametrize("value", ["0", "true", ""], ids=["zero", "true", "empty"])
async def test_only_the_documented_value_publishes_it(value, monkeypatch) -> None:
    """`=1`, exactly, as upstream compares it."""
    monkeypatch.setenv(runtime.PUBLIC_MANIFEST_ENV, value)

    assert (await registry.manifest_handler(Request())).status == 404


async def test_it_is_served_as_json_once_published(published) -> None:
    response = await registry.manifest_handler(Request())

    assert response.status == 200
    assert response.headers["content-type"] == "application/json"
    assert json.loads(response.body)["version"] == runtime.MANIFEST_VERSION


async def test_the_document_indexes_workflows_and_steps_by_source_file(published) -> None:
    """Keyed file, then function, then id -- the shape `createManifest` writes.

    The ids are asserted literally rather than read back off the registry: a
    reader joins runs to this document on them, so the scheme is the contract,
    not an implementation detail.
    """
    manifest = json.loads((await registry.manifest_handler(Request())).body)

    assert manifest["workflows"] == {
        THIS_FILE: {
            "checkout": {
                "workflowId": f"workflow//{__name__}//checkout",
                # Empty, as upstream's own fallback is: its graphs come from
                # static analysis of a bundle, which has no counterpart here.
                "graph": {"nodes": [], "edges": []},
            }
        }
    }
    assert manifest["steps"] == {THIS_FILE: {"charge": {"stepId": f"step//{__name__}//charge"}}}
    # A TypeScript-only construct, but present: a reader indexes into it.
    assert manifest["classes"] == {}


async def test_the_ids_are_the_ones_the_runtime_uses(published) -> None:
    """The join would be silently empty if these ever drifted apart."""
    manifest = json.loads((await registry.manifest_handler(Request())).body)

    assert manifest["workflows"][THIS_FILE]["checkout"]["workflowId"] == checkout.workflow_id
    assert manifest["steps"][THIS_FILE]["charge"]["stepId"] == charge.name


async def test_an_empty_registry_still_answers(published) -> None:
    """A process that registered nothing is not an error, just an empty index."""
    manifest = json.loads(
        (await core.Workflows(as_vercel_job=False).manifest_handler(Request())).body
    )

    assert manifest == {
        "version": runtime.MANIFEST_VERSION,
        "steps": {},
        "workflows": {},
        "classes": {},
    }
