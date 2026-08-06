"""Tests for the URL overrides VercelWorld reads, mirroring world-vercel's
``utils.ts``.

Two independent knobs, one per hop:

- ``VERCEL_WORKFLOW_SERVER_URL``, the workflow-server itself. Preview
  deployments carry it, so a Python app has to honour it to reach the same
  workflow-server as its TypeScript peers.
- ``WORKFLOW_VERCEL_BACKEND_URL``, which swaps the api.vercel.com proxy the
  world talks to when it has project config.
"""

from __future__ import annotations

import pytest

from vercel._internal.workflow.worlds import vercel as vercel_mod

BRANCH_URL = "https://workflow-server-git-branch.vercel.sh"
BACKEND_URL = "https://api-vercel-git-branch.vercel.sh/v1/workflow"


@pytest.fixture(autouse=True)
def _clean_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start every test from "neither variable set"."""
    monkeypatch.delenv("VERCEL_WORKFLOW_SERVER_URL", raising=False)
    monkeypatch.delenv("WORKFLOW_VERCEL_BACKEND_URL", raising=False)


def test_defaults_to_production_without_an_override() -> None:
    world = vercel_mod.VercelWorld(token="tok")

    assert world._base_url == "https://vercel-workflow.com/api"
    assert "x-vercel-workflow-api-url" not in world._headers


def test_env_var_redirects_the_direct_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_WORKFLOW_SERVER_URL", BRANCH_URL)

    world = vercel_mod.VercelWorld(token="tok")

    assert world._base_url == f"{BRANCH_URL}/api"


def test_env_var_reaches_the_proxy_as_a_header(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VERCEL_WORKFLOW_SERVER_URL", BRANCH_URL)

    world = vercel_mod.VercelWorld(token="tok", project_id="prj_1", team_id="team_1")

    # The proxy is still api.vercel.com; the override travels in the header it
    # forwards to the workflow-server.
    assert world._base_url == "https://api.vercel.com/v1/workflow"
    assert world._headers["x-vercel-workflow-api-url"] == BRANCH_URL


def test_backend_url_swaps_the_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORKFLOW_VERCEL_BACKEND_URL", BACKEND_URL)

    world = vercel_mod.VercelWorld(token="tok", project_id="prj_1", team_id="team_1")

    assert world._base_url == BACKEND_URL
    assert world._queue_client().base_url == f"{BACKEND_URL}/queues-proxy"


def test_backend_url_is_ignored_without_project_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """No project config means no proxy, so there is nothing to swap."""
    monkeypatch.setenv("WORKFLOW_VERCEL_BACKEND_URL", BACKEND_URL)

    world = vercel_mod.VercelWorld(token="tok")

    assert world._base_url == "https://vercel-workflow.com/api"


def test_the_two_overrides_are_independent(monkeypatch: pytest.MonkeyPatch) -> None:
    """The proxy moves, and the workflow-server it should route to still
    travels in the header."""
    monkeypatch.setenv("WORKFLOW_VERCEL_BACKEND_URL", BACKEND_URL)
    monkeypatch.setenv("VERCEL_WORKFLOW_SERVER_URL", BRANCH_URL)

    world = vercel_mod.VercelWorld(token="tok", project_id="prj_1", team_id="team_1")

    assert world._base_url == BACKEND_URL
    assert world._headers["x-vercel-workflow-api-url"] == BRANCH_URL


def test_override_is_read_per_world_not_at_import(monkeypatch: pytest.MonkeyPatch) -> None:
    """The env var is resolved when a world is built, so a process that sets it
    after importing this module still picks it up."""
    before = vercel_mod.VercelWorld(token="tok")
    monkeypatch.setenv("VERCEL_WORKFLOW_SERVER_URL", BRANCH_URL)
    after = vercel_mod.VercelWorld(token="tok")

    assert before._base_url == "https://vercel-workflow.com/api"
    assert after._base_url == f"{BRANCH_URL}/api"
