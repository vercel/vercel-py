"""Interop: the JavaScript ``workflow`` CLI reading Python-written local files.

``LocalWorld`` shares its ``.workflow-data`` directory with the TypeScript
``@workflow/world-local`` package, and the unit tests in
``tests/unit/test_workflow_local_world_format.py`` pin the file format against
hand-written expectations. Those expectations can only encode what their author
believed the TS side does. This test runs the real consumer instead: a genuine
Python workflow executes end to end against a real ``LocalWorld``, and then the
published ``workflow`` CLI is asked to read what it wrote.

Nothing is faked. ``LocalWorld`` runs its embedded queue service in-process,
so the run goes through the real queue and the real handlers -- the same path
``vercel dev`` takes -- and every file the CLI reads was written by the
implementation under test.

Metadata *and* payloads are asserted: Python writes a single ``devl``-prefixed
devalue payload, so the CLI's hydration pipeline reads the values Python
recorded rather than failing soft to ``{}``.

The suite needs Node. Following the precedent in ``tests/integration/
test_devalue.py`` it is optional locally and mandatory on CI, so a developer
without Node is not blocked but a real divergence cannot merge green. Locally
the first run pays an ``npm install`` (~450 packages), cached in a
version-keyed temp directory and reused; CI installs the CLI in its own step
and points ``VERCEL_WORKFLOW_CLI`` at it, so npm never runs inside a test
there. Set that variable to a ``workflow`` checkout's ``bin/run.js`` to test
against something other than the pinned release -- useful when a format change
is being made on both sides at once.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest

from vercel._internal.workflow import core, runtime, serialization as ser, world as w
from vercel._internal.workflow.worlds import local as local_mod
from vercel.queue.testing import clear_subscriptions

# The version fetched from npm when VERCEL_WORKFLOW_CLI is not set. Keep this in
# step with the `@workflow/world-local` release the on-disk format is written
# against -- an older CLI predates fields the format now carries -- and with the
# pin in the "Install the workflow CLI" step of .github/workflows/ci.yml.
WORKFLOW_CLI_VERSION = "5.0.0-beta.38"

_IS_CI = bool(os.getenv("CI"))

# How long the Python side gets to finish the run. It takes well under a second;
# this is only here so that a run which never finishes fails the test instead of
# hanging until the CI job's own timeout.
RUN_DEADLINE_SECONDS = 30


# ═══════════════════════════════════════════════════════════════════════════
# locating the JavaScript CLI
# ═══════════════════════════════════════════════════════════════════════════


def _entry_in(prefix: Path) -> Path:
    return prefix / "node_modules" / "workflow" / "bin" / "run.js"


def _npm_install_into(prefix: Path) -> bool:
    prefix.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            "npm",
            "install",
            f"workflow@{WORKFLOW_CLI_VERSION}",
            "--no-save",
            "--no-audit",
            "--no-fund",
            # The tree pulls in @swc/core, esbuild and cbor-extract, whose
            # postinstalls fetch native binaries. `inspect` is a pure-JS read
            # path that needs none of them, so skipping the scripts is both
            # faster and one less thing executing from the registry.
            "--ignore-scripts",
            "--prefix",
            str(prefix),
        ],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    return result.returncode == 0 and _entry_in(prefix).is_file()


def _npm_install_cli() -> Path | None:
    """Install the pinned CLI into a version-keyed temp dir, and reuse it.

    Mirrors ``test_devalue.py``: pytest-xdist gives every worker its own session
    fixture, so several processes reach this at once. npm rebuilds
    ``node_modules`` in place and non-atomically, so installing straight into
    the shared directory lets one worker observe ``run.js`` and then have it
    vanish underneath a running node. Each worker therefore installs into a
    private staging directory and publishes it with a single atomic rename;
    losers of that race reuse the winner's copy.
    """
    shared = Path(tempfile.gettempdir()) / f"vercel-py-workflow-cli-{WORKFLOW_CLI_VERSION}"
    if _entry_in(shared).is_file():
        return _entry_in(shared)

    staging = Path(tempfile.mkdtemp(prefix=f"vercel-py-workflow-cli-{WORKFLOW_CLI_VERSION}-"))
    if not _npm_install_into(staging):
        shutil.rmtree(staging, ignore_errors=True)
        return None

    try:
        # Atomic when `shared` does not exist; raises if another worker won.
        staging.rename(shared)
    except OSError:
        if _entry_in(shared).is_file():
            shutil.rmtree(staging, ignore_errors=True)
            return _entry_in(shared)
        # `shared` exists but holds no usable install (e.g. a partial tree from
        # an interrupted run). Keep our own copy rather than deleting a path
        # another process may be reading.
        return _entry_in(staging)

    return _entry_in(shared)


def _resolve_cli_entry() -> tuple[Path | None, str]:
    """Return the CLI entry point, or ``None`` plus the reason it is missing."""
    override = os.getenv("VERCEL_WORKFLOW_CLI")
    if override:
        candidate = Path(override).expanduser()
        if candidate.is_dir():
            candidate = candidate / "bin" / "run.js"
        if not candidate.is_file():
            return None, f"VERCEL_WORKFLOW_CLI does not point at a workflow CLI: {override}"
        return candidate, ""

    if shutil.which("node") is None:
        return None, "node is not installed"
    if shutil.which("npm") is None:
        return None, "npm is not installed"

    entry = _npm_install_cli()
    if entry is None:
        return None, f"could not npm install workflow@{WORKFLOW_CLI_VERSION}"
    return entry, ""


@pytest.fixture(scope="session")
def workflow_cli() -> Path:
    entry, reason = _resolve_cli_entry()
    if entry is None:
        message = (
            f"JS workflow CLI unavailable ({reason}). Install node, or point "
            f"VERCEL_WORKFLOW_CLI at a workflow checkout's bin/run.js."
        )
        if _IS_CI:
            pytest.fail(message)
        pytest.skip(message)
    return entry


def _inspect(cli: Path, data_dir: Path, *args: str) -> Any:
    """Run ``workflow inspect … --json`` against ``data_dir`` and parse stdout.

    In JSON mode the CLI sends every log line to stderr, so stdout is pure
    JSON. The environment is scrubbed of the other ``WORKFLOW_*`` variables so
    an ambient one cannot silently redirect the command at another backend or
    data directory, and the working directory is the throwaway data dir so no
    ``workflow`` config or ``.workflow-data`` from this repo is discovered.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("WORKFLOW_")}
    env.pop("DEBUG", None)
    env["WORKFLOW_LOCAL_DATA_DIR"] = str(data_dir)
    env["WORKFLOW_NO_UPDATE_CHECK"] = "1"

    result = subprocess.run(
        ["node", str(cli), "inspect", *args, "--backend", "local", "--json"],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=data_dir,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"`workflow inspect {' '.join(args)}` exited {result.returncode}\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError(
            f"`workflow inspect {' '.join(args)}` did not print JSON: {error}\n"
            f"--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
        ) from None


# ═══════════════════════════════════════════════════════════════════════════
# the Python workflow under test
# ═══════════════════════════════════════════════════════════════════════════

# Module level, not function level: replay re-imports the workflow's defining
# module by name inside the sandbox, so the workflow has to live somewhere
# importable. `as_vercel_job=False` only defers subscribing the handlers to the
# queue -- `py_run` does that itself, once a world pointing at its `tmp_path`
# is installed.
registry = core.Workflows(as_vercel_job=False)


@registry.step
async def charge(*, amount: int) -> int:
    """A named parameter, so the call is recorded name-keyed: `[{"amount": 21}]`."""
    return amount * 2


@registry.step
async def notify(total: int, /) -> str:
    """Positional-only, so the call is recorded the way TS records `[42]`."""
    return f"charged {total}"


@registry.workflow
async def checkout(amount: int) -> str:
    total = await charge(amount=amount)
    return await notify(total)


@pytest.fixture(autouse=True)
def _reset_world():
    yield
    w.set_world(None)
    # Queue subscriptions are process-global and refuse to be registered
    # twice for the same topic pattern, so each test has to hand its own back.
    clear_subscriptions()


@pytest.fixture
async def py_run(tmp_path, monkeypatch) -> tuple[Path, str]:
    """Execute the workflow for real and return its data dir and run id.

    Nothing is faked. `LocalWorld` runs an embedded queue service in-process,
    and `workflow_entrypoint` subscribes the real combined handler to it --
    which is what `Workflows()` does for itself outside a test, and what a
    developer gets from `vercel dev`. Doing it here rather than at import is
    what lets the run land in this test's `tmp_path`: the entrypoint binds
    whichever world is installed when it is called.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)
    runtime.workflow_entrypoint(registry)

    try:
        run = await runtime.start(checkout, 21)
        result = await asyncio.wait_for(run.return_value(), RUN_DEADLINE_SECONDS)

        # Guard the premise: if Python itself did not finish the run, a later
        # assertion about the CLI's view would be misleading.
        assert result == "charged 42", f"python run returned {result!r}"
        final = await world.runs_get(run.run_id)
        assert final.status == "completed", f"python run did not complete: {final.status}"
        assert final.output == ser.dehydrate("charged 42")
    finally:
        # Closed here, before the tests run, rather than in a teardown: the
        # embedded service's cancel scope has to be exited from the task that
        # entered it, and a fixture's teardown is not that task. Nothing below
        # needs the world -- what the CLI reads is on disk.
        await world.aclose()

    return tmp_path, run.run_id


# ═══════════════════════════════════════════════════════════════════════════
# tests
# ═══════════════════════════════════════════════════════════════════════════


async def test_cli_lists_the_run_python_wrote(workflow_cli, py_run) -> None:
    data_dir, run_id = py_run

    page = _inspect(workflow_cli, data_dir, "runs")

    assert page["hasMore"] is False
    assert [run["runId"] for run in page["data"]] == [run_id]
    (run,) = page["data"]
    assert run["status"] == "completed"
    assert run["workflowName"] == checkout.workflow_id
    assert run["specVersion"] == 2
    # Written by py because TS materializes it on every run row.
    assert run["attributes"] == {}
    # Dates survive as `Date`s: the CLI re-serializes them to the same
    # millisecond-precision ISO strings, not to epoch (which is what a null
    # would coerce to through `z.coerce.date()`).
    assert run["createdAt"].endswith("Z")
    assert run["startedAt"] <= run["completedAt"]


async def test_cli_lists_the_steps_python_wrote(workflow_cli, py_run) -> None:
    data_dir, run_id = py_run

    steps = _inspect(workflow_cli, data_dir, "steps", f"--runId={run_id}")

    assert {step["stepName"] for step in steps} == {charge.name, notify.name}
    for step in steps:
        assert step["runId"] == run_id
        assert step["status"] == "completed"
        assert step["attempt"] == 1
        assert step["specVersion"] == 2


async def test_cli_lists_the_event_log_python_wrote(workflow_cli, py_run) -> None:
    data_dir, run_id = py_run

    events = _inspect(workflow_cli, data_dir, "events", f"--runId={run_id}")

    assert all(event["runId"] == run_id for event in events)
    # `inspect events` sorts newest first. Reversed, this is the exact lifecycle
    # the Python runtime recorded -- so the CLI agrees on both the set of events
    # and their (createdAt, eventId) ordering.
    assert [event["eventType"] for event in reversed(events)] == [
        "run_created",
        "run_started",
        "step_created",
        "step_started",
        "step_completed",
        "step_created",
        "step_started",
        "step_completed",
        "run_completed",
    ]
    created = [event for event in events if event["eventType"] == "step_created"]
    assert {event["eventData"]["stepName"] for event in created} == {charge.name, notify.name}


async def test_cli_shows_the_single_run_python_wrote(workflow_cli, py_run) -> None:
    # The detail view takes a different code path from the list view: it
    # resolves data and runs the hydration pipeline over the payloads.
    data_dir, run_id = py_run

    run = _inspect(workflow_cli, data_dir, "run", run_id)

    assert run["runId"] == run_id
    assert run["status"] == "completed"
    assert run["workflowName"] == checkout.workflow_id


async def test_cli_hydrates_the_payloads_python_wrote(workflow_cli, py_run) -> None:
    """The end of the round trip: TS devalue reads what Python devalue wrote.

    Hydration on the TS side fails *soft* -- an unreadable payload renders as
    ``{}`` rather than erroring -- so this is the assertion that would notice a
    payload-format regression. The other tests here would not.

    The values are also the ones TypeScript would have written: ``checkout``
    names its parameter, so ``input`` here is what ``start(wf, [{amount: 21}])``
    writes -- the single object a JavaScript callee receives.
    """
    data_dir, run_id = py_run

    run = _inspect(workflow_cli, data_dir, "run", run_id)

    assert run["input"] == [{"amount": 21}]
    assert run["output"] == "charged 42"


async def test_cli_hydrates_the_step_payloads_python_wrote(workflow_cli, py_run) -> None:
    data_dir, run_id = py_run

    # `--withData` is what makes the list view resolve and hydrate payloads.
    steps = _inspect(workflow_cli, data_dir, "steps", f"--runId={run_id}", "--withData")
    by_name = {step["stepName"]: step for step in steps}

    # Both encodings, side by side, as the CLI hydrates them: a named parameter
    # is name-keyed, a positional-only one is the bare array TS writes.
    assert by_name[charge.name]["input"] == {"args": [{"amount": 21}]}
    assert by_name[charge.name]["output"] == 42
    assert by_name[notify.name]["input"] == {"args": [42]}
    assert by_name[notify.name]["output"] == "charged 42"
