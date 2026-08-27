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
import contextlib
import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from vercel.queue.testing import clear_subscriptions
from vercel.workflow._internal import core, runtime, serialization as ser, world as w
from vercel.workflow._internal.streams import workflow_run_stream_id as stream_id
from vercel.workflow._internal.worlds import local as local_mod

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

# How long a `workflow health` run gets. Generous on purpose: the command's own
# health-check timeout is 30s, and a failure to answer should surface as its
# report saying so -- which names what went wrong -- rather than as this
# deadline firing first, which names nothing.
CLI_DEADLINE_SECONDS = 90


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
        encoding="utf-8",
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


def _inspect_raw(cli: Path, data_dir: Path, *args: str) -> str:
    """Run ``workflow inspect … --json`` against ``data_dir``, returning stdout.

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
        encoding="utf-8",
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
    return result.stdout


def _inspect(cli: Path, data_dir: Path, *args: str) -> Any:
    """Run ``workflow inspect … --json`` and parse stdout as one JSON value."""
    raw = _inspect_raw(cli, data_dir, *args)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as error:
        raise AssertionError(
            f"`workflow inspect {' '.join(args)}` did not print JSON: {error}\n"
            f"--- stdout ---\n{raw}"
        ) from None


def _inspect_lines(cli: Path, data_dir: Path, *args: str) -> list[Any]:
    """Like :func:`_inspect`, for a command that prints one JSON value per line.

    ``inspect stream`` streams: it writes a line per chunk and returns when the
    stream closes. The timeout inside `_inspect` is what keeps a stream the
    workflow forgot to close from hanging the suite instead of failing it.
    """
    raw = _inspect_raw(cli, data_dir, *args)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


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
    # An attribute write from host context, which the CLI reads back off the
    # run row along with the ones the body wrote.
    await runtime.set_attributes(source="step-body")
    # Streams the progress a client would render, then leaves the stream open:
    # the run's stream spans steps, and `finish` is what ends it.
    writable = runtime.get_writable()
    await writable.write("charging")
    await writable.write({"amount": amount})
    return amount * 2


@registry.step
async def notify(total: int, /) -> str:
    """Positional-only, so the call is recorded the way TS records `[42]`."""
    await runtime.get_writable().write(f"notifying about {total}")
    return f"charged {total}"


@registry.step
async def finish() -> None:
    # The CLI's stream reader blocks until EOF, which is exactly the behavior a
    # browser client sees: nothing closes a run's stream implicitly.
    await runtime.get_writable().close()


@registry.workflow
async def checkout(amount: int) -> str:
    # Two writes from the body, the second a removal: that reaches the wire as
    # an explicit `null`, which the TS reader requires and would reject if it
    # were dropped.
    await runtime.set_attributes(phase="charging", temporary="yes")
    total = await charge(amount=amount)
    result = await notify(total)
    await finish()
    await runtime.remove_attributes("temporary")
    return result


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
    # Materialized by py's own `attr_set` handling, and read back through the
    # TS schema: both writers (the body and a step), and the key the body
    # removed is gone rather than present-and-null.
    assert run["attributes"] == {"phase": "charging", "source": "step-body"}
    # Dates survive as `Date`s: the CLI re-serializes them to the same
    # millisecond-precision ISO strings, not to epoch (which is what a null
    # would coerce to through `z.coerce.date()`).
    assert run["createdAt"].endswith("Z")
    assert run["startedAt"] <= run["completedAt"]


async def test_cli_lists_the_steps_python_wrote(workflow_cli, py_run) -> None:
    data_dir, run_id = py_run

    steps = _inspect(workflow_cli, data_dir, "steps", f"--runId={run_id}")

    assert {step["stepName"] for step in steps} == {charge.name, notify.name, finish.name}
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
        # The body's first attribute write, then `charge`, whose own write
        # lands from host context while the step is running.
        "attr_set",
        "step_created",
        "step_started",
        "attr_set",
        "step_completed",
        *["step_created", "step_started", "step_completed"] * 2,
        # The body's second write, after the last step and before the run ends.
        "attr_set",
        "run_completed",
    ]
    attr_events = [event for event in events if event["eventType"] == "attr_set"]
    assert sorted(event["eventData"]["writer"]["type"] for event in attr_events) == [
        "step",
        "workflow",
        "workflow",
    ]
    created = [event for event in events if event["eventType"] == "step_created"]
    assert {event["eventData"]["stepName"] for event in created} == {
        charge.name,
        notify.name,
        finish.name,
    }


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


async def test_cli_lists_the_stream_python_wrote(workflow_cli, py_run) -> None:
    data_dir, run_id = py_run

    streams = _inspect(workflow_cli, data_dir, "streams", f"--runId={run_id}")

    # Reads `streams/runs/<runId>.json`, the registry py writes alongside the
    # chunk files.
    assert streams == [{"runId": run_id, "streamId": stream_id(run_id)}]


async def test_cli_reads_the_stream_chunks_python_wrote(workflow_cli, py_run) -> None:
    """The real consumer for a stream: `world.streams.get` piped through
    `getDeserializeStream`.

    This is the assertion that would catch a framing regression. Everything
    else about a stream can look right -- the files exist, the CLI lists it --
    while the frames inside are unreadable, and only a consumer that actually
    decodes them notices. It also proves the ordering: the CLI emits chunks in
    stored index order, so the sequence here is the order the steps wrote in,
    across three separate step invocations.
    """
    data_dir, run_id = py_run

    chunks = _inspect_lines(workflow_cli, data_dir, "stream", stream_id(run_id), f"--run={run_id}")

    assert chunks == ["charging", {"amount": 21}, "notifying about 42"]


# ═══════════════════════════════════════════════════════════════════════════
# serving the flow route over real HTTP
# ═══════════════════════════════════════════════════════════════════════════
#
# Everything above reads files. `workflow health` cannot: both of its transports
# are HTTP requests to a running server -- `@workflow/world-local`'s queue *is*
# an HTTP POST to `<baseUrl>/.well-known/workflow/v1/flow`, with no shared broker
# to hand a message over on disk.
#
# So this stands up the half the SDK does not ship: a socket, and an app owner's
# routing. That adapter is as much under test as the handler is; it is the same
# shape as `workbench/python/app.py` in the `workflow` repo.


@contextlib.contextmanager
def _serving(handler: w.HTTPHandler, seen: list[str]) -> Iterator[str]:
    """Serve *handler* on a real port for the duration of the block.

    The server thread hands each request to the event loop the test runs on
    rather than to one of its own: the world, its embedded queue service and the
    subscribed handler all live there, and a second loop would mean two of
    everything.
    """
    # Imported here because replay re-imports this module -- the one defining
    # `checkout` -- inside the workflow sandbox, and executes its imports there.
    # `import httpx` alone fails a run: `httpx/__init__` pulls in its CLI entry
    # point, which pulls in `rich`, which calls `random.getrandbits()` on import.
    import http.server
    import threading

    import httpx

    class ServerRequest(w.HTTPRequest):
        """One `BaseHTTPRequestHandler` request, as an SDK `HTTPRequest`.

        The body arrives already read: the handler runs on the event loop, and
        reading the socket from there would reach into another thread's file
        object.
        """

        def __init__(self, method: str, target: str, headers: Any, body: bytes) -> None:
            self._method = method
            self._target = target
            self._headers = httpx.Headers(headers.items())
            self._body = body

        @property
        def method(self) -> str:
            return self._method

        @property
        def url(self) -> str:
            return self._target

        @property
        def headers(self) -> httpx.Headers:
            return self._headers

        async def aiter_bytes(self, chunk_size: int | None = None):
            yield self._body

    loop = asyncio.get_event_loop()

    class Route(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _serve(self) -> None:
            body = self.rfile.read(int(self.headers.get("content-length") or 0))
            seen.append(f"{self.command} {self.path}")
            if self.path.split("?")[0] != runtime.ENDPOINT_PATH:
                self.send_response(404)
                self.send_header("content-length", "0")
                self.end_headers()
                return
            request = ServerRequest(self.command, self.path, self.headers, body)
            future = asyncio.run_coroutine_threadsafe(handler(request), loop)
            response = future.result(timeout=RUN_DEADLINE_SECONDS)
            self.send_response(response.status)
            for name, value in response.headers.items():
                self.send_header(name, value)
            self.send_header("content-length", str(len(response.body)))
            self.end_headers()
            self.wfile.write(response.body)

        do_POST = do_GET = do_HEAD = do_OPTIONS = _serve

        def log_message(self, *args: Any) -> None:
            """Silence the default stderr access log."""

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Route)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=RUN_DEADLINE_SECONDS)


async def _health(cli: Path, data_dir: Path, base_url: str) -> Any:
    """Run ``workflow health --json`` against a server at *base_url*.

    `WORKFLOW_LOCAL_BASE_URL` is what both halves of the command read: the CLI
    resolves the URL to precheck from it, and `@workflow/world-local` publishes
    the queue probe to it. Setting it also settles which port is used, instead
    of leaving that to the CLI's port detection.

    Run on the loop rather than through `subprocess.run`: this loop serves the
    requests the command is waiting for, so blocking it would deadlock.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("WORKFLOW_")}
    env.pop("DEBUG", None)
    env["WORKFLOW_LOCAL_DATA_DIR"] = str(data_dir)
    env["WORKFLOW_LOCAL_BASE_URL"] = base_url
    env["WORKFLOW_NO_UPDATE_CHECK"] = "1"

    process = await asyncio.create_subprocess_exec(
        "node",
        str(cli),
        "health",
        "--backend",
        "local",
        "--json",
        cwd=data_dir,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), CLI_DEADLINE_SECONDS)
    if process.returncode != 0:
        raise AssertionError(
            f"`workflow health` exited {process.returncode}\n"
            f"--- stdout ---\n{stdout.decode()}\n--- stderr ---\n{stderr.decode()}"
        )
    return json.loads(stdout)


async def test_cli_health_check_is_answered_by_python(workflow_cli, tmp_path, monkeypatch) -> None:
    """The real `workflow health`, end to end, against a real Python endpoint.

    The only test here where the CLI writes as well as reads, so it covers what
    no file-format assertion can: that the probe Node publishes is recognised,
    and that the answer its reader polls for is one it can parse. Every name in
    the exchange -- queue topic, response stream, synthetic run -- is derived on
    each side and never sent, so one drifting shows up here and nowhere else.
    """
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = local_mod.LocalWorld()
    w.set_world(world)
    flow = runtime.workflow_entrypoint(registry)

    # Start the embedded queue service from this task, before any request can.
    # Its cancel scope has to be exited by the task that entered it, and the
    # deliveries below run in tasks of their own -- so `aclose()` in the
    # `finally` would fail if the first delivery had opened it.
    await world._get_queue_client()

    seen: list[str] = []
    try:
        with _serving(flow, seen) as base_url:
            report = await _health(workflow_cli, tmp_path, base_url)
    finally:
        await world.aclose()

    assert report["allHealthy"] is True, report
    assert report["results"] == [
        {
            "endpoint": "workflow",
            "healthy": True,
            "latencyMs": report["results"][0].get("latencyMs"),
        }
    ]

    # Both transports, in the order the command uses them: the HTTP probe as a
    # reachability precheck, then the queue probe as the actual health check.
    assert seen == [
        f"POST {runtime.ENDPOINT_PATH}?__health",
        f"POST {runtime.ENDPOINT_PATH}",
    ]
