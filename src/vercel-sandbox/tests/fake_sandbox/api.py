"""A stateful, in-memory Sandbox API.

`FakeSandboxApi` serves the endpoints `vercel.sandbox` calls from memory. It
plugs into the SDK through the public `httpx_client_factory` seam, so every
layer above HTTP is the real code: options, the session, the service, the API
client and its wire models, and the sync and async runtimes.

Behaviour follows the API service (`vercel/api`, `services/api-sandboxes`):
response envelopes, error codes and statuses, lifecycle transitions, resume
and snapshot semantics, and network-policy redaction. Where the fake is
simpler it says so, and tests that need the API to misbehave say so
explicitly with `fail`, `respond`, or `hold`.
"""

from __future__ import annotations

import gzip
import io
import itertools
import json
import re
import tarfile
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, NoReturn
from urllib.parse import unquote

import anyio
import httpx2 as httpx
import sniffio

from vercel._internal.core.options import ServiceOptions
from vercel.api import session as sdk_session
from vercel.sandbox import SandboxCredentials, SandboxServiceOptions
from vercel.sandbox._internal.interactive_session import InteractiveConnector
from vercel.sandbox._internal.runtime_options import SandboxRuntimeOptions
from vercel.sandbox.sync import SandboxServiceOptions as SyncSandboxServiceOptions

from . import network_policy, wire
from .clock import VirtualClock
from .filesystem import FakeFilesystem, File, FilesystemError
from .interactive import FakeInteractiveConnector, FakeTerminal
from .programs import ExecutableNotFound, ProgramRegistry, ProgramResult
from .state import FakeCommand, FakeDrive, FakeSandbox, FakeSession, FakeSnapshot

BASE_URL = "https://sandbox.test"

_CREATE_FIELDS = frozenset(
    {
        "env",
        "failoverRegions",
        "image",
        "keepLastSnapshots",
        "mounts",
        "name",
        "networkId",
        "networkPolicy",
        "persistent",
        "ports",
        "projectId",
        "region",
        "resources",
        "snapshotExpiration",
        "source",
        "tags",
        "timeout",
    }
)
_FORK_FIELDS = _CREATE_FIELDS - {"projectId", "source"}
_UPDATE_FIELDS = frozenset(
    {
        "currentSnapshotId",
        "env",
        "failoverRegions",
        "keepLastSnapshots",
        "mounts",
        "networkId",
        "networkPolicy",
        "persistent",
        "ports",
        "region",
        "resources",
        "snapshotExpiration",
        "tags",
        "timeout",
    }
)


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message

    def response(self) -> httpx.Response:
        return httpx.Response(self.status, json=wire.error_payload(self.code, self.message))


@dataclass(frozen=True)
class RecordedRequest:
    """One request the SDK sent, with the route it matched."""

    route: str
    method: str
    path: str
    params: httpx.QueryParams
    headers: httpx.Headers
    content: bytes
    path_params: Mapping[str, str] = field(default_factory=dict)

    def json(self) -> Any:
        return json.loads(self.content) if self.content else None


@dataclass
class _Override:
    route: str
    response: Callable[[RecordedRequest], httpx.Response]
    times: int
    when: Callable[[RecordedRequest], bool] | None


class Gate:
    """Hold matching requests until released.

    By default a request is held before it is served, so the fake's state has
    not changed yet. With ``after_response`` it is served first and only the
    reply is held, which is how a test delivers a stale reply after some
    other operation has moved on. ``times`` bounds how many requests one gate
    captures; later ones pass through, or into the next gate.
    """

    def __init__(
        self,
        route: str,
        when: Callable[[RecordedRequest], bool] | None,
        *,
        times: int | None,
        after_response: bool,
    ) -> None:
        self.route = route
        self.when = when
        self.times = times
        self.after_response = after_response
        self.entered = 0
        self._released = threading.Event()

    def release(self) -> None:
        self._released.set()

    @property
    def released(self) -> bool:
        return self._released.is_set()

    def _captures(self, request: RecordedRequest) -> bool:
        if self.released or self.route != request.route:
            return False
        if self.times is not None and self.entered >= self.times:
            return False
        return self.when is None or self.when(request)

    async def wait_entered(self, count: int = 1) -> None:
        """Wait, in an event loop, until ``count`` requests are held."""
        with anyio.fail_after(5):
            while self.entered < count:
                await anyio.sleep(0.001)

    def wait_entered_sync(self, count: int = 1) -> None:
        """Wait, from a thread, until ``count`` requests are held."""
        deadline = time.monotonic() + 5
        while self.entered < count:
            if time.monotonic() > deadline:
                raise TimeoutError(f"no request reached {self.route}")
            time.sleep(0.001)

    def _block_sync(self) -> None:
        if not self._released.wait(10):
            raise TimeoutError(f"{self.route} was held and never released")

    async def _block_async(self) -> None:
        with anyio.fail_after(10):
            while not self._released.is_set():
                await anyio.sleep(0.001)


_Handler = Callable[["FakeSandboxApi", RecordedRequest], httpx.Response]

_ROUTES: list[tuple[str, re.Pattern[str], str]] = [
    (method, re.compile(f"^{pattern}$"), name)
    for method, pattern, name in [
        ("POST", r"/v3/sandboxes", "sandboxes.create"),
        ("GET", r"/v2/sandboxes", "sandboxes.list"),
        ("GET", r"/v2/sandboxes/sessions", "sessions.list"),
        ("GET", r"/v2/sandboxes/snapshots", "snapshots.list"),
        ("GET", r"/v2/sandboxes/snapshots/(?P<snapshot_id>[^/]+)", "snapshots.get"),
        ("DELETE", r"/v2/sandboxes/snapshots/(?P<snapshot_id>[^/]+)", "snapshots.delete"),
        ("GET", r"/v2/sandboxes/drives", "drives.list"),
        ("GET", r"/v2/sandboxes/drives/(?P<name>[^/]+)", "drives.get"),
        ("POST", r"/v2/sandboxes/drives/(?P<name>[^/]+)", "drives.get_or_create"),
        ("DELETE", r"/v2/sandboxes/drives/(?P<name>[^/]+)", "drives.delete"),
        ("GET", r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)", "sessions.get"),
        ("POST", r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/stop", "sessions.stop"),
        (
            "POST",
            r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/extend-timeout",
            "sessions.extend_timeout",
        ),
        (
            "POST",
            r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/network-policy",
            "sessions.network_policy",
        ),
        ("POST", r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/snapshot", "sessions.snapshot"),
        (
            "POST",
            r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/interactive",
            "sessions.interactive",
        ),
        ("POST", r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/cmd", "commands.create"),
        ("GET", r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/cmd", "commands.list"),
        (
            "GET",
            r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/cmd/(?P<command_id>[^/]+)",
            "commands.get",
        ),
        (
            "POST",
            r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/cmd/(?P<command_id>[^/]+)/kill",
            "commands.kill",
        ),
        (
            "GET",
            r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/cmd/(?P<command_id>[^/]+)/logs",
            "commands.logs",
        ),
        ("POST", r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/fs/read", "fs.read"),
        ("POST", r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/fs/write", "fs.write"),
        ("POST", r"/v2/sandboxes/sessions/(?P<session_id>[^/]+)/fs/mkdir", "fs.mkdir"),
        ("POST", r"/v2/sandboxes/(?P<name>[^/]+)/fork", "sandboxes.fork"),
        ("GET", r"/v2/sandboxes/(?P<name>[^/]+)", "sandboxes.get"),
        ("PATCH", r"/v2/sandboxes/(?P<name>[^/]+)", "sandboxes.update"),
        ("DELETE", r"/v2/sandboxes/(?P<name>[^/]+)", "sandboxes.delete"),
    ]
]


def _ndjson(records: list[wire.JSON]) -> httpx.Response:
    content = "".join(json.dumps(record) + "\n" for record in records).encode()
    return httpx.Response(200, content=content, headers={"content-type": "application/x-ndjson"})


def _not_found(message: str) -> NoReturn:
    raise ApiError(404, "not_found", message)


def _bad_request(message: str) -> NoReturn:
    raise ApiError(400, "bad_request", message)


class FakeSandboxApi:
    """The Sandbox API, in memory.

    Args:
        token: Bearer token the SDK must send.
        team_id: Team the SDK must scope requests to.
        project_id: The credentials' project; the default for project-scoped
            requests that do not name one.
        project_names: Project names the API resolves to these IDs, as it
            accepts either wherever it takes a project.
        region: Region the SDK's service options default placements to.
        startup_seconds: How long a new session stays ``pending``.
    """

    def __init__(
        self,
        *,
        token: str = "token",
        team_id: str = "team_123",
        project_id: str = "prj_123",
        project_names: Mapping[str, str] | None = None,
        region: str | None = None,
        startup_seconds: float = 0.0,
        clock: VirtualClock | None = None,
    ) -> None:
        self.token = token
        self.team_id = team_id
        self.project_id = project_id
        self.project_names = dict(project_names or {})
        self.region = region
        self.startup_seconds = startup_seconds
        self.resolve_image: Callable[[str], str] = lambda reference: reference
        self.clock = clock or VirtualClock()
        self.programs = ProgramRegistry()
        self.interactive: InteractiveConnector = FakeInteractiveConnector(self)
        self.sandboxes: dict[tuple[str, str], FakeSandbox] = {}
        self.sessions: dict[str, FakeSession] = {}
        self.snapshots: dict[str, FakeSnapshot] = {}
        self.drives: dict[tuple[str, str], FakeDrive] = {}
        self.requests: list[RecordedRequest] = []
        self.terminals: list[FakeTerminal] = []
        self.interactive_tickets: dict[str, str] = {}
        self._overrides: list[_Override] = []
        self._gates: list[Gate] = []
        self._ids: dict[str, Iterator[int]] = {}
        self._lock = threading.RLock()

    # ---- wiring the SDK to the fake ---------------------------------------

    def credentials(self) -> SandboxCredentials:
        return SandboxCredentials(
            token=self.token, team_id=self.team_id, project_id=self.project_id
        )

    def service_options(self, *, sync: bool) -> list[ServiceOptions]:
        """Session options that point a sync or async session at the fake."""
        credentials = self.credentials()
        sandbox_options: ServiceOptions
        if sync:
            sandbox_options = SyncSandboxServiceOptions(
                base_url=BASE_URL, credentials_factory=lambda: credentials, region=self.region
            )
        else:

            async def credentials_factory() -> SandboxCredentials:
                return credentials

            sandbox_options = SandboxServiceOptions(
                base_url=BASE_URL, credentials_factory=credentials_factory, region=self.region
            )
        runtime = SandboxRuntimeOptions(
            sleep=self.clock.sleep if sync else self.clock.async_sleep,
            interactive=self.interactive,
        )
        return [sandbox_options, runtime]

    def httpx_client_factory(self, *, sync: bool) -> Callable[[], Any]:
        if sync:
            return lambda: httpx.Client(transport=httpx.MockTransport(self._handle_sync))
        return lambda: httpx.AsyncClient(transport=httpx.MockTransport(self._handle_async))

    def session_kwargs(self, *, sync: bool) -> dict[str, Any]:
        return {
            "service_options": self.service_options(sync=sync),
            "httpx_client_factory": self.httpx_client_factory(sync=sync),
        }

    def session(self, *, sync: bool | None = None) -> Any:
        """A `vercel.api.session(...)` bound to the fake.

        The mode defaults to the caller's: async inside an event loop, sync
        outside one.
        """
        if sync is None:
            try:
                sniffio.current_async_library()
            except sniffio.AsyncLibraryNotFoundError:
                sync = True
            else:
                sync = False
        return sdk_session(**self.session_kwargs(sync=sync))

    # ---- shaping the fake's behaviour ---------------------------------------

    def command(self, pattern: str, program: Any = None, **result: Any) -> None:
        """Handle a command line, as `vercel-sandbox-mock`'s `command()` does.

        Pass a program callable, or `stdout`, `stderr`, `exit_code`, and
        `duration` for a fixed result.
        """
        self.programs.register(
            pattern, program if program is not None else ProgramResult.of(**result)
        )

    def respond(
        self,
        route: str,
        response: httpx.Response | Callable[[RecordedRequest], httpx.Response],
        *,
        times: int = 1,
        when: Callable[[RecordedRequest], bool] | None = None,
    ) -> None:
        """Answer the next matching request with ``response``, changing nothing."""
        factory = response if callable(response) else (lambda _request: response)
        self._overrides.append(_Override(route, factory, times, when))

    def fail(
        self,
        route: str,
        *,
        status: int,
        code: str,
        message: str = "injected failure",
        times: int = 1,
        when: Callable[[RecordedRequest], bool] | None = None,
    ) -> None:
        """Answer the next matching request with an API error."""
        self.respond(
            route,
            lambda _request: httpx.Response(status, json=wire.error_payload(code, message)),
            times=times,
            when=when,
        )

    def hold(
        self,
        route: str,
        *,
        when: Callable[[RecordedRequest], bool] | None = None,
        times: int | None = None,
        after_response: bool = False,
    ) -> Gate:
        """Hold matching requests until the returned gate is released."""
        gate = Gate(route, when, times=times, after_response=after_response)
        self._gates.append(gate)
        return gate

    # ---- inspecting state ---------------------------------------------------

    def sandbox(self, name: str, *, project_id: str | None = None) -> FakeSandbox:
        return self.sandboxes[(self._canonical(project_id), name)]

    def current_session(self, name: str, *, project_id: str | None = None) -> FakeSession:
        return self.sessions[self.sandbox(name, project_id=project_id).current_session_id]

    def requests_to(
        self, route: str, *, when: Callable[[RecordedRequest], bool] | None = None
    ) -> list[RecordedRequest]:
        return [
            request
            for request in self.requests
            if request.route == route and (when is None or when(request))
        ]

    def routes(self) -> list[str]:
        """The route of every request so far, in order."""
        return [request.route for request in self.requests]

    async def wait_for_requests(
        self,
        route: str,
        count: int = 1,
        *,
        when: Callable[[RecordedRequest], bool] | None = None,
    ) -> None:
        """Wait, in an event loop, until ``count`` matching requests arrived."""
        with anyio.fail_after(5):
            while len(self.requests_to(route, when=when)) < count:
                await anyio.sleep(0.001)

    def wait_for_requests_sync(
        self,
        route: str,
        count: int = 1,
        *,
        when: Callable[[RecordedRequest], bool] | None = None,
    ) -> None:
        """Wait, from a thread, until ``count`` matching requests arrived."""
        deadline = time.monotonic() + 5
        while len(self.requests_to(route, when=when)) < count:
            if time.monotonic() > deadline:
                raise TimeoutError(f"{route} saw fewer than {count} requests")
            time.sleep(0.001)

    def stop(
        self, name: str, *, project_id: str | None = None, status: str = "stopped"
    ) -> FakeSession:
        """Move a sandbox's session out of band, as a timeout or the platform would.

        ``status`` may also be a transitional state (``stopping``,
        ``snapshotting``) or ``failed``.
        """
        with self._lock:
            session = self.current_session(name, project_id=project_id)
            if status == "stopped":
                self._stop_session(session)
            else:
                session.status = status
            return session

    def seed(
        self,
        name: str = "preview",
        *,
        project_id: str | None = None,
        files: Mapping[str, bytes | str] | None = None,
        **fields: Any,
    ) -> FakeSandbox:
        """Create a sandbox with a running session without going through the SDK.

        ``fields`` are request-body fields as `POST /v3/sandboxes` takes them.
        """
        with self._lock:
            project = self._canonical(project_id)
            sandbox = self._new_sandbox({"name": name, **fields}, project_id=project, source=None)
            self.sandboxes[(project, name)] = sandbox
            session = self._start_session(sandbox, files=None, source_snapshot_id=None)
            for path, data in (files or {}).items():
                session.fs.write(
                    FakeFilesystem.resolve(session.cwd, path),
                    data.encode() if isinstance(data, str) else data,
                )
            return sandbox

    def seed_snapshot(
        self, name: str = "preview", *, project_id: str | None = None
    ) -> FakeSnapshot:
        """Snapshot a sandbox's current filesystem without going through the SDK."""
        with self._lock:
            sandbox = self.sandbox(name, project_id=project_id)
            return self._snapshot(
                sandbox, self.sessions[sandbox.current_session_id], method="manual"
            )

    # ---- request handling ----------------------------------------------------

    def _match(self, request: httpx.Request) -> RecordedRequest:
        path = request.url.path
        raw_path = request.url.raw_path.split(b"?", 1)[0].decode("ascii")
        for method, pattern, name in _ROUTES:
            if method == request.method and (match := pattern.match(raw_path)):
                route = name
                path_params = {key: unquote(value) for key, value in match.groupdict().items()}
                break
        else:
            route, path_params = "unknown", {}
        return RecordedRequest(
            route=route,
            method=request.method,
            path=path,
            params=request.url.params,
            headers=request.headers,
            content=request.content,
            path_params=path_params,
        )

    def _gate_for(self, request: RecordedRequest) -> Gate | None:
        with self._lock:
            for gate in self._gates:
                if gate._captures(request):
                    gate.entered += 1
                    return gate
        return None

    def _handle_sync(self, request: httpx.Request) -> httpx.Response:
        request.read()
        recorded = self._match(request)
        self.requests.append(recorded)
        gate = self._gate_for(recorded)
        if gate is None:
            return self._serve(recorded)
        if gate.after_response:
            response = self._serve(recorded)
            gate._block_sync()
            return response
        gate._block_sync()
        return self._serve(recorded)

    async def _handle_async(self, request: httpx.Request) -> httpx.Response:
        await request.aread()
        recorded = self._match(request)
        self.requests.append(recorded)
        gate = self._gate_for(recorded)
        if gate is None:
            return self._serve(recorded)
        if gate.after_response:
            response = self._serve(recorded)
            await gate._block_async()
            return response
        await gate._block_async()
        return self._serve(recorded)

    def _serve(self, request: RecordedRequest) -> httpx.Response:
        with self._lock:
            for override in self._overrides:
                if override.route == request.route and override.times > 0:
                    if override.when is None or override.when(request):
                        override.times -= 1
                        return override.response(request)
            try:
                self._authorize(request)
                handler = _HANDLERS.get(request.route)
                if handler is None:
                    _not_found(f"No route for {request.method} {request.path}")
                for session in self.sessions.values():
                    self._refresh(session)
                return handler(self, request)
            except ApiError as error:
                return error.response()

    def _authorize(self, request: RecordedRequest) -> None:
        if request.headers.get("authorization") != f"Bearer {self.token}":
            raise ApiError(403, "forbidden", "Not authorized")
        if request.params.get("teamId") != self.team_id:
            raise ApiError(403, "forbidden", "Not authorized for this team")

    # ---- helpers -------------------------------------------------------------

    def _id(self, prefix: str) -> str:
        counter = self._ids.setdefault(prefix, itertools.count(1))
        return f"{prefix}_{next(counter)}"

    def _now(self) -> int:
        return self.clock.now_ms()

    def _canonical(self, project: str | None) -> str:
        project = project or self.project_id
        return self.project_names.get(project, project)

    def _project(self, request: RecordedRequest, key: str = "projectId") -> str:
        return self._canonical(request.params.get(key))

    def _find_sandbox(self, project_id: str, name: str) -> FakeSandbox:
        sandbox = self.sandboxes.get((project_id, name))
        if sandbox is None:
            _not_found(f"Named sandbox '{name}' not found for this project.")
        return sandbox

    def _find_session(self, request: RecordedRequest) -> FakeSession:
        session = self.sessions.get(request.path_params["session_id"])
        if session is None:
            _not_found("Sandbox not found.")
        return session

    def _session_of(self, sandbox: FakeSandbox) -> FakeSession | None:
        return self.sessions.get(sandbox.current_session_id)

    def _refresh(self, session: FakeSession) -> None:
        was_running = session.status == "running"
        session.refresh(self._now())
        if was_running and session.status == "stopped":
            self._after_stop(session)

    @staticmethod
    def _assert_accepts_commands(session: FakeSession) -> None:
        if session.status == "stopping":
            raise ApiError(
                422, "sandbox_stopping", "Sandbox is stopping and no longer accepts commands."
            )
        if session.status in {"stopped", "aborted"}:
            raise ApiError(
                410,
                "sandbox_stopped",
                "Sandbox has stopped execution and is no longer available",
            )
        if session.status == "failed":
            raise ApiError(
                410,
                "sandbox_failed",
                "Sandbox has failed due to an internal error and is no longer available",
            )
        if session.status == "snapshotting":
            raise ApiError(
                422,
                "sandbox_snapshotting",
                "Sandbox is creating a snapshot and will be stopped shortly.",
            )

    def _start_session(
        self, sandbox: FakeSandbox, *, files: FakeFilesystem | None, source_snapshot_id: str | None
    ) -> FakeSession:
        now = self._now()
        fs = files.copy() if files is not None else FakeFilesystem()
        session = FakeSession(
            id=self._id("sbx"),
            sandbox_name=sandbox.name,
            project_id=sandbox.project_id,
            region=sandbox.region,
            memory=sandbox.memory,
            vcpus=sandbox.vcpus,
            timeout_ms=sandbox.timeout_ms,
            cwd=sandbox.cwd,
            requested_at=now,
            running_at=now + round(self.startup_seconds * 1000),
            fs=fs,
            network_policy=None if sandbox.network_policy is None else dict(sandbox.network_policy),
            network_id=sandbox.network_id,
            env=dict(sandbox.env),
            source_snapshot_id=source_snapshot_id,
        )
        session.refresh(now)
        self.sessions[session.id] = session
        sandbox.current_session_id = session.id
        sandbox.status_updated_at = now
        self._mount_drives(sandbox, session)
        return session

    def _mount_drives(self, sandbox: FakeSandbox, session: FakeSession) -> None:
        for path, mount in (sandbox.mounts or {}).items():
            drive = self.drives.get((sandbox.project_id, mount["drive"]))
            if drive is None:
                continue
            for drive_path, node in drive.files.nodes.items():
                if drive_path == "/":
                    session.fs.mkdir(path, parents=True, exist_ok=True)
                    continue
                target = path.rstrip("/") + drive_path
                if isinstance(node, File):
                    session.fs.write(target, node.data, mode=node.mode)
                else:
                    session.fs.mkdir(target, parents=True, exist_ok=True)
            if mount.get("mode", "read-write") == "read-write":
                drive.current_session_id = session.id
                drive.current_sandbox_name = sandbox.name

    def _unmount_drives(self, sandbox: FakeSandbox, session: FakeSession) -> None:
        for path, mount in (sandbox.mounts or {}).items():
            drive = self.drives.get((sandbox.project_id, mount["drive"]))
            if drive is None or drive.current_session_id != session.id:
                continue
            files = FakeFilesystem()
            prefix = path.rstrip("/")
            for candidate, node in session.fs.nodes.items():
                if candidate.startswith(prefix + "/"):
                    relative = candidate[len(prefix) :]
                    files.nodes[relative] = node
            drive.files = files
            drive.current_session_id = None
            drive.current_sandbox_name = None

    def _snapshot(self, sandbox: FakeSandbox, session: FakeSession, *, method: str) -> FakeSnapshot:
        now = self._now()
        expiration = sandbox.snapshot_expiration
        snapshot = FakeSnapshot(
            id=self._id("snap"),
            sandbox_name=sandbox.name,
            source_session_id=session.id,
            region=session.region,
            files=session.fs.copy(),
            created_at=now,
            image=sandbox.image,
            expires_at=now + expiration if expiration else None,
            creation_method=method,
            parent_id=sandbox.current_snapshot_id,
        )
        self.snapshots[snapshot.id] = snapshot
        sandbox.current_snapshot_id = snapshot.id
        sandbox.updated_at = now
        return snapshot

    def _after_stop(self, session: FakeSession) -> FakeSnapshot | None:
        sandbox = self.sandboxes.get((session.project_id, session.sandbox_name))
        if sandbox is None:
            return None
        self._unmount_drives(sandbox, session)
        sandbox.status_updated_at = self._now()
        if sandbox.persistent and sandbox.current_session_id == session.id:
            return self._snapshot(sandbox, session, method="automatic")
        return None

    def _stop_session(self, session: FakeSession) -> FakeSnapshot | None:
        if session.gone:
            return None
        now = self._now()
        session.status = "stopped"
        session.stopped_at = now
        session.updated_at = now
        return self._after_stop(session)

    def _sandbox_response(
        self, sandbox: FakeSandbox, *, resumed: bool | None = None, system_routes: bool = False
    ) -> httpx.Response:
        session = self._session_of(sandbox)
        return httpx.Response(
            200,
            json=wire.sandbox_response(
                sandbox=sandbox.payload(session),
                session=None if session is None else session.payload(),
                routes=sandbox.routes(include_system=system_routes),
                resumed=resumed,
            ),
        )

    def _read_json(self, request: RecordedRequest, allowed: frozenset[str]) -> wire.JSON:
        body = request.json() or {}
        if not isinstance(body, dict):
            _bad_request("Request body must be a JSON object")
        unknown = {key for key in body if key not in allowed and not key.startswith("__")}
        if unknown:
            _bad_request(f"Unexpected properties: {sorted(unknown)}")
        return body

    def _validated_mounts(self, project_id: str, mounts: Any) -> dict[str, dict[str, str]] | None:
        if mounts is None:
            return None
        resolved: dict[str, dict[str, str]] = {}
        for path, mount in mounts.items():
            if (project_id, mount["drive"]) not in self.drives:
                _not_found(f"Drive '{mount['drive']}' not found.")
            resolved[path] = {"drive": mount["drive"], "mode": mount.get("mode", "read-write")}
        return resolved or None

    def _new_sandbox(
        self, body: wire.JSON, *, project_id: str, source: FakeSandbox | None
    ) -> FakeSandbox:
        name = body.get("name") or self._id("sandbox")
        if (project_id, name) in self.sandboxes:
            _bad_request(
                f"A sandbox with the name '{name}' already exists for this project. "
                "Use GET /sandboxes/:name to resume it or delete it first."
            )
        region = body.get("region") or (source.region if source else "iad1")
        failover = body.get("failoverRegions")
        if failover is None:
            failover = list(source.failover_regions) if source else []
        if region in failover and "region" in body:
            _bad_request(f"failoverRegions must not include the sandbox region: {region}")
        vcpus = (body.get("resources") or {}).get("vcpus") or (source.vcpus if source else 2)
        policy = body.get("networkPolicy")
        session_policy = (
            network_policy.session_readback(policy)
            if policy is not None
            else (dict(source.network_policy) if source and source.network_policy else None)
        )
        now = self._now()
        return FakeSandbox(
            name=name,
            project_id=project_id,
            persistent=body.get("persistent", source.persistent if source else True),
            image=(
                self.resolve_image(body["image"])
                if body.get("image")
                else (source.image if source else wire.DEFAULT_IMAGE)
            ),
            region=region,
            failover_regions=list(failover),
            vcpus=vcpus,
            memory=vcpus * 2048,
            timeout_ms=body.get("timeout") or (source.timeout_ms if source else 300_000),
            created_at=now,
            ports=list(body.get("ports", source.ports if source else [])),
            env=dict(body.get("env") or (source.env if source else {})),
            tags=dict(body.get("tags") or (source.tags if source else {})),
            mounts=self._validated_mounts(project_id, body.get("mounts")),
            network_policy=session_policy,
            network_id=body.get("networkId", source.network_id if source else None),
            snapshot_expiration=body.get(
                "snapshotExpiration", source.snapshot_expiration if source else None
            ),
            keep_last_snapshots=body.get(
                "keepLastSnapshots", source.keep_last_snapshots if source else None
            ),
            private_parameters={key: value for key, value in body.items() if key.startswith("__")},
        )

    # ---- sandboxes -------------------------------------------------------------

    def _create_sandbox(self, request: RecordedRequest) -> httpx.Response:
        body = self._read_json(request, _CREATE_FIELDS)
        project_id = self._canonical(body.get("projectId"))
        files: FakeFilesystem | None = None
        snapshot_id: str | None = None
        source = body.get("source") or {}
        if source.get("type") == "snapshot":
            snapshot = self.snapshots.get(source.get("snapshotId", ""))
            if snapshot is None or snapshot.status != "created":
                raise ApiError(400, "snapshot_not_found", "Snapshot not found.")
            files, snapshot_id = snapshot.files, snapshot.id
        sandbox = self._new_sandbox(body, project_id=project_id, source=None)
        sandbox.source_snapshot_id = snapshot_id
        if snapshot_id is not None and not body.get("image"):
            sandbox.image = self.snapshots[snapshot_id].image
        self.sandboxes[(project_id, sandbox.name)] = sandbox
        self._start_session(sandbox, files=files, source_snapshot_id=snapshot_id)
        return self._sandbox_response(sandbox)

    def _fork_sandbox(self, request: RecordedRequest) -> httpx.Response:
        body = self._read_json(request, _FORK_FIELDS)
        project_id = self._project(request)
        source = self._find_sandbox(project_id, request.path_params["name"])
        sandbox = self._new_sandbox(body, project_id=project_id, source=source)
        snapshot = self.snapshots.get(source.current_snapshot_id or "")
        files = snapshot.files if snapshot is not None and snapshot.status == "created" else None
        sandbox.source_snapshot_id = snapshot.id if files is not None and snapshot else None
        self.sandboxes[(project_id, sandbox.name)] = sandbox
        self._start_session(sandbox, files=files, source_snapshot_id=sandbox.source_snapshot_id)
        return self._sandbox_response(sandbox)

    def _get_sandbox(self, request: RecordedRequest) -> httpx.Response:
        sandbox = self._find_sandbox(self._project(request), request.path_params["name"])
        session = self._session_of(sandbox)
        system_routes = request.params.get("__includeSystemRoutes") == "true"
        if request.params.get("resume") != "true" or (session is not None and not session.gone):
            return self._sandbox_response(sandbox, resumed=False, system_routes=system_routes)
        snapshot = self.snapshots.get(sandbox.current_snapshot_id or "")
        if sandbox.current_snapshot_id and (snapshot is None or snapshot.status != "created"):
            raise ApiError(
                400, "snapshot_not_found", "Cannot resume sandbox: no snapshot available."
            )
        if snapshot is None and not sandbox.persistent:
            raise ApiError(
                400, "snapshot_not_found", "Cannot resume sandbox: no snapshot available."
            )
        self._start_session(
            sandbox,
            files=None if snapshot is None else snapshot.files,
            source_snapshot_id=None if snapshot is None else snapshot.id,
        )
        return self._sandbox_response(sandbox, resumed=True, system_routes=system_routes)

    def _list_sandboxes(self, request: RecordedRequest) -> httpx.Response:
        project_id = self._project(request, "project")
        sandboxes = [
            sandbox for (project, _), sandbox in self.sandboxes.items() if project == project_id
        ]
        if prefix := request.params.get("namePrefix"):
            sandboxes = [sandbox for sandbox in sandboxes if sandbox.name.startswith(prefix)]
        for tag in request.params.get_list("tags"):
            key, _, value = tag.partition(":")
            sandboxes = [sandbox for sandbox in sandboxes if sandbox.tags.get(key) == value]
        sort_by = request.params.get("sortBy") or "createdAt"
        sort_key: Callable[[FakeSandbox], Any] = {
            "createdAt": lambda sandbox: sandbox.created_at,
            "name": lambda sandbox: sandbox.name,
            "statusUpdatedAt": lambda sandbox: sandbox.status_updated_at,
            "currentSnapshotId": lambda sandbox: sandbox.current_snapshot_id or "",
        }[sort_by]
        sandboxes.sort(key=sort_key, reverse=request.params.get("sortOrder", "desc") == "desc")
        page, pagination = self._page(request, sandboxes)
        return httpx.Response(
            200,
            json={
                "sandboxes": [sandbox.payload(self._session_of(sandbox)) for sandbox in page],
                "pagination": pagination,
            },
        )

    def _update_sandbox(self, request: RecordedRequest) -> httpx.Response:
        body = self._read_json(request, _UPDATE_FIELDS)
        sandbox = self._find_sandbox(self._project(request), request.path_params["name"])
        session = self._session_of(sandbox)
        if "region" in body or "failoverRegions" in body:
            region = body.get("region") or sandbox.region
            failover = body.get("failoverRegions", sandbox.failover_regions)
            if region in failover:
                _bad_request(f"failoverRegions must not include the sandbox region: {region}")
            sandbox.region, sandbox.failover_regions = region, list(failover)
        if "persistent" in body:
            sandbox.persistent = body["persistent"]
        if vcpus := (body.get("resources") or {}).get("vcpus"):
            sandbox.vcpus, sandbox.memory = vcpus, vcpus * 2048
        if "timeout" in body:
            sandbox.timeout_ms = body["timeout"]
        if "networkPolicy" in body:
            sandbox.network_policy = network_policy.session_readback(body["networkPolicy"])
        if "networkId" in body:
            sandbox.network_id = body["networkId"]
        if "env" in body:
            sandbox.env = dict(body["env"])
        if "tags" in body:
            sandbox.tags = dict(body["tags"])
        if "mounts" in body:
            sandbox.mounts = self._validated_mounts(sandbox.project_id, body["mounts"])
        if "snapshotExpiration" in body:
            sandbox.snapshot_expiration = body["snapshotExpiration"]
        if "keepLastSnapshots" in body:
            sandbox.keep_last_snapshots = body["keepLastSnapshots"]
        if "currentSnapshotId" in body:
            sandbox.current_snapshot_id = body["currentSnapshotId"]
        if "ports" in body:
            sandbox.ports = list(body["ports"])
        sandbox.updated_at = self._now()
        payload: wire.JSON = {"sandbox": sandbox.payload(session)}
        if "ports" in body:
            payload["routes"] = sandbox.routes()
        return httpx.Response(200, json=payload)

    def _delete_sandbox(self, request: RecordedRequest) -> httpx.Response:
        project_id = self._project(request)
        sandbox = self._find_sandbox(project_id, request.path_params["name"])
        session = self._session_of(sandbox)
        if session is not None and not session.gone:
            session.status, session.stopped_at = "stopped", self._now()
            self._unmount_drives(sandbox, session)
        payload = sandbox.payload(session)
        del self.sandboxes[(project_id, sandbox.name)]
        if request.params.get("deleteOrphanSnapshots") == "true":
            referenced = {
                snapshot_id
                for other in self.sandboxes.values()
                for snapshot_id in (other.current_snapshot_id, other.source_snapshot_id)
                if snapshot_id
            }
            for snapshot in self.snapshots.values():
                if snapshot.sandbox_name == sandbox.name and snapshot.id not in referenced:
                    snapshot.status, snapshot.updated_at = "deleted", self._now()
        return httpx.Response(200, json={"sandbox": payload})

    # ---- sessions ----------------------------------------------------------------

    def _get_session(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        sandbox = self.sandboxes.get((session.project_id, session.sandbox_name))
        routes = sandbox.routes() if sandbox is not None else []
        return httpx.Response(200, json={"session": session.payload(), "routes": routes})

    def _list_sessions(self, request: RecordedRequest) -> httpx.Response:
        project_id = self._project(request, "project")
        sessions = [
            session for session in self.sessions.values() if session.project_id == project_id
        ]
        if name := request.params.get("name"):
            sessions = [session for session in sessions if session.sandbox_name == name]
        sessions.sort(
            key=lambda session: session.requested_at,
            reverse=request.params.get("sortOrder", "desc") == "desc",
        )
        page, pagination = self._page(request, sessions)
        return httpx.Response(
            200,
            json={"sessions": [session.payload() for session in page], "pagination": pagination},
        )

    def _stop(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        if session.gone:
            return httpx.Response(200, json={"session": session.payload()})
        snapshot = self._stop_session(session)
        sandbox = self.sandboxes.get((session.project_id, session.sandbox_name))
        payload: wire.JSON = {"session": session.payload()}
        if sandbox is not None:
            payload["sandbox"] = sandbox.payload(self._session_of(sandbox))
        if snapshot is not None:
            payload["snapshot"] = snapshot.payload()
        return httpx.Response(200, json=payload)

    def _extend_timeout(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        self._assert_accepts_commands(session)
        session.timeout_ms += int((request.json() or {})["duration"])
        session.updated_at = self._now()
        return httpx.Response(200, json={"session": session.payload()})

    def _update_network_policy(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        self._assert_accepts_commands(session)
        readback = network_policy.session_readback(request.json() or {})
        session.network_policy = readback
        session.updated_at = self._now()
        sandbox = self.sandboxes.get((session.project_id, session.sandbox_name))
        if sandbox is not None:
            sandbox.network_policy = readback
        return httpx.Response(200, json={"session": session.payload()})

    def _create_snapshot(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        self._assert_accepts_commands(session)
        sandbox = self.sandboxes[(session.project_id, session.sandbox_name)]
        snapshot = self._snapshot(sandbox, session, method="manual")
        expiration = (request.json() or {}).get("expiration")
        if expiration is not None:
            snapshot.expires_at = snapshot.created_at + expiration if expiration else None
        session.status, session.stopped_at = "stopped", self._now()
        self._unmount_drives(sandbox, session)
        return httpx.Response(
            201, json={"snapshot": snapshot.payload(), "session": session.payload()}
        )

    def _open_interactive(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        self._assert_accepts_commands(session)
        token = self._id("tok")
        self.interactive_tickets[token] = session.id
        return httpx.Response(
            200,
            json={"url": f"wss://{session.id}.interactive.sandbox.test/ws", "token": token},
        )

    # ---- commands --------------------------------------------------------------

    def _find_command(self, request: RecordedRequest) -> tuple[FakeSession, FakeCommand]:
        session = self._find_session(request)
        command = session.commands.get(request.path_params["command_id"])
        if command is None:
            _not_found("Command not found.")
        return session, command

    def _run(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        body = request.json() or {}
        if body.get("logs") and not body.get("wait"):
            raise ApiError(
                400, "bad_request", "Invalid request: logs can only be streamed if `wait` is true."
            )
        self._assert_accepts_commands(session)
        name, args = body["command"], list(body.get("args") or [])
        cwd = FakeFilesystem.resolve(session.cwd, body["cwd"]) if body.get("cwd") else session.cwd
        try:
            result = self.programs.run(
                [name, *args],
                cwd=cwd,
                env={**session.env, **(body.get("env") or {})},
                fs=session.fs,
            )
        except ExecutableNotFound:
            raise ApiError(
                400, "executable_not_found", f'exec: "{name}": executable file not found in $PATH'
            ) from None
        command = FakeCommand(
            id=self._id("cmd"),
            session_id=session.id,
            name=name,
            args=args,
            cwd=cwd,
            started_at=self._now(),
            result=result,
            timeout_ms=body.get("timeout"),
        )
        session.commands[command.id] = command
        if not body.get("wait"):
            return httpx.Response(
                200, json={"command": command.payload(now=self._now()) | {"exitCode": None}}
            )
        records: list[wire.JSON] = [
            {"command": command.payload(now=self._now()) | {"exitCode": None}}
        ]
        self.clock.advance_to(command.end)
        if body.get("logs"):
            records.extend(wire.log_record(stream, data) for stream, data in command.output())
        records.append({"command": command.payload(now=self._now(), finished=True)})
        return _ndjson(records)

    def _list_commands(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        return httpx.Response(
            200,
            json={"commands": [c.payload(now=self._now()) for c in session.commands.values()]},
        )

    def _get_command(self, request: RecordedRequest) -> httpx.Response:
        _session, command = self._find_command(request)
        if request.params.get("wait") == "true":
            self.clock.advance_to(command.end)
        return httpx.Response(200, json={"command": command.payload(now=self._now())})

    def _kill(self, request: RecordedRequest) -> httpx.Response:
        session, command = self._find_command(request)
        self._assert_accepts_commands(session)
        if self._now() < command.end:
            command.killed_at = self._now()
            command.kill_signal = int((request.json() or {}).get("signal", 15))
        return httpx.Response(200, json={"command": command.payload(now=self._now())})

    def _logs(self, request: RecordedRequest) -> httpx.Response:
        session, command = self._find_command(request)
        self._assert_accepts_commands(session)
        self.clock.advance_to(command.end)
        return _ndjson([wire.log_record(stream, data) for stream, data in command.output()])

    # ---- filesystem ---------------------------------------------------------------

    def _read_file(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        self._assert_accepts_commands(session)
        body = request.json() or {}
        path = FakeFilesystem.resolve(body.get("cwd") or session.cwd, body["path"])
        try:
            data = session.fs.read(path)
        except FilesystemError as error:
            if error.code == "not_found":
                _not_found("File not found.")
            raise ApiError(400, "file_error", f"{path} is a directory") from None
        return httpx.Response(
            200, content=data, headers={"content-type": "application/octet-stream"}
        )

    def _write_files(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        self._assert_accepts_commands(session)
        if "application/gzip" not in request.headers.get("content-type", ""):
            raise ApiError(
                400, "InvalidContentType", "Invalid content type. Expected application/gzip"
            )
        root = request.headers.get("x-cwd") or session.cwd
        archive = gzip.decompress(request.content) if request.content else b""
        if not archive:
            return httpx.Response(200, json={})
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
            for member in tar.getmembers():
                path = FakeFilesystem.resolve(root, member.name)
                if member.isdir():
                    session.fs.mkdir(path, parents=True, exist_ok=True)
                    continue
                extracted = tar.extractfile(member)
                data = b"" if extracted is None else extracted.read()
                try:
                    session.fs.write(path, data, mode=member.mode)
                except FilesystemError:
                    raise ApiError(
                        400, "write_file_failed", f"open {path}: is a directory"
                    ) from None
        return httpx.Response(200, json={})

    def _mkdir(self, request: RecordedRequest) -> httpx.Response:
        session = self._find_session(request)
        self._assert_accepts_commands(session)
        body = request.json() or {}
        path = FakeFilesystem.resolve(body.get("cwd") or session.cwd, body["path"])
        recursive = body.get("recursive", False)
        try:
            session.fs.mkdir(path, parents=recursive, exist_ok=recursive)
        except FilesystemError as error:
            raise ApiError(
                400, "file_error", f"unable to create path {path}: {error.code}"
            ) from None
        return httpx.Response(200, json={})

    # ---- snapshots and drives -------------------------------------------------------

    def _find_snapshot(self, request: RecordedRequest) -> FakeSnapshot:
        snapshot = self.snapshots.get(request.path_params["snapshot_id"])
        if snapshot is None:
            _not_found("Snapshot not found.")
        return snapshot

    def _get_snapshot(self, request: RecordedRequest) -> httpx.Response:
        return httpx.Response(200, json={"snapshot": self._find_snapshot(request).payload()})

    def _delete_snapshot(self, request: RecordedRequest) -> httpx.Response:
        snapshot = self._find_snapshot(request)
        snapshot.status, snapshot.updated_at = "deleted", self._now()
        return httpx.Response(200, json={"snapshot": snapshot.payload()})

    def _list_snapshots(self, request: RecordedRequest) -> httpx.Response:
        project_id = self._project(request, "project")
        names = {name for (project, name) in self.sandboxes if project == project_id}
        snapshots = list(self.snapshots.values())
        if name := request.params.get("name"):
            snapshots = [snapshot for snapshot in snapshots if snapshot.sandbox_name == name]
        else:
            snapshots = [snapshot for snapshot in snapshots if snapshot.sandbox_name in names]
        snapshots.sort(
            key=lambda snapshot: snapshot.created_at,
            reverse=request.params.get("sortOrder", "desc") == "desc",
        )
        page, pagination = self._page(request, snapshots)
        return httpx.Response(
            200,
            json={"snapshots": [snapshot.payload() for snapshot in page], "pagination": pagination},
        )

    def _find_drive(self, request: RecordedRequest) -> FakeDrive:
        project_id = self._project(request)
        key = request.path_params["name"]
        drive = self.drives.get((project_id, key)) or next(
            (d for (project, _), d in self.drives.items() if project == project_id and d.id == key),
            None,
        )
        if drive is None:
            _not_found("Drive not found.")
        return drive

    def _get_drive(self, request: RecordedRequest) -> httpx.Response:
        return httpx.Response(200, json={"drive": self._find_drive(request).payload()})

    def _get_or_create_drive(self, request: RecordedRequest) -> httpx.Response:
        body = request.json() or {}
        project_id = self._canonical(body.get("projectId"))
        name = request.path_params["name"]
        size, region = body.get("maxSizeBytes"), body.get("region")
        drive = self.drives.get((project_id, name))
        created = drive is None
        if drive is None:
            if size is not None and size % 4096:
                _bad_request("The maxSizeBytes value must be a multiple of 4096 bytes.")
            drive = FakeDrive(
                id=self._id("drive"),
                name=name,
                project_id=project_id,
                region=region or "iad1",
                max_size_bytes=size or 100 * 1024**3,
                created_at=self._now(),
            )
            self.drives[(project_id, name)] = drive
        if size is not None and drive.max_size_bytes != size:
            raise ApiError(
                409, "conflict", f'Drive "{name}" already exists with a different maxSizeBytes.'
            )
        if region is not None and drive.region != region:
            raise ApiError(409, "conflict", f'Drive "{name}" already exists in a different region.')
        return httpx.Response(201 if created else 200, json={"drive": drive.payload()})

    def _delete_drive(self, request: RecordedRequest) -> httpx.Response:
        drive = self._find_drive(request)
        session = self.sessions.get(drive.current_session_id or "")
        if session is not None and not session.gone:
            raise ApiError(
                409,
                "drive_attached",
                "Cannot delete a drive that is currently attached to a sandbox.",
            )
        del self.drives[(drive.project_id, drive.name)]
        return httpx.Response(200, json={"drive": drive.payload()})

    def _list_drives(self, request: RecordedRequest) -> httpx.Response:
        project_id = self._project(request)
        drives = [drive for (project, _), drive in self.drives.items() if project == project_id]
        if prefix := request.params.get("namePrefix"):
            drives = [drive for drive in drives if drive.name.startswith(prefix)]
        sort_by = request.params.get("sortBy") or "createdAt"
        sort_key: Callable[[FakeDrive], Any] = {
            "createdAt": lambda drive: drive.created_at,
            "updatedAt": lambda drive: drive.updated_at or drive.created_at,
            "name": lambda drive: drive.name,
        }[sort_by]
        drives.sort(key=sort_key, reverse=request.params.get("sortOrder", "desc") == "desc")
        page, pagination = self._page(request, drives)
        return httpx.Response(
            200, json={"drives": [drive.payload() for drive in page], "pagination": pagination}
        )

    @staticmethod
    def _page(request: RecordedRequest, items: list[Any]) -> tuple[list[Any], wire.JSON]:
        start = int(request.params.get("cursor") or 0)
        limit = int(request.params.get("limit") or 50)
        page = items[start : start + limit]
        more = start + limit < len(items)
        return page, {"count": len(page), "next": str(start + limit) if more else None}

    def iter_sessions(self, name: str) -> Iterator[FakeSession]:
        return (session for session in self.sessions.values() if session.sandbox_name == name)


_HANDLERS: dict[str, _Handler] = {
    "sandboxes.create": FakeSandboxApi._create_sandbox,
    "sandboxes.list": FakeSandboxApi._list_sandboxes,
    "sandboxes.get": FakeSandboxApi._get_sandbox,
    "sandboxes.update": FakeSandboxApi._update_sandbox,
    "sandboxes.delete": FakeSandboxApi._delete_sandbox,
    "sandboxes.fork": FakeSandboxApi._fork_sandbox,
    "sessions.list": FakeSandboxApi._list_sessions,
    "sessions.get": FakeSandboxApi._get_session,
    "sessions.stop": FakeSandboxApi._stop,
    "sessions.extend_timeout": FakeSandboxApi._extend_timeout,
    "sessions.network_policy": FakeSandboxApi._update_network_policy,
    "sessions.snapshot": FakeSandboxApi._create_snapshot,
    "sessions.interactive": FakeSandboxApi._open_interactive,
    "commands.create": FakeSandboxApi._run,
    "commands.list": FakeSandboxApi._list_commands,
    "commands.get": FakeSandboxApi._get_command,
    "commands.kill": FakeSandboxApi._kill,
    "commands.logs": FakeSandboxApi._logs,
    "fs.read": FakeSandboxApi._read_file,
    "fs.write": FakeSandboxApi._write_files,
    "fs.mkdir": FakeSandboxApi._mkdir,
    "snapshots.list": FakeSandboxApi._list_snapshots,
    "snapshots.get": FakeSandboxApi._get_snapshot,
    "snapshots.delete": FakeSandboxApi._delete_snapshot,
    "drives.list": FakeSandboxApi._list_drives,
    "drives.get": FakeSandboxApi._get_drive,
    "drives.get_or_create": FakeSandboxApi._get_or_create_drive,
    "drives.delete": FakeSandboxApi._delete_drive,
}


__all__ = ["BASE_URL", "FakeSandboxApi", "Gate", "RecordedRequest"]
