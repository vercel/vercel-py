"""Runtime binding clients for unstable Sandbox handles."""

from collections.abc import AsyncIterator, Iterator
from typing import Any

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.sandbox.errors import SandboxTerminalStateError
from vercel._internal.unstable.sandbox.handles import (
    Sandbox,
    SandboxCommand,
    SandboxRuntimeSession,
    Snapshot,
    SyncSandbox,
    SyncSandboxCommand,
    SyncSandboxRuntimeSession,
    SyncSnapshot,
)
from vercel._internal.unstable.sandbox.log_stream import _parse_command_log_record
from vercel._internal.unstable.sandbox.models import SandboxCommandLog
from vercel._internal.unstable.sandbox.pagination import (
    QuerySandboxesPage,
    QuerySandboxesParams,
    QuerySessionsPage,
    QuerySessionsParams,
    QuerySnapshotsPage,
    QuerySnapshotsParams,
)
from vercel._internal.unstable.sandbox.service import SandboxService, _SandboxTerminalState
from vercel._internal.unstable.sandbox.state import (
    SandboxCommandState,
    SandboxRuntimeSessionState,
    SandboxState,
    SnapshotState,
)


def _terminal_error(error: _SandboxTerminalState, sandbox: object) -> SandboxTerminalStateError:
    return SandboxTerminalStateError(
        f"Sandbox {error.sandbox.name!r} reached terminal state {error.status!r}",
        status=error.status,
        sandbox=sandbox,
    )


class AsyncSandboxClient:
    """Bind neutral service results to async public handles."""

    def __init__(self, service: SandboxService) -> None:
        self._service = service

    @property
    def service(self) -> SandboxService:
        return self._service

    @property
    def api_client(self) -> object:
        return self._service.api_client

    def bind_sandbox(self, state: SandboxState) -> Sandbox:
        return Sandbox(payload=state, client=self)

    def bind_runtime_session(self, state: SandboxRuntimeSessionState) -> SandboxRuntimeSession:
        return SandboxRuntimeSession(payload=state, client=self)

    def bind_command(self, state: SandboxCommandState) -> SandboxCommand:
        return SandboxCommand(payload=state, client=self)

    def bind_snapshot(self, state: SnapshotState) -> Snapshot:
        return Snapshot(payload=state, client=self)

    async def create_sandbox(self, **kwargs: Any) -> Sandbox:
        try:
            return self.bind_sandbox(await self._service.create_sandbox(**kwargs))
        except _SandboxTerminalState as error:
            raise _terminal_error(error, self.bind_sandbox(error.sandbox)) from error

    async def get_sandbox(self, **kwargs: Any) -> Sandbox:
        return self.bind_sandbox(await self._service.get_sandbox(**kwargs))

    async def query_sandboxes_page(self, **kwargs: Any) -> QuerySandboxesPage[Sandbox]:
        page = await self._service.query_sandboxes_page(**kwargs)
        return QuerySandboxesPage(
            sandboxes=[self.bind_sandbox(state) for state in page.sandboxes],
            next_cursor=page.next_cursor,
        )

    def query_sandboxes(self, **kwargs: Any) -> AsyncIterator[Sandbox]:
        async def iterate() -> AsyncIterator[Sandbox]:
            params = QuerySandboxesParams(
                page_size=kwargs.get("page_size"), cursor=kwargs.get("cursor")
            )
            while True:
                page = await self.query_sandboxes_page(
                    query=kwargs.get("query"),
                    project_id=kwargs.get("project_id"),
                    page_size=params.page_size,
                    cursor=params.cursor,
                )
                for sandbox in page.sandboxes:
                    yield sandbox
                if page.next_cursor is None or not page.sandboxes:
                    return
                params = params.with_cursor(page.next_cursor)

        return iterate()

    async def destroy_sandbox_payload(self, **kwargs: Any) -> SandboxState:
        return await self._service.destroy_sandbox(**kwargs)

    async def update_sandbox_payload(self, **kwargs: Any) -> SandboxState:
        return await self._service.update_sandbox(**kwargs)

    async def create_runtime_session(self, **kwargs: Any) -> SandboxRuntimeSession:
        return self.bind_runtime_session(await self._service.create_runtime_session(**kwargs))

    async def stop_runtime_session_payload(self, **kwargs: Any) -> SandboxRuntimeSessionState:
        return await self._service.stop_runtime_session(**kwargs)

    async def get_runtime_session_payload(self, **kwargs: Any) -> SandboxRuntimeSessionState:
        return await self._service.get_runtime_session(**kwargs)

    async def query_sessions_page(self, **kwargs: Any) -> QuerySessionsPage[SandboxRuntimeSession]:
        page = await self._service.query_sessions_page(**kwargs)
        return QuerySessionsPage(
            sessions=[self.bind_runtime_session(state) for state in page.sessions],
            next_cursor=page.next_cursor,
        )

    def query_sessions(self, **kwargs: Any) -> AsyncIterator[SandboxRuntimeSession]:
        async def iterate() -> AsyncIterator[SandboxRuntimeSession]:
            params = QuerySessionsParams(
                page_size=kwargs.get("page_size"), cursor=kwargs.get("cursor")
            )
            while True:
                page = await self.query_sessions_page(
                    project_id=kwargs.get("project_id"),
                    name=kwargs.get("name"),
                    page_size=params.page_size,
                    cursor=params.cursor,
                    sort_order=kwargs.get("sort_order"),
                )
                for session in page.sessions:
                    yield session
                if page.next_cursor is None or not page.sessions:
                    return
                params = params.with_cursor(page.next_cursor)

        return iterate()

    async def extend_runtime_session_timeout_payload(
        self, **kwargs: Any
    ) -> SandboxRuntimeSessionState:
        return await self._service.extend_runtime_session_timeout(**kwargs)

    async def update_runtime_session_network_policy_payload(
        self, **kwargs: Any
    ) -> SandboxRuntimeSessionState:
        return await self._service.update_runtime_session_network_policy(**kwargs)

    async def create_snapshot_for_session(
        self, **kwargs: Any
    ) -> tuple[Snapshot, SandboxRuntimeSessionState]:
        result = await self._service.create_snapshot(**kwargs)
        return self.bind_snapshot(result.snapshot), result.session

    async def query_snapshots_page(self, **kwargs: Any) -> QuerySnapshotsPage[Snapshot]:
        page = await self._service.query_snapshots_page(**kwargs)
        return QuerySnapshotsPage(
            snapshots=[self.bind_snapshot(state) for state in page.snapshots],
            next_cursor=page.next_cursor,
        )

    def query_snapshots(self, **kwargs: Any) -> AsyncIterator[Snapshot]:
        async def iterate() -> AsyncIterator[Snapshot]:
            params = QuerySnapshotsParams(
                page_size=kwargs.get("page_size"), cursor=kwargs.get("cursor")
            )
            while True:
                page = await self.query_snapshots_page(
                    project_id=kwargs.get("project_id"),
                    name=kwargs.get("name"),
                    page_size=params.page_size,
                    cursor=params.cursor,
                    sort_order=kwargs.get("sort_order"),
                )
                for snapshot in page.snapshots:
                    yield snapshot
                if page.next_cursor is None or not page.snapshots:
                    return
                params = params.with_cursor(page.next_cursor)

        return iterate()

    async def get_snapshot(self, **kwargs: Any) -> Snapshot:
        return self.bind_snapshot(await self._service.get_snapshot(**kwargs))

    async def delete_snapshot_payload(self, **kwargs: Any) -> SnapshotState:
        return await self._service.delete_snapshot(**kwargs)

    async def run_command(self, **kwargs: Any) -> SandboxCommand:
        return self.bind_command(await self._service.run_command(**kwargs))

    async def start_command(self, **kwargs: Any) -> SandboxCommand:
        return self.bind_command(await self._service.start_command(**kwargs))

    async def get_command(self, **kwargs: Any) -> SandboxCommand:
        return self.bind_command(await self._service.get_command(**kwargs))

    async def get_command_payload(self, **kwargs: Any) -> SandboxCommandState:
        return await self._service.get_command(**kwargs)

    async def query_commands(self, **kwargs: Any) -> list[SandboxCommand]:
        return [self.bind_command(state) for state in await self._service.query_commands(**kwargs)]

    async def kill_command_payload(self, **kwargs: Any) -> SandboxCommandState:
        return await self._service.kill_command(**kwargs)

    async def mkdir(self, **kwargs: Any) -> None:
        await self._service.mkdir(**kwargs)

    async def read_file(self, **kwargs: Any) -> bytes:
        return await self._service.read_file(**kwargs)

    async def write_files(self, **kwargs: Any) -> None:
        await self._service.write_files(**kwargs)

    def command_logs(self, **kwargs: Any) -> AsyncIterator[SandboxCommandLog]:
        async def iterate() -> AsyncIterator[SandboxCommandLog]:
            response = await self._service.command_logs_response(**kwargs)
            try:
                async for line in response.aiter_lines():
                    if line:
                        event = _parse_command_log_record(line)
                        if event is not None:
                            yield event
            finally:
                await response.aclose()

        return iterate()


class SyncSandboxClient:
    """Bind neutral service results to sync public handles."""

    def __init__(self, service: SandboxService) -> None:
        self._service = service

    @property
    def service(self) -> SandboxService:
        return self._service

    @property
    def api_client(self) -> object:
        return self._service.api_client

    def bind_sandbox(self, state: SandboxState) -> SyncSandbox:
        return SyncSandbox(payload=state, client=self)

    def bind_runtime_session(self, state: SandboxRuntimeSessionState) -> SyncSandboxRuntimeSession:
        return SyncSandboxRuntimeSession(payload=state, client=self)

    def bind_command(self, state: SandboxCommandState) -> SyncSandboxCommand:
        return SyncSandboxCommand(payload=state, client=self)

    def bind_snapshot(self, state: SnapshotState) -> SyncSnapshot:
        return SyncSnapshot(payload=state, client=self)

    def create_sandbox(self, **kwargs: Any) -> SyncSandbox:
        try:
            return self.bind_sandbox(iter_coroutine(self._service.create_sandbox(**kwargs)))
        except _SandboxTerminalState as error:
            raise _terminal_error(error, self.bind_sandbox(error.sandbox)) from error

    def get_sandbox(self, **kwargs: Any) -> SyncSandbox:
        return self.bind_sandbox(iter_coroutine(self._service.get_sandbox(**kwargs)))

    def query_sandboxes_page(self, **kwargs: Any) -> QuerySandboxesPage[SyncSandbox]:
        page = iter_coroutine(self._service.query_sandboxes_page(**kwargs))
        return QuerySandboxesPage(
            sandboxes=[self.bind_sandbox(state) for state in page.sandboxes],
            next_cursor=page.next_cursor,
        )

    def query_sandboxes(self, **kwargs: Any) -> Iterator[SyncSandbox]:
        params = QuerySandboxesParams(
            page_size=kwargs.get("page_size"), cursor=kwargs.get("cursor")
        )
        while True:
            page = self.query_sandboxes_page(
                query=kwargs.get("query"),
                project_id=kwargs.get("project_id"),
                page_size=params.page_size,
                cursor=params.cursor,
            )
            yield from page.sandboxes
            if page.next_cursor is None or not page.sandboxes:
                return
            params = params.with_cursor(page.next_cursor)

    def destroy_sandbox_payload(self, **kwargs: Any) -> SandboxState:
        return iter_coroutine(self._service.destroy_sandbox(**kwargs))

    def update_sandbox_payload(self, **kwargs: Any) -> SandboxState:
        return iter_coroutine(self._service.update_sandbox(**kwargs))

    def create_runtime_session(self, **kwargs: Any) -> SyncSandboxRuntimeSession:
        return self.bind_runtime_session(
            iter_coroutine(self._service.create_runtime_session(**kwargs))
        )

    def stop_runtime_session_payload(self, **kwargs: Any) -> SandboxRuntimeSessionState:
        return iter_coroutine(self._service.stop_runtime_session(**kwargs))

    def get_runtime_session_payload(self, **kwargs: Any) -> SandboxRuntimeSessionState:
        return iter_coroutine(self._service.get_runtime_session(**kwargs))

    def query_sessions_page(self, **kwargs: Any) -> QuerySessionsPage[SyncSandboxRuntimeSession]:
        page = iter_coroutine(self._service.query_sessions_page(**kwargs))
        return QuerySessionsPage(
            sessions=[self.bind_runtime_session(state) for state in page.sessions],
            next_cursor=page.next_cursor,
        )

    def query_sessions(self, **kwargs: Any) -> Iterator[SyncSandboxRuntimeSession]:
        params = QuerySessionsParams(page_size=kwargs.get("page_size"), cursor=kwargs.get("cursor"))
        while True:
            page = self.query_sessions_page(
                project_id=kwargs.get("project_id"),
                name=kwargs.get("name"),
                page_size=params.page_size,
                cursor=params.cursor,
                sort_order=kwargs.get("sort_order"),
            )
            yield from page.sessions
            if page.next_cursor is None or not page.sessions:
                return
            params = params.with_cursor(page.next_cursor)

    def extend_runtime_session_timeout_payload(self, **kwargs: Any) -> SandboxRuntimeSessionState:
        return iter_coroutine(self._service.extend_runtime_session_timeout(**kwargs))

    def update_runtime_session_network_policy_payload(
        self, **kwargs: Any
    ) -> SandboxRuntimeSessionState:
        return iter_coroutine(self._service.update_runtime_session_network_policy(**kwargs))

    def create_snapshot_for_session(
        self, **kwargs: Any
    ) -> tuple[SyncSnapshot, SandboxRuntimeSessionState]:
        result = iter_coroutine(self._service.create_snapshot(**kwargs))
        return self.bind_snapshot(result.snapshot), result.session

    def query_snapshots_page(self, **kwargs: Any) -> QuerySnapshotsPage[SyncSnapshot]:
        page = iter_coroutine(self._service.query_snapshots_page(**kwargs))
        return QuerySnapshotsPage(
            snapshots=[self.bind_snapshot(state) for state in page.snapshots],
            next_cursor=page.next_cursor,
        )

    def query_snapshots(self, **kwargs: Any) -> Iterator[SyncSnapshot]:
        params = QuerySnapshotsParams(
            page_size=kwargs.get("page_size"), cursor=kwargs.get("cursor")
        )
        while True:
            page = self.query_snapshots_page(
                project_id=kwargs.get("project_id"),
                name=kwargs.get("name"),
                page_size=params.page_size,
                cursor=params.cursor,
                sort_order=kwargs.get("sort_order"),
            )
            yield from page.snapshots
            if page.next_cursor is None or not page.snapshots:
                return
            params = params.with_cursor(page.next_cursor)

    def get_snapshot(self, **kwargs: Any) -> SyncSnapshot:
        return self.bind_snapshot(iter_coroutine(self._service.get_snapshot(**kwargs)))

    def delete_snapshot_payload(self, **kwargs: Any) -> SnapshotState:
        return iter_coroutine(self._service.delete_snapshot(**kwargs))

    def run_command(self, **kwargs: Any) -> SyncSandboxCommand:
        return self.bind_command(iter_coroutine(self._service.run_command(**kwargs)))

    def start_command(self, **kwargs: Any) -> SyncSandboxCommand:
        return self.bind_command(iter_coroutine(self._service.start_command(**kwargs)))

    def get_command(self, **kwargs: Any) -> SyncSandboxCommand:
        return self.bind_command(iter_coroutine(self._service.get_command(**kwargs)))

    def get_command_payload(self, **kwargs: Any) -> SandboxCommandState:
        return iter_coroutine(self._service.get_command(**kwargs))

    def query_commands(self, **kwargs: Any) -> list[SyncSandboxCommand]:
        return [
            self.bind_command(state)
            for state in iter_coroutine(self._service.query_commands(**kwargs))
        ]

    def kill_command_payload(self, **kwargs: Any) -> SandboxCommandState:
        return iter_coroutine(self._service.kill_command(**kwargs))

    def mkdir(self, **kwargs: Any) -> None:
        iter_coroutine(self._service.mkdir(**kwargs))

    def read_file(self, **kwargs: Any) -> bytes:
        return iter_coroutine(self._service.read_file(**kwargs))

    def write_files(self, **kwargs: Any) -> None:
        iter_coroutine(self._service.write_files(**kwargs))

    def command_logs(self, **kwargs: Any) -> Iterator[SandboxCommandLog]:
        response = iter_coroutine(self._service.command_logs_response(**kwargs))
        try:
            for line in response.iter_lines():
                if line:
                    event = _parse_command_log_record(line)
                    if event is not None:
                        yield event
        finally:
            response.close()
