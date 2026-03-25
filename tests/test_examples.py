from __future__ import annotations

import asyncio
import importlib.util
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

import pytest

# Required on CI, optional locally
_is_ci = bool(os.getenv("CI"))
_has_credentials = bool(
    os.getenv("BLOB_READ_WRITE_TOKEN")
    and os.getenv("VERCEL_TOKEN")
    and os.getenv("VERCEL_PROJECT_ID")
    and os.getenv("VERCEL_TEAM_ID")
)

_examples_dir = Path(__file__).resolve().parents[1] / "examples"
_example_files = (
    sorted([p for p in _examples_dir.iterdir() if p.is_file() and p.suffix == ".py"])
    if _examples_dir.is_dir()
    else []
)


@dataclass
class _ListedSnapshot:
    id: str
    created_at: int
    expires_at: int | None


@dataclass
class _CreatedSnapshot:
    snapshot_id: str
    status: str
    created_at: int
    expires_at: int | None


class _WriteFile(TypedDict):
    path: str
    content: bytes


class _NoopAsyncClient:
    async def aclose(self) -> None:
        return None


class _NoopSyncClient:
    def close(self) -> None:
        return None


def _load_example_module(module_name: str, script_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _build_snapshot_example_fakes():
    state = {
        "async_records": [],
        "sync_records": [],
        "async_snapshot_calls": [],
        "sync_snapshot_calls": [],
        "async_list_calls": [],
        "sync_list_calls": [],
        "async_pager_awaits": 0,
        "async_page_iterations": 0,
        "async_item_iterations": 0,
        "sync_page_iterations": 0,
        "sync_item_iterations": 0,
        "async_page_ids": [],
        "async_item_ids": [],
        "sync_page_ids": [],
        "sync_item_ids": [],
        "async_deleted_ids": [],
        "sync_deleted_ids": [],
        "async_sandbox_index": 0,
        "sync_sandbox_index": 0,
        "async_snapshot_index": 0,
        "sync_snapshot_index": 0,
    }

    def _created_at(index: int) -> int:
        return 1_705_320_600_000 + index

    def _expires_at(created_at: int, expiration: int | None) -> int | None:
        if expiration in (None, 0):
            return None
        assert expiration is not None
        return created_at + expiration

    def _make_async_listed_snapshots() -> list[_ListedSnapshot]:
        return [
            _ListedSnapshot(
                id=record["id"],
                created_at=record["created_at"],
                expires_at=record["expires_at"],
            )
            for record in state["async_records"]
        ]

    def _make_sync_listed_snapshots() -> list[_ListedSnapshot]:
        return [
            _ListedSnapshot(
                id=record["id"],
                created_at=record["created_at"],
                expires_at=record["expires_at"],
            )
            for record in state["sync_records"]
        ]

    class FakeAsyncSnapshotPage:
        def __init__(self, snapshots: list[_ListedSnapshot]) -> None:
            self.snapshots = snapshots

        async def iter_pages(self):
            state["async_page_iterations"] += 1
            state["async_page_ids"].append([snapshot.id for snapshot in self.snapshots])
            yield self

        async def iter_items(self):
            state["async_item_iterations"] += 1
            for snapshot in self.snapshots:
                state["async_item_ids"].append(snapshot.id)
                yield snapshot

    class FakeAsyncSnapshotPager:
        def __init__(self, snapshots: list[_ListedSnapshot]) -> None:
            self._page = FakeAsyncSnapshotPage(snapshots)

        async def _get_first_page(self) -> FakeAsyncSnapshotPage:
            state["async_pager_awaits"] += 1
            return self._page

        def __await__(self):
            return self._get_first_page().__await__()

        def __aiter__(self):
            return self.iter_items()

        async def iter_pages(self):
            state["async_page_iterations"] += 1
            state["async_page_ids"].append([snapshot.id for snapshot in self._page.snapshots])
            yield self._page

        async def iter_items(self):
            state["async_item_iterations"] += 1
            for snapshot in self._page.snapshots:
                state["async_item_ids"].append(snapshot.id)
                yield snapshot

    class FakeSyncSnapshotPage:
        def __init__(self, snapshots: list[_ListedSnapshot]) -> None:
            self.snapshots = snapshots

        def iter_pages(self):
            state["sync_page_iterations"] += 1
            state["sync_page_ids"].append([snapshot.id for snapshot in self.snapshots])
            yield self

        def iter_items(self):
            state["sync_item_iterations"] += 1
            for snapshot in self.snapshots:
                state["sync_item_ids"].append(snapshot.id)
                yield snapshot

    @dataclass
    class FakeAsyncSnapshotHandle(_CreatedSnapshot):
        client: _NoopAsyncClient

        async def delete(self) -> None:
            state["async_deleted_ids"].append(self.snapshot_id)
            self.status = "deleted"

    @dataclass
    class FakeSyncSnapshotHandle(_CreatedSnapshot):
        client: _NoopSyncClient

        def delete(self) -> None:
            state["sync_deleted_ids"].append(self.snapshot_id)
            self.status = "deleted"

    class FakeAsyncSandbox:
        def __init__(
            self,
            sandbox_id: str,
            *,
            files: dict[str, bytes] | None = None,
            source_snapshot_id: str | None = None,
        ) -> None:
            self.sandbox_id = sandbox_id
            self.source_snapshot_id = source_snapshot_id
            self.status = "running"
            self.files = dict(files or {})
            self.client = _NoopAsyncClient()

        @classmethod
        async def create(
            cls,
            *,
            timeout: int,
            source: dict[str, str] | None = None,
        ) -> FakeAsyncSandbox:
            del timeout
            state["async_sandbox_index"] += 1
            sandbox_id = f"async-sandbox-{state['async_sandbox_index']}"
            files: dict[str, bytes] | None = None
            source_snapshot_id = None
            if source is not None:
                source_snapshot_id = source["snapshot_id"]
                source_record = next(
                    record
                    for record in state["async_records"]
                    if record["id"] == source_snapshot_id
                )
                files = dict(source_record["files"])
            return cls(sandbox_id, files=files, source_snapshot_id=source_snapshot_id)

        async def write_files(self, files: list[_WriteFile]) -> None:
            for file in files:
                self.files[file["path"]] = file["content"]

        async def read_file(self, path: str) -> bytes:
            return self.files[path]

        async def snapshot(self, *, expiration: int | None = None) -> _CreatedSnapshot:
            state["async_snapshot_calls"].append(expiration)
            state["async_snapshot_index"] += 1
            created_at = _created_at(state["async_snapshot_index"])
            snapshot_id = f"async-snapshot-{state['async_snapshot_index']}"
            expires_at = _expires_at(created_at, expiration)
            state["async_records"].append(
                {
                    "id": snapshot_id,
                    "created_at": created_at,
                    "expires_at": expires_at,
                    "files": dict(self.files),
                }
            )
            self.status = "stopped"
            return _CreatedSnapshot(
                snapshot_id=snapshot_id,
                status="created",
                created_at=created_at,
                expires_at=expires_at,
            )

    class FakeSyncSandbox:
        def __init__(
            self,
            sandbox_id: str,
            *,
            files: dict[str, bytes] | None = None,
            source_snapshot_id: str | None = None,
        ) -> None:
            self.sandbox_id = sandbox_id
            self.source_snapshot_id = source_snapshot_id
            self.status = "running"
            self.files = dict(files or {})
            self.client = _NoopSyncClient()

        @classmethod
        def create(
            cls,
            *,
            timeout: int,
            source: dict[str, str] | None = None,
        ) -> FakeSyncSandbox:
            del timeout
            state["sync_sandbox_index"] += 1
            sandbox_id = f"sync-sandbox-{state['sync_sandbox_index']}"
            files: dict[str, bytes] | None = None
            source_snapshot_id = None
            if source is not None:
                source_snapshot_id = source["snapshot_id"]
                source_record = next(
                    record for record in state["sync_records"] if record["id"] == source_snapshot_id
                )
                files = dict(source_record["files"])
            return cls(sandbox_id, files=files, source_snapshot_id=source_snapshot_id)

        def write_files(self, files: list[_WriteFile]) -> None:
            for file in files:
                self.files[file["path"]] = file["content"]

        def read_file(self, path: str) -> bytes:
            return self.files[path]

        def snapshot(self, *, expiration: int | None = None) -> _CreatedSnapshot:
            state["sync_snapshot_calls"].append(expiration)
            state["sync_snapshot_index"] += 1
            created_at = _created_at(state["sync_snapshot_index"] + 10)
            snapshot_id = f"sync-snapshot-{state['sync_snapshot_index']}"
            expires_at = _expires_at(created_at, expiration)
            state["sync_records"].append(
                {
                    "id": snapshot_id,
                    "created_at": created_at,
                    "expires_at": expires_at,
                    "files": dict(self.files),
                }
            )
            self.status = "stopped"
            return _CreatedSnapshot(
                snapshot_id=snapshot_id,
                status="created",
                created_at=created_at,
                expires_at=expires_at,
            )

    class FakeAsyncSnapshot:
        @staticmethod
        async def get(*, snapshot_id: str) -> FakeAsyncSnapshotHandle:
            record = next(
                record for record in state["async_records"] if record["id"] == snapshot_id
            )
            return FakeAsyncSnapshotHandle(
                snapshot_id=snapshot_id,
                status="created",
                created_at=record["created_at"],
                expires_at=record["expires_at"],
                client=_NoopAsyncClient(),
            )

        @staticmethod
        def list(*, limit: int | None = None, since: int | None = None):
            state["async_list_calls"].append({"limit": limit, "since": since})
            return FakeAsyncSnapshotPager(_make_async_listed_snapshots())

    class FakeSnapshot:
        @staticmethod
        def get(*, snapshot_id: str) -> FakeSyncSnapshotHandle:
            record = next(record for record in state["sync_records"] if record["id"] == snapshot_id)
            return FakeSyncSnapshotHandle(
                snapshot_id=snapshot_id,
                status="created",
                created_at=record["created_at"],
                expires_at=record["expires_at"],
                client=_NoopSyncClient(),
            )

        @staticmethod
        def list(*, limit: int | None = None, since: int | None = None):
            state["sync_list_calls"].append({"limit": limit, "since": since})
            return FakeSyncSnapshotPage(_make_sync_listed_snapshots())

    return state, FakeAsyncSandbox, FakeAsyncSnapshot, FakeSyncSandbox, FakeSnapshot


def test_snapshot_example_uses_snapshot_listing_and_expiration(monkeypatch: pytest.MonkeyPatch):
    script_path = _examples_dir / "sandbox_11_snapshots.py"
    monkeypatch.setenv("VERCEL_PROJECT_ID", "prj_example_test")
    module = _load_example_module("sandbox_11_snapshots_test_module", script_path)
    state, fake_async_sandbox, fake_async_snapshot, fake_sync_sandbox, fake_snapshot = (
        _build_snapshot_example_fakes()
    )

    monkeypatch.setattr(module, "AsyncSandbox", fake_async_sandbox)
    monkeypatch.setattr(module, "AsyncSnapshot", fake_async_snapshot)
    monkeypatch.setattr(module, "Sandbox", fake_sync_sandbox)
    monkeypatch.setattr(module, "Snapshot", fake_snapshot)

    asyncio.run(module.async_demo())
    module.sync_demo()

    assert state["async_snapshot_calls"] == [86_400_000, 0]
    assert state["sync_snapshot_calls"] == [86_400_000, 0]
    assert state["async_records"][0]["expires_at"] == state["async_records"][0]["created_at"] + (
        86_400_000
    )
    assert state["async_records"][1]["expires_at"] is None
    assert state["sync_records"][0]["expires_at"] == state["sync_records"][0]["created_at"] + (
        86_400_000
    )
    assert state["sync_records"][1]["expires_at"] is None
    assert state["async_list_calls"] == [
        {"limit": 10, "since": state["async_records"][0]["created_at"] - 1},
        {"limit": 10, "since": state["async_records"][0]["created_at"] - 1},
    ]
    assert state["sync_list_calls"] == [
        {"limit": 10, "since": state["sync_records"][0]["created_at"] - 1},
        {"limit": 10, "since": state["sync_records"][0]["created_at"] - 1},
    ]
    assert state["async_pager_awaits"] == 1
    assert state["async_page_iterations"] == 1
    assert state["async_item_iterations"] == 1
    assert state["sync_page_iterations"] == 1
    assert state["sync_item_iterations"] == 1
    assert state["async_page_ids"] == [["async-snapshot-1", "async-snapshot-2"]]
    assert state["async_item_ids"] == ["async-snapshot-1", "async-snapshot-2"]
    assert state["sync_page_ids"] == [["sync-snapshot-1", "sync-snapshot-2"]]
    assert state["sync_item_ids"] == ["sync-snapshot-1", "sync-snapshot-2"]
    assert state["async_deleted_ids"] == ["async-snapshot-1", "async-snapshot-2"]
    assert state["sync_deleted_ids"] == ["sync-snapshot-1", "sync-snapshot-2"]


@pytest.mark.skipif(
    not _is_ci and not _has_credentials,
    reason="Requires BLOB_READ_WRITE_TOKEN, VERCEL_TOKEN, VERCEL_PROJECT_ID, and VERCEL_TEAM_ID",
)
@pytest.mark.parametrize("script_path", _example_files, ids=lambda p: p.name)
def test_example(script_path: Path) -> None:
    """Run a single example script and verify it succeeds."""
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as e:
        stdout = e.stdout.decode() if e.stdout else ""
        stderr = e.stderr.decode() if e.stderr else ""
        # Tail stdout to avoid overwhelming output
        max_chars = 10000
        if len(stdout) > max_chars:
            stdout = f"... [{len(stdout) - max_chars} chars truncated] ...\n" + stdout[-max_chars:]
        pytest.fail(
            f"{script_path.name} timed out after {e.timeout}s\n"
            f"STDOUT (tail):\n{stdout}\n"
            f"STDERR:\n{stderr}"
        )
    assert result.returncode == 0, (
        f"{script_path.name} failed with code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
