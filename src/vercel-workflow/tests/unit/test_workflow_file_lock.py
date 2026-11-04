"""Directory lease ownership, recovery, and cancellation boundaries."""

from __future__ import annotations

import os
from pathlib import Path

import anyio
import pytest

from vercel.workflow._internal import file_lock as locks


async def test_missing_target_is_locked_and_released(tmp_path) -> None:
    target = tmp_path / "absent.json"
    path = tmp_path / "absent.json.lock"
    async with locks.file_lock(target) as lease:
        assert path.is_dir()
        assert not target.exists()
        lease.check()
    assert not path.exists()


async def test_body_error_is_preserved_and_lock_released(tmp_path) -> None:
    target = tmp_path / "resource"
    error = ValueError("body failed")
    with pytest.raises(ValueError) as raised:
        async with locks.file_lock(target):
            raise error
    assert raised.value is error
    assert not Path(f"{target}.lock").exists()


async def test_body_cancellation_releases_lock(tmp_path) -> None:
    target = tmp_path / "resource"
    with anyio.move_on_after(0.05) as scope:
        async with locks.file_lock(target):
            await anyio.sleep_forever()
    assert scope.cancel_called
    assert not Path(f"{target}.lock").exists()


async def test_waiter_times_out_without_removing_live_lock(tmp_path) -> None:
    target = tmp_path / "resource"
    async with locks.file_lock(target) as owner:
        with pytest.raises(locks.FileLockTimeoutError):
            async with locks.file_lock(target, timeout=0.05):
                pytest.fail("two owners")
        owner.check()


async def test_waiter_acquires_after_owner_releases(tmp_path) -> None:
    target = tmp_path / "resource"
    acquired = anyio.Event()

    async def wait() -> None:
        async with locks.file_lock(target):
            acquired.set()

    async with anyio.create_task_group() as group:
        async with locks.file_lock(target):
            group.start_soon(wait)
            await anyio.sleep(0.05)
            assert not acquired.is_set()
        with anyio.fail_after(2):
            await acquired.wait()


async def test_expired_directory_is_recovered(tmp_path) -> None:
    target = tmp_path / "resource"
    path = Path(f"{target}.lock")
    path.mkdir()
    os.utime(path, (0, 0))
    async with locks.file_lock(target, timeout=0) as owner:
        owner.check()
        assert path.stat().st_mtime > 0
    assert not path.exists()


@pytest.mark.parametrize("replace", [False, True])
async def test_stale_observation_does_not_delete_renewed_or_replaced_lock(
    tmp_path, monkeypatch, replace
) -> None:
    target = tmp_path / "resource"
    path = Path(f"{target}.lock")
    path.mkdir()
    os.utime(path, (0, 0))
    stat = Path.stat
    changed = False
    successor = None

    def stale_stat(self, *args, **kwargs):
        nonlocal changed, successor
        observed = stat(self, *args, **kwargs)
        if self == path and not changed:
            changed = True
            # Another owner renews/replaces the lock while this waiter still
            # holds an old stat result. Recovery must revalidate that result.
            if replace:
                path.rmdir()
                path.mkdir()
            os.utime(path, None)
            successor = stat(path)
        return observed

    monkeypatch.setattr(Path, "stat", stale_stat)
    with pytest.raises(locks.FileLockTimeoutError):
        async with locks.file_lock(target, timeout=0):
            pytest.fail("stole a fresh lock using an old observation")
    assert path.stat() == successor


async def test_heartbeat_renews_ownership_marker(tmp_path) -> None:
    target = tmp_path / "resource"
    async with locks.file_lock(target, stale=2) as owner:
        initial = owner.path.stat().st_mtime_ns
        with anyio.fail_after(3):
            while owner.path.stat().st_mtime_ns == initial:
                await anyio.sleep(0.02)
        owner.check()
        with pytest.raises(locks.FileLockTimeoutError):
            async with locks.file_lock(target, stale=2, timeout=0):
                pytest.fail("stole a heartbeating lock")


@pytest.mark.parametrize("replace", [False, True])
async def test_lost_lease_cancels_body_and_preserves_successor(tmp_path, replace) -> None:
    target = tmp_path / "resource"
    path = Path(f"{target}.lock")
    successor = None
    with pytest.raises(locks.FileLockCompromisedError):
        with anyio.fail_after(3):
            async with locks.file_lock(target):
                path.rmdir()
                if replace:
                    path.mkdir()
                    successor = path.stat()
                await anyio.sleep_forever()
    assert path.exists() is replace
    if replace:
        assert path.stat() == successor


async def test_ownership_is_checked_before_release_without_await(tmp_path) -> None:
    target = tmp_path / "resource"
    path = Path(f"{target}.lock")
    with pytest.raises(locks.FileLockCompromisedError):
        async with locks.file_lock(target):
            os.utime(path, (0, 0))
    assert path.exists()


async def test_cancelled_waiter_does_not_remove_owner_lock(tmp_path) -> None:
    target = tmp_path / "resource"
    async with locks.file_lock(target) as owner:
        with anyio.move_on_after(0.05) as scope:
            async with locks.file_lock(target):
                pytest.fail("two owners")
        assert scope.cancel_called
        owner.check()


@pytest.mark.parametrize("kwargs", [{"stale": 0}, {"timeout": -1}, {"update": float("nan")}])
async def test_invalid_timings_are_rejected(tmp_path, kwargs) -> None:
    with pytest.raises(ValueError):
        async with locks.file_lock(tmp_path / "resource", **kwargs):
            pytest.fail("accepted invalid lease timings")


async def test_transient_heartbeat_error_can_recover(tmp_path, monkeypatch) -> None:
    refresh = locks.FileLock._refresh
    attempts = 0

    def intermittent(self):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise PermissionError("temporary filesystem error")
        refresh(self)

    monkeypatch.setattr(locks.FileLock, "_refresh", intermittent)
    async with locks.file_lock(tmp_path / "resource", stale=3) as owner:
        with anyio.fail_after(4):
            while attempts < 2:
                await anyio.sleep(0.02)
        owner.check()


async def test_persistent_heartbeat_error_cancels_body(tmp_path, monkeypatch) -> None:
    def unavailable(self):
        raise PermissionError("filesystem unavailable")

    monkeypatch.setattr(locks.FileLock, "_refresh", unavailable)
    path = tmp_path / "resource.lock"
    with pytest.raises(locks.FileLockCompromisedError) as raised:
        with anyio.fail_after(4):
            async with locks.file_lock(tmp_path / "resource", stale=2):
                await anyio.sleep_forever()
    assert isinstance(raised.value.__cause__, PermissionError)
    # Ownership is uncertain: leave the directory for later stale recovery.
    assert path.exists()
