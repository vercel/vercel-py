"""User-visible Linux groups, shared directories, and membership changes."""

import inspect
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import pytest

from vercel.sandbox import SandboxError, SandboxFilesystemError, SandboxGroupAlreadyExistsError

from ._sandbox_scenarios import AsyncDriver, SyncDriver
from .conftest import requires_sandbox_credentials


async def _call(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


async def _run(handle: Any, command: str, args: list[str] | None = None, **kwargs: Any) -> Any:
    return await _call(handle.run_command, command, args, capture_output=True, **kwargs)


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("driver", [AsyncDriver(), SyncDriver()], ids=["async", "sync"])
async def test_group_members_share_a_directory(driver: Any) -> None:
    async with driver.session():
        async with driver.ephemeral_sandbox(f"vercel-py-groups-{uuid4().hex[:10]}") as box:
            runtime = await _call(box.session)
            alice = await _call(box.create_user, "alice")
            bob = await _call(runtime.create_user, "bob")
            carol = await _call(box.create_user, "carol")

            devs = await _call(box.create_group, "devs")
            assert (devs.groupname, devs.shared_dir) == ("devs", "/shared/devs")
            default_user = (await _run(box, "id", ["-un"], check=True)).stdout.decode().strip()
            shared = await _run(box, "stat", ["-c", "%U:%G:%a", devs.shared_dir], check=True)
            assert shared.stdout == f"{default_user}:devs:2770\n".encode()

            with pytest.raises(SandboxGroupAlreadyExistsError) as duplicate:
                await _call(runtime.create_group, "devs")
            assert duplicate.value.groupname == "devs"
            recovered = runtime.as_group(duplicate.value.groupname)
            await _call(recovered.add_user, alice)
            await _call(bob.add_to_group, devs)
            for member in (alice, bob):
                groups = (await _run(member, "id", ["-Gn"], check=True)).stdout.decode().split()
                assert set(groups) == {member.username, "devs"}

            # An occupied shared path must not leave a group behind.
            await _run(box, "/bin/mkdir", ["/shared/occupied"], sudo=True, check=True)
            with pytest.raises(SandboxError, match="Shared path already exists") as occupied:
                await _call(box.create_group, "occupied")
            assert not isinstance(occupied.value, SandboxGroupAlreadyExistsError)
            assert (await _run(box, "getent", ["group", "occupied"])).returncode != 0

            # Commands create entries with the shared group, as setgid assigns it.
            spec = f"{devs.shared_dir}/spec.txt"
            await _run(alice, "bash", ["-c", 'printf "spec v2" > "$1"', "write", spec], check=True)
            assert (await _run(bob, "cat", [spec], check=True)).stdout == b"spec v2"
            assert (await _run(carol, "cat", [spec])).returncode != 0
            assert (await _run(carol, "ls", [devs.shared_dir])).returncode != 0

            # SDK uploads and directories keep that group too, so every member,
            # the default account, and later uploads can reach them.
            notes = f"{devs.shared_dir}/notes.txt"
            reply = f"{devs.shared_dir}/drafts/reply.txt"
            await _call(alice.fs.write_text, notes, "from alice")
            await _call(alice.fs.mkdir, f"{devs.shared_dir}/drafts")
            await _call(bob.fs.write_text, reply, "from bob")
            assert await _call(bob.fs.read_text, notes) == "from alice"
            assert await _call(alice.fs.read_text, reply) == "from bob"
            assert await _call(box.fs.read_text, spec) == "spec v2"
            ownership = await _run(
                box,
                "stat",
                ["-c", "%U:%G:%a", spec, notes, f"{devs.shared_dir}/drafts", reply],
                sudo=True,
                check=True,
            )
            assert (
                ownership.stdout
                == (
                    f"alice:devs:644\nalice:devs:644\n{default_user}:devs:2770\nbob:devs:644\n"
                ).encode()
            )
            await _run(bob, "touch", [f"{devs.shared_dir}/drafts/from-command"], check=True)
            with pytest.raises(SandboxFilesystemError):
                await _call(carol.fs.read_text, notes)

            # Uploads outside a shared directory still use the primary group.
            await _call(alice.fs.write_text, "private.txt", "alice only", mode=0o600)
            private = await _run(alice, "stat", ["-c", "%U:%G:%a", "private.txt"], check=True)
            assert private.stdout == b"alice:alice:600\n"

            # Membership changes apply to the next command.
            await _call(devs.remove_user, "bob")
            assert (await _run(bob, "cat", [spec])).returncode != 0
            with pytest.raises(SandboxFilesystemError):
                await _call(bob.fs.read_text, notes)
            await _call(alice.remove_from_group, "devs")
            assert (await _run(alice, "id", ["-Gn"], check=True)).stdout == b"alice\n"

            with pytest.raises(SandboxError, match="not a member"):
                await _call(devs.remove_user, carol)
            with pytest.raises(SandboxError, match="does not exist"):
                await _call(box.as_group("missing_group").add_user, carol)
            with pytest.raises(SandboxError, match="does not exist"):
                await _call(devs.add_user, "missing_account")
