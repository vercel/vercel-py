"""User-visible Linux identities, filesystem isolation, and resume semantics."""

import inspect
import subprocess
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import anyio
import pytest

from vercel.sandbox import SandboxError, SandboxFilesystemError, SandboxPathNotFoundError

from ._sandbox_scenarios import AsyncDriver, SyncDriver
from .conftest import requires_sandbox_credentials


async def _call(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    result = method(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@asynccontextmanager
async def _scope(manager: Any) -> AsyncIterator[Any]:
    if hasattr(manager, "__aenter__"):
        async with manager as value:
            yield value
    else:
        with manager as value:
            yield value


async def _run(handle: Any, command: str, args: list[str] | None = None, **kwargs: Any) -> Any:
    return await _call(handle.run_process, command, args, capture_output=True, **kwargs)


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("driver", [AsyncDriver(), SyncDriver()], ids=["async", "sync"])
async def test_user_upload_provisioning_and_process_regressions(driver: Any) -> None:
    async with driver.session():
        async with driver.ephemeral_sandbox(
            f"vercel-py-user-regressions-{uuid4().hex[:10]}"
        ) as box:
            alice = await _call(box.create_user, "alice")
            bob = await _call(box.create_user, "bob")
            await _run(box, "/bin/mkdir", ["/tmp/shared"], sudo=True, check=True)
            await _run(box, "/bin/chmod", ["777", "/tmp/shared"], sudo=True, check=True)

            # Accessible parents cannot hide overly broad permissions on a
            # partially uploaded file. Check new and preexisting public files.
            for existing in (False, True):
                path = f"/tmp/shared/partial-{existing}"
                if existing:
                    await _call(alice.fs.write_text, path, "old contents", mode=0o644)
                with pytest.raises(RuntimeError, match="abort upload"):
                    async with _scope(
                        alice.fs.open(path, "wb", size=20, permissions=0o600)
                    ) as target:
                        await _call(target.write, b"secret")
                        assert await _call(alice.fs.read_bytes, path) == b"secret"
                        assert (await _run(alice, "stat", ["-c", "%a", path])).stdout == "600\n"
                        with pytest.raises(SandboxFilesystemError):
                            await _call(bob.fs.read_bytes, path)
                        raise RuntimeError("abort upload")
                assert await _call(alice.fs.read_bytes, path) == b"secret"
                assert (await _run(alice, "stat", ["-c", "%a", path])).stdout == "600\n"
                with pytest.raises(SandboxFilesystemError):
                    await _call(bob.fs.read_bytes, path)

            readonly = "/tmp/shared/readonly"
            await _call(alice.fs.write_text, readonly, "keep", mode=0o400)
            with pytest.raises(SandboxFilesystemError):
                async with _scope(
                    alice.fs.open(readonly, "wb", size=4, permissions=0o600)
                ) as target:
                    await _call(target.write, b"lost")
            assert await _call(alice.fs.read_text, readonly) == "keep"
            assert (await _run(alice, "stat", ["-c", "%a", readonly])).stdout == "400\n"

            # Read '-' as a filename through both eager and streaming reads.
            await _call(alice.fs.write_bytes, "-", b"literal dash\0\xff", mode=0o600)
            assert await _call(alice.fs.read_bytes, "-") == b"literal dash\0\xff"
            async with _scope(alice.fs.open("-", "rb")) as source:
                assert await _call(source.read) == b"literal dash\0\xff"

            # Rejected homes must retain ownership, permissions, and contents;
            # neither regular homes nor dangling symlinks may create accounts.
            await _run(box, "/bin/mkdir", ["/home/occupied"], sudo=True, check=True)
            await _run(box, "/bin/chmod", ["755", "/home/occupied"], sudo=True, check=True)
            await _call(box.fs.write_text, "/home/occupied/keep", "untouched")
            before = (await _run(box, "stat", ["-c", "%u:%g:%a", "/home/occupied"])).stdout
            await _run(box, "ln", ["-s", "/home/occupied", "/home/linked"], sudo=True, check=True)
            await _run(
                box, "ln", ["-s", "/tmp/absent-home", "/home/dangling"], sudo=True, check=True
            )
            for name in ("occupied", "linked", "dangling"):
                with pytest.raises(SandboxError, match="Home path already exists"):
                    await _call(box.create_user, name)
                assert (await _run(box, "getent", ["passwd", name])).returncode != 0
                assert (await _run(box, "getent", ["group", name])).returncode != 0
            assert (await _run(box, "stat", ["-c", "%u:%g:%a", "/home/occupied"])).stdout == before
            assert await _call(box.fs.read_text, "/home/occupied/keep") == "untouched"
            assert (await _run(box, "readlink", ["/home/linked"])).stdout == "/home/occupied\n"
            assert not await _call(box.fs.exists, "/tmp/absent-home")

            await _call(alice.fs.mkdir, "workspace")
            # Catchable signals must run the command's handler and preserve
            # its exit status, even when graceful cleanup outlasts wait's
            # initial interruption in the account monitors.
            for signal in ("INT", "TERM", "USR1"):
                cleanup = '/bin/sleep 0.3; printf "CLEANED\\n"; ' if signal != "INT" else ""
                args = [
                    "-c",
                    f"trap 'printf \"HANDLED\\n\"; {cleanup}exit 0' {signal}\n"
                    'printf "READY\\n"\nwhile :; do /bin/sleep 0.1; done',
                ]
                process = await _call(alice.create_process, "/bin/bash", args, cwd="workspace")
                metadata = ("/bin/bash", args, "/home/alice/workspace")
                with anyio.fail_after(10):
                    assert await _call(process.stdout.readline) == "READY\n"
                    if signal == "TERM":
                        await _call(process.terminate)
                    else:
                        await _call(process.send_signal, signal)
                    assert (process.name, process.args, process.cwd) == metadata
                    output, _ = await _call(process.communicate)
                    assert output == "HANDLED\n" + ("CLEANED\n" if cleanup else "")
                    assert await _call(process.wait) == 0

            # An unhandled signal must retain the actual signal exit status.
            process = await _call(
                alice.create_process,
                "/bin/bash",
                ["-c", 'printf "READY\\n"; exec /bin/sleep 60'],
            )
            with anyio.fail_after(10):
                assert await _call(process.stdout.readline) == "READY\n"
                await _call(process.terminate)
                output, _ = await _call(process.communicate)
                assert output == ""
                assert await _call(process.wait) == 143

            # Pause signals cannot safely target the command through this
            # launcher. Rejection must preserve a runnable target and all
            # handle metadata, rather than stopping only the monitor.
            args = ["-c", 'printf "%s\\n" "$$"; exec /bin/sleep 60']
            process = await _call(alice.create_process, "/bin/bash", args, cwd="workspace")
            metadata = ("/bin/bash", args, "/home/alice/workspace")
            with anyio.fail_after(10):
                pid = (await _call(process.stdout.readline)).strip()
                for signal in ("STOP", "TSTP", "TTIN", "TTOU"):
                    with pytest.raises(NotImplementedError, match=f"SIG{signal}.*user processes"):
                        await _call(process.send_signal, signal)
                    assert (process.name, process.args, process.cwd) == metadata
                    assert process.returncode is None
                    state = await _run(box, "/bin/ps", ["-o", "stat=", "-p", pid])
                    assert state.returncode == 0
                    assert state.stdout.strip().startswith("S")
                await _call(process.refresh)
                assert process.returncode is None
                assert (process.name, process.args, process.cwd) == metadata
                await _call(process.kill)
                assert await _call(process.communicate) == ("", "")
                assert await _call(process.wait) != 0

            # Check actual Linux children, not only backend return codes. The
            # command ignores catchable cleanup signals; KILL must still work.
            for method, sudo in (("kill", False), ("send_signal", True)):
                args = [
                    "-c",
                    'trap "" TERM USR1 ALRM; /bin/sleep 60 & printf "%s %s\\n" "$$" "$!"; wait',
                ]
                process = await _call(
                    alice.create_process, "/bin/bash", args, cwd="workspace", sudo=sudo
                )
                pids = (await _call(process.stdout.readline)).strip().split()
                assert len(pids) == 2
                metadata = ("/bin/bash", args, "/home/alice/workspace")
                await _call(process.send_signal, "CONT")
                assert (process.name, process.args, process.cwd) == metadata
                started = time.monotonic()
                await _call(
                    getattr(process, method), *(["KILL"] if method == "send_signal" else [])
                )
                assert (process.name, process.args, process.cwd) == metadata
                with anyio.fail_after(10):
                    assert await _call(process.communicate) == ("", "")
                    assert await _call(process.wait) != 0
                assert time.monotonic() - started < 10
                states = (await _run(box, "/bin/ps", ["-o", "stat=", "-p", ",".join(pids)])).stdout
                assert all(state.startswith("Z") for state in states.split())
                assert (process.name, process.args, process.cwd) == metadata

            # The internal deadline is trusted even with a custom PATH, while
            # the command itself continues to resolve the caller's PATH.
            custom_bin = "/home/alice/bin"
            await _call(alice.fs.write_text, "bin/timeout", "#!/bin/sh\nexit 42\n", mode=0o700)
            await _call(
                alice.fs.write_text,
                "bin/custom-command",
                '#!/bin/sh\nprintf "%s\\n" "$PATH"; exec /bin/sleep 60\n',
                mode=0o700,
            )
            for api in ("run_process", "create_process"):
                for command, args in (("/bin/sleep", ["60"]), ("custom-command", [])):
                    started = time.monotonic()
                    result = await _call(
                        getattr(alice, api),
                        command,
                        args,
                        env={"PATH": custom_bin},
                        kill_after=0.3,
                        **({"capture_output": True} if api == "run_process" else {}),
                    )
                    output = result.stdout
                    if api == "create_process":
                        with anyio.fail_after(10):
                            output, _ = await _call(result.communicate)
                            await _call(result.wait)
                    assert result.returncode not in (0, 42, 126, 127)
                    assert time.monotonic() - started < 10
                    assert output == (custom_bin + "\n" if command == "custom-command" else "")


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("driver", [AsyncDriver(), SyncDriver()], ids=["async", "sync"])
@pytest.mark.parametrize("returncode", [0, 42, 200])
async def test_user_rapid_term_handler_preserves_returncode(driver: Any, returncode: int) -> None:
    async with driver.session():
        async with driver.ephemeral_sandbox(f"vercel-py-user-term-{uuid4().hex[:10]}") as box:
            alice = await _call(box.create_user, "alice")
            args = [
                "-c",
                f"trap 'printf \"HANDLED\\n\"; exit {returncode}' TERM\n"
                'printf "READY\\n"\nwhile :; do /usr/bin/sleep 0.01; done',
            ]
            # Repeated immediate completions exercise the interrupted-wait race.
            for _ in range(5):
                process = await _call(alice.create_process, "/bin/bash", args)
                with anyio.fail_after(10):
                    assert await _call(process.stdout.readline) == "READY\n"
                    await _call(process.terminate)
                    output, _ = await _call(process.communicate)
                    assert output == "HANDLED\n"
                    assert process.returncode == returncode
                    assert await _call(process.wait) == returncode


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("driver", [AsyncDriver(), SyncDriver()], ids=["async", "sync"])
async def test_user_identity_and_permission_enforcement(driver: Any) -> None:
    async with driver.session():
        async with driver.ephemeral_sandbox(f"vercel-py-users-{uuid4().hex[:10]}") as box:
            runtime = await _call(box.session)
            alice = await _call(box.create_user, "alice")
            bob = await _call(runtime.create_user, "bob")
            assert alice.home_dir == "/home/alice"
            assert bob.home_dir == "/home/bob"
            with pytest.raises(SandboxError):
                await _call(runtime.create_user, "alice")

            # Safe argv includes empty strings, quotes, newlines, and shell syntax.
            argv = [
                "",
                "two words",
                "'quotes'",
                '"double"',
                "$(touch /tmp/injected)",
                "a\nb",
                "--flag",
            ]
            echoed = await _run(alice, "printf", ["%s\\0", *argv], check=True)
            assert echoed.stdout == "\0".join(argv) + "\0"
            assert not await _call(box.fs.exists, "/tmp/injected")
            await _call(alice.fs.write_text, "-runner", "#!/bin/sh\nprintf safe", mode=0o700)
            dash_command = await _run(
                alice, "-runner", env={"PATH": "/home/alice:/usr/bin:/bin"}, check=True
            )
            assert dash_command.stdout == "safe"
            environment = await _run(
                alice,
                "bash",
                ["-c", 'printf "%s\\n" "$HOME" "$USER" "$LOGNAME" "$CUSTOM"; pwd; id -un; id -gn'],
                env={"CUSTOM": "a 'value' $HOME\nnext"},
                check=True,
            )
            assert (
                environment.stdout
                == "/home/alice\nalice\nalice\na 'value' $HOME\nnext\n/home/alice\nalice\nalice\n"
            )
            root_command = await _run(alice, "id", ["-u"], sudo=True, check=True)
            assert root_command.stdout == "0\n"
            assert (await _run(alice, "whoami", check=True)).stdout == "alice\n"

            process = await _call(bob.create_process, "whoami")
            assert process.name == "whoami" and process.args == []
            assert process.cwd == "/home/bob"
            stdout, stderr = await _call(process.communicate)
            assert stdout == "bob\n" and stderr == ""
            assert process.returncode == 0
            assert process.cwd == "/home/bob" and process.name == "whoami"
            sleeper = await _call(bob.create_process, "sleep", ["60"])
            with anyio.fail_after(10):
                await _call(sleeper.terminate)
                assert await _call(sleeper.wait) != 0
                assert (await _run(alice, "sleep", ["60"], kill_after=0.1)).returncode != 0
            with pytest.raises(SandboxPathNotFoundError):
                await _call(alice.fs.read_text, "missing.txt")
            with pytest.raises(SandboxPathNotFoundError):
                await _call(alice.fs.mkdir, "missing/child", recursive=False)
            await _run(box, "groupadd", ["agents"], sudo=True, check=True)
            await _run(box, "usermod", ["-aG", "agents", "alice"], sudo=True, check=True)
            groups = (await _run(alice, "id", ["-Gn"], check=True)).stdout.split()
            assert set(groups) == {"alice", "agents"}
            with pytest.raises(subprocess.CalledProcessError):
                await _run(alice, "false", check=True)

            data = bytes(range(256)) * 320  # Binary transfer spans multiple command chunks.
            await _call(alice.fs.mkdir, "workspace")
            await _call(alice.fs.write_bytes, "secret.bin", data, cwd="workspace", mode=0o600)
            assert await _call(alice.fs.read_bytes, "secret.bin", cwd="workspace") == data
            assert await _call(alice.fs.is_dir, "workspace")
            assert await _call(alice.fs.is_file, "workspace/secret.bin")
            assert [entry.path for entry in await _call(alice.fs.listdir, "workspace")] == [
                "secret.bin"
            ]
            ownership = await _run(
                alice, "stat", ["-c", "%U:%G:%a", "workspace/secret.bin"], check=True
            )
            assert ownership.stdout == "alice:alice:600\n"
            assert (
                await _run(alice, "pwd", cwd="workspace", check=True)
            ).stdout == "/home/alice/workspace\n"
            async with _scope(alice.fs.batch()) as batch:
                batch.write_text("batch/note.txt", "batch contents", mode=0o600)
            assert await _call(alice.fs.read_text, "batch/note.txt") == "batch contents"
            for size in (None, len(data)):
                async with _scope(
                    alice.fs.open("stream.bin", "wb", size=size, permissions=0o600)
                ) as target:
                    await _call(target.write, data)
                async with _scope(alice.fs.open("stream.bin", "rb")) as source:
                    assert await _call(source.read, 19) == data[:19]
                    assert await _call(source.read) == data[19:]
            private = "/home/alice/workspace/secret.bin"
            operations: list[tuple[Callable[..., Any], tuple[str, ...], dict[str, Any]]] = [
                (bob.fs.read_bytes, (private,), {}),
                (bob.fs.write_text, (private, "overwrite"), {}),
                (bob.fs.mkdir, ("/home/alice/new-dir",), {}),
                (bob.fs.listdir, ("/home/alice",), {}),
                (bob.fs.remove, (private,), {}),
                (bob.fs.rename, (private, "/tmp/stolen"), {}),
            ]
            for method, args, kwargs in operations:
                with pytest.raises(SandboxFilesystemError):
                    await _call(method, *args, **kwargs)
            for mode in ("r", "w", "rb", "wb"):
                with pytest.raises(SandboxFilesystemError):
                    async with _scope(bob.fs.open(private, mode)) as stream:
                        if mode.startswith("w"):
                            await _call(stream.write, b"denied" if "b" in mode else "denied")
            with pytest.raises(SandboxFilesystemError):
                async with _scope(bob.fs.batch()) as batch:
                    batch.write_text(private, "denied")
            assert not await _call(bob.fs.exists, private)
            assert not await _call(bob.fs.is_file, private)
            assert not await _call(bob.fs.is_dir, "/home/alice/workspace")
            assert await _call(alice.fs.read_bytes, "workspace/secret.bin") == data

            # An absolute path and a symlink must not bypass the account's permissions.
            await _run(bob, "ln", ["-s", private, "/home/bob/link"], check=True)
            for path in ("/etc/shadow", "link"):
                with pytest.raises(SandboxFilesystemError):
                    await _call(bob.fs.write_text, path, "denied")
            await _call(alice.fs.write_text, "readonly.txt", "keep", mode=0o400)
            with pytest.raises(SandboxFilesystemError):
                await _call(alice.fs.write_text, "readonly.txt", "overwrite")
            assert await _call(alice.fs.read_text, "readonly.txt") == "keep"
            await _call(alice.fs.rename, "workspace/secret.bin", "workspace/moved.bin")
            await _call(alice.fs.remove, "workspace/moved.bin")
            assert not await _call(alice.fs.exists, "workspace/moved.bin")

            # Existing accounts may have nonconventional homes and primary groups.
            await _run(
                box,
                "useradd",
                ["-m", "-d", "/srv/custom-home", "-g", "users", "custom"],
                sudo=True,
                check=True,
            )
            custom = runtime.as_user("custom")
            assert custom.home_dir is None
            assert (await _run(custom, "pwd", check=True)).stdout == "/srv/custom-home\n"
            assert custom.home_dir == "/srv/custom-home"
            assert (await _run(custom, "id", ["-gn"], check=True)).stdout == "users\n"
            root = box.as_user("root")
            assert root.home_dir == "/root"
            assert (await _run(root, "pwd", check=True)).stdout == "/root\n"
            absent = box.as_user("missing_account")
            with pytest.raises(SandboxError, match="does not exist"):
                await _run(absent, "true")
            assert (await _run(box, "getent", ["passwd", "missing_account"])).returncode != 0


@requires_sandbox_credentials
@pytest.mark.live
@pytest.mark.asyncio
@pytest.mark.parametrize("driver", [AsyncDriver(), SyncDriver()], ids=["async", "sync"])
async def test_sandbox_users_resume_and_session_users_remain_pinned(driver: Any) -> None:
    async with driver.session():
        async with driver.ephemeral_sandbox(f"vercel-py-users-resume-{uuid4().hex[:10]}") as box:
            alice = await _call(box.create_user, "alice")
            runtime = await _call(box.session)
            pinned = runtime.as_user("alice")
            await _call(alice.fs.write_text, "persisted.txt", "survives snapshot")
            await _call(box.snapshot)
            await _call(runtime.stop)
            assert await _call(alice.fs.read_text, "persisted.txt") == "survives snapshot"
            resumed = await _run(alice, "whoami", check=True)
            assert resumed.stdout == "alice\n" and resumed.session_id != runtime.id
            with pytest.raises(SandboxError):
                await _run(pinned, "true")
