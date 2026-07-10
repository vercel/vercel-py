"""Tests for the workflow sandbox (vercel.workflow.py_sandbox).

Covers:
- _RESTRICTIONS: builtins, datetime, os, time, socket, random, threading, asyncio
- _BLOCKED: subprocess, ssl, ctypes, multiprocessing, signal, etc.
- _PASSTHROUGHS: stdlib modules that pass through unchanged
- Loop proxy: allowlisted methods pass, everything else restricted
- Random determinism: seeded random produces repeatable results
- Module isolation: non-passthrough modules are freshly imported
"""

from __future__ import annotations

import sys

import pytest

from vercel._internal.workflow.py_sandbox import SandboxRestrictionError, workflow_sandbox
from vercel.workflow.sandbox import (
    ALL_CLEANUPS,
    SandboxCleanupContext,
    SandboxPolicy,
)

CLEANUP_ALL = SandboxPolicy(cleanups=ALL_CLEANUPS)

SEED = "test-seed-42"


# ── helpers ────────────────────────────────────────────────────


def _run_in_sandbox(code: str, seed: str = SEED) -> dict:
    """exec *code* inside a sandbox and return its local namespace."""
    ns: dict = {}
    with workflow_sandbox(random_seed=seed):
        # Pass sandbox builtins so that exec'd code sees the proxy.
        ns["__builtins__"] = sys.modules["builtins"]
        exec(code, ns)  # noqa: S102
    return ns


def _raises_in_sandbox(code: str, seed: str = SEED) -> None:
    """Assert that *code* raises SandboxRestrictionError inside a sandbox."""
    with pytest.raises(SandboxRestrictionError):
        _run_in_sandbox(code, seed)


# ═══════════════════════════════════════════════════════════════
#  builtins restrictions
# ═══════════════════════════════════════════════════════════════


class TestBuiltinsRestrictions:
    def test_open_blocked(self):
        _raises_in_sandbox("open('/dev/null')")

    def test_input_blocked(self):
        _raises_in_sandbox("input()")

    def test_breakpoint_blocked(self):
        _raises_in_sandbox("breakpoint()")

    def test_eval_blocked(self):
        _raises_in_sandbox("eval('1+1')")

    def test_exec_blocked(self):
        _raises_in_sandbox("exec('pass')")

    def test_compile_blocked(self):
        _raises_in_sandbox("compile('pass', '<string>', 'exec')")

    def test_print_allowed(self):
        _run_in_sandbox("print('hello')")

    def test_len_allowed(self):
        ns = _run_in_sandbox("result = len([1,2,3])")
        assert ns["result"] == 3

    def test_isinstance_allowed(self):
        ns = _run_in_sandbox("result = isinstance(42, int)")
        assert ns["result"] is True


# ═══════════════════════════════════════════════════════════════
#  datetime restrictions
# ═══════════════════════════════════════════════════════════════


class TestDatetimeRestrictions:
    def test_datetime_now_blocked(self):
        _raises_in_sandbox("import datetime; datetime.datetime.now()")

    def test_datetime_utcnow_blocked(self):
        _raises_in_sandbox("import datetime; datetime.datetime.utcnow()")

    def test_date_today_blocked(self):
        _raises_in_sandbox("import datetime; datetime.date.today()")

    def test_datetime_constructor_allowed(self):
        ns = _run_in_sandbox("import datetime; result = datetime.datetime(2024, 1, 1, 12, 0, 0)")
        assert ns["result"].year == 2024

    def test_timedelta_allowed(self):
        ns = _run_in_sandbox("import datetime; result = datetime.timedelta(days=5).total_seconds()")
        assert ns["result"] == 5 * 86400

    def test_isinstance_datetime(self):
        ns = _run_in_sandbox(
            "import datetime; result = isinstance(datetime.datetime(2024, 1, 1), datetime.datetime)"
        )
        assert ns["result"] is True

    def test_isinstance_date(self):
        ns = _run_in_sandbox(
            "import datetime; result = isinstance(datetime.date(2024, 1, 1), datetime.date)"
        )
        assert ns["result"] is True


# ═══════════════════════════════════════════════════════════════
#  os restrictions (allowlist)
# ═══════════════════════════════════════════════════════════════


class TestOsRestrictions:
    def test_os_path_allowed(self):
        ns = _run_in_sandbox("import os; result = os.path.join('a', 'b')")
        assert ns["result"] == "a/b"

    def test_os_sep_allowed(self):
        ns = _run_in_sandbox("import os; result = os.sep")
        assert ns["result"] == "/"

    def test_os_name_allowed(self):
        ns = _run_in_sandbox("import os; result = os.name")
        assert isinstance(ns["result"], str)

    def test_os_fspath_allowed(self):
        ns = _run_in_sandbox("import os; result = os.fspath('/tmp')")
        assert ns["result"] == "/tmp"

    def test_os_constants_allowed(self):
        _run_in_sandbox("import os; _ = os.O_RDONLY")

    def test_os_getcwd_allowed(self):
        ns = _run_in_sandbox("import os; result = os.getcwd()")
        assert isinstance(ns["result"], str)

    def test_os_listdir_blocked(self):
        _raises_in_sandbox("import os; os.listdir('.')")

    def test_os_urandom_blocked(self):
        _raises_in_sandbox("import os; os.urandom(16)")

    def test_os_getpid_blocked(self):
        _raises_in_sandbox("import os; os.getpid()")

    def test_os_fork_dropped(self):
        with workflow_sandbox(random_seed=SEED):
            import os

            assert not hasattr(os, "fork")

    def test_os_register_at_fork_dropped(self):
        with workflow_sandbox(random_seed=SEED):
            import os

            assert not hasattr(os, "register_at_fork")

    def test_traceback_format_exc_with_real_files(self):
        """traceback.format_exc() must work inside the sandbox when the
        traceback includes frames from real files on disk.

        Regression: traceback uses linecache which calls os.stat() and
        open(), triggering SandboxRestrictionError.
        """
        # json.loads raises from a real .py file (not <string>)
        ns = _run_in_sandbox(
            "import traceback, ast\n"
            "try:\n"
            "    ast.literal_eval('{')\n"
            "except Exception:\n"
            "    result = traceback.format_exc()\n"
        )
        # traceback.format_exc() must produce output with real file paths
        assert "ast.py" in ns["result"]

    def test_os_environ_is_copy(self):
        """os.environ should be a static copy, not the live environ."""
        import os as real_os

        ns = _run_in_sandbox("import os; result = type(os.environ)")
        # Should be a dict (copy), not os._Environ
        assert ns["result"] is dict or ns["result"] is not type(real_os.environ)


# ═══════════════════════════════════════════════════════════════
#  time restrictions (allowlist)
# ═══════════════════════════════════════════════════════════════


class TestTimeRestrictions:
    def test_time_time_blocked(self):
        _raises_in_sandbox("import time; time.time()")

    def test_time_sleep_blocked(self):
        _raises_in_sandbox("import time; time.sleep(0)")

    def test_time_monotonic_blocked(self):
        _raises_in_sandbox("import time; time.monotonic()")

    def test_time_perf_counter_blocked(self):
        _raises_in_sandbox("import time; time.perf_counter()")

    def test_time_gmtime_blocked(self):
        _raises_in_sandbox("import time; time.gmtime()")

    def test_time_localtime_blocked(self):
        _raises_in_sandbox("import time; time.localtime()")

    def test_time_mktime_allowed(self):
        ns = _run_in_sandbox("import time; result = time.mktime((2024, 1, 1, 0, 0, 0, 0, 1, -1))")
        assert isinstance(ns["result"], float)

    def test_time_strptime_accessible(self):
        """strptime is in the allowlist (not restricted), but may fail at
        runtime because _strptime internally calls strftime which is blocked."""
        ns = _run_in_sandbox("import time; result = time.strptime")
        assert callable(ns["result"])

    def test_time_struct_time_allowed(self):
        _run_in_sandbox("import time; _ = time.struct_time")

    def test_time_constants_allowed(self):
        _run_in_sandbox("import time; _ = time.CLOCK_MONOTONIC")

    @pytest.mark.asyncio
    async def test_concurrent_coroutine_not_affected_by_sandbox(self):
        """Regression test: a concurrent coroutine that lazy-imports time
        while another coroutine has the sandbox active must NOT be
        restricted — the sandbox context is per-task via ContextVar."""
        import asyncio

        barrier = asyncio.Event()
        result = None

        async def sandbox_task():
            """Activates the sandbox and waits inside it."""
            with workflow_sandbox(random_seed="test"):
                barrier.set()
                await asyncio.sleep(0.05)

        async def victim_task():
            """Lazy-imports time while sandbox is active, then calls time.time()."""
            nonlocal result
            await barrier.wait()
            import time

            result = time.time()

        await asyncio.gather(sandbox_task(), victim_task())
        assert isinstance(result, float) and result > 0

    @pytest.mark.asyncio
    async def test_concurrent_sandboxes_do_not_corrupt_sys_modules(self):
        """Two concurrent sandboxes must not corrupt sys.modules.

        The first sandbox to enter patches sys.modules; the second must
        not snapshot the patched state.  On exit, sys.modules must be
        fully restored regardless of exit order."""
        import asyncio
        import time as real_time

        ready_a = asyncio.Event()
        ready_b = asyncio.Event()

        async def sandbox_a():
            with workflow_sandbox(random_seed="a"):
                ready_a.set()
                await ready_b.wait()
                _raises_in_sandbox("import time; time.time()")
                await asyncio.sleep(0.02)

        async def sandbox_b():
            with workflow_sandbox(random_seed="b"):
                ready_b.set()
                await ready_a.wait()
                _raises_in_sandbox("import time; time.time()")
                await asyncio.sleep(0.01)

        await asyncio.gather(sandbox_a(), sandbox_b())
        # After both exit, sys.modules must be restored
        assert sys.modules.get("time") is real_time
        # Sanity check: no proxy modules leaked
        assert isinstance(sys.modules["time"].time, type(real_time.time))

    @pytest.mark.asyncio
    async def test_concurrent_sandboxes_have_independent_random_seeds(self):
        """Two concurrent sandboxes with different seeds must each see
        their own deterministic random sequence, not interfere with
        each other."""
        import asyncio

        results: dict[str, list[float]] = {}

        async def run_sandbox(
            seed: str,
            res: dict[str, list[float]],
            ready: asyncio.Event,
            other: asyncio.Event,
        ):
            with workflow_sandbox(random_seed=seed):
                ready.set()
                await other.wait()
                ns: dict[str, object] = {}
                ns["__builtins__"] = sys.modules["builtins"]
                exec(  # noqa: S102
                    "import random; result = [random.random() for _ in range(5)]",
                    ns,
                )
                res[seed] = ns["result"]  # type: ignore[assignment]

        r1, r2 = asyncio.Event(), asyncio.Event()
        await asyncio.gather(
            run_sandbox("seed-A", results, r1, r2),
            run_sandbox("seed-B", results, r2, r1),
        )
        assert "seed-A" in results and "seed-B" in results
        assert results["seed-A"] != results["seed-B"]

        # Each sequence must be deterministic: run again and compare
        results2: dict[str, list[float]] = {}
        r1, r2 = asyncio.Event(), asyncio.Event()
        await asyncio.gather(
            run_sandbox("seed-A", results2, r1, r2),
            run_sandbox("seed-B", results2, r2, r1),
        )
        assert results["seed-A"] == results2["seed-A"]
        assert results["seed-B"] == results2["seed-B"]

    @pytest.mark.asyncio
    async def test_concurrent_sandboxes_interleaved_random(self):
        """When two sandboxes interleave their random calls, each must
        still get its own deterministic sequence."""
        import asyncio

        step1 = asyncio.Event()
        step2 = asyncio.Event()
        results: dict[str, list[float]] = {}

        async def sandbox_a():
            with workflow_sandbox(random_seed="aaa"):
                ns = {}
                ns["__builtins__"] = sys.modules["builtins"]
                exec("import random; r1 = random.random()", ns)  # noqa: S102
                step1.set()  # let B run
                await step2.wait()  # wait for B to call random
                exec("r2 = random.random()", ns)  # noqa: S102
                results["a"] = [ns["r1"], ns["r2"]]

        async def sandbox_b():
            with workflow_sandbox(random_seed="bbb"):
                await step1.wait()  # wait for A's first call
                ns = {}
                ns["__builtins__"] = sys.modules["builtins"]
                exec("import random; r1 = random.random()", ns)  # noqa: S102
                step2.set()  # let A continue
                results["b"] = [ns["r1"]]

        await asyncio.gather(sandbox_a(), sandbox_b())

        # Run A alone to get the expected sequence
        expected_a: list[float] = []
        with workflow_sandbox(random_seed="aaa"):
            ns = {}
            ns["__builtins__"] = sys.modules["builtins"]
            exec(  # noqa: S102
                "import random; result = [random.random(), random.random()]",
                ns,
            )
            expected_a = ns["result"]

        assert results["a"] == expected_a, (
            f"Sandbox A's random sequence was corrupted by concurrent sandbox B: "
            f"got {results['a']}, expected {expected_a}"
        )


# ═══════════════════════════════════════════════════════════════
#  socket restrictions (allowlist)
# ═══════════════════════════════════════════════════════════════


class TestSocketRestrictions:
    def test_socket_socket_blocked(self):
        _raises_in_sandbox("import socket; socket.socket()")

    def test_socket_create_connection_blocked(self):
        _raises_in_sandbox("import socket; socket.create_connection(('localhost', 80))")

    def test_socket_htons_allowed(self):
        ns = _run_in_sandbox("import socket; result = socket.htons(80)")
        assert isinstance(ns["result"], int)

    def test_socket_inet_aton_allowed(self):
        ns = _run_in_sandbox("import socket; result = socket.inet_aton('127.0.0.1')")
        assert isinstance(ns["result"], bytes)

    def test_socket_constants_allowed(self):
        _run_in_sandbox("import socket; _ = socket.AF_INET")

    def test_socket_error_allowed(self):
        _run_in_sandbox("import socket; _ = socket.error")


# ═══════════════════════════════════════════════════════════════
#  random restrictions + determinism
# ═══════════════════════════════════════════════════════════════


class TestRandomRestrictions:
    def test_random_new_instance_seed_none_blocked(self):
        """New Random() instances use _RestrictedRandom which blocks seed(None)."""
        _raises_in_sandbox("import random; random.Random().seed()")

    def test_random_seed_explicit_allowed(self):
        _run_in_sandbox("import random; random.seed(42)")

    def test_random_deterministic(self):
        """Same seed should produce the same sequence."""
        ns1 = _run_in_sandbox("import random; result = [random.random() for _ in range(5)]")
        ns2 = _run_in_sandbox("import random; result = [random.random() for _ in range(5)]")
        assert ns1["result"] == ns2["result"]

    def test_random_different_seeds(self):
        """Different seeds should produce different sequences."""
        ns1 = _run_in_sandbox(
            "import random; result = [random.random() for _ in range(5)]",
            seed="seed-a",
        )
        ns2 = _run_in_sandbox(
            "import random; result = [random.random() for _ in range(5)]",
            seed="seed-b",
        )
        assert ns1["result"] != ns2["result"]

    def test_random_randint_deterministic(self):
        ns1 = _run_in_sandbox(
            "import random; result = [random.randint(0, 1000) for _ in range(10)]"
        )
        ns2 = _run_in_sandbox(
            "import random; result = [random.randint(0, 1000) for _ in range(10)]"
        )
        assert ns1["result"] == ns2["result"]

    def test_random_instance_seed_none_blocked(self):
        _raises_in_sandbox("import random; r = random.Random(); r.seed()")

    def test_random_instance_seed_explicit_allowed(self):
        ns = _run_in_sandbox("import random; r = random.Random(42); result = r.random()")
        assert isinstance(ns["result"], float)

    def test_host_random_not_affected(self):
        """Sandbox should not modify the host random state."""
        import random

        state_before = random.getstate()
        _run_in_sandbox("import random; random.random()")
        state_after = random.getstate()
        assert state_before == state_after


# ═══════════════════════════════════════════════════════════════
#  threading restrictions
# ═══════════════════════════════════════════════════════════════


class TestThreadingRestrictions:
    def test_thread_blocked(self):
        _raises_in_sandbox("import threading; threading.Thread(target=lambda: None).start()")

    def test_timer_blocked(self):
        _raises_in_sandbox("import threading; threading.Timer(0, lambda: None).start()")

    def test_settrace_blocked(self):
        _raises_in_sandbox("import threading; threading.settrace(lambda *a: None)")

    def test_setprofile_blocked(self):
        _raises_in_sandbox("import threading; threading.setprofile(lambda *a: None)")

    def test_lock_allowed(self):
        _run_in_sandbox("import threading; lock = threading.Lock(); lock.acquire(); lock.release()")

    def test_event_allowed(self):
        _run_in_sandbox("import threading; e = threading.Event(); e.set(); e.is_set()")

    def test_current_thread_allowed(self):
        _run_in_sandbox("import threading; threading.current_thread()")


# ═══════════════════════════════════════════════════════════════
#  asyncio restrictions + loop proxy
# ═══════════════════════════════════════════════════════════════


class TestAsyncioRestrictions:
    def test_asyncio_imports(self):
        """asyncio should be importable inside the sandbox."""
        _run_in_sandbox("import asyncio")

    @pytest.mark.asyncio
    async def test_loop_call_later_blocked(self):
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            loop = asyncio.get_running_loop()
            with pytest.raises(SandboxRestrictionError, match="loop.call_later"):
                loop.call_later(0, lambda: None)

    @pytest.mark.asyncio
    async def test_loop_call_at_blocked(self):
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            loop = asyncio.get_running_loop()
            with pytest.raises(SandboxRestrictionError, match="loop.call_at"):
                loop.call_at(0, lambda: None)

    @pytest.mark.asyncio
    async def test_loop_create_connection_blocked(self):
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            loop = asyncio.get_running_loop()
            with pytest.raises(SandboxRestrictionError, match="loop.create_connection"):
                loop.create_connection(None, "localhost", 80)

    @pytest.mark.asyncio
    async def test_loop_subprocess_exec_blocked(self):
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            loop = asyncio.get_running_loop()
            with pytest.raises(SandboxRestrictionError, match="loop.subprocess_exec"):
                loop.subprocess_exec(None, "echo")

    @pytest.mark.asyncio
    async def test_loop_subprocess_shell_blocked(self):
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            loop = asyncio.get_running_loop()
            with pytest.raises(SandboxRestrictionError, match="loop.subprocess_shell"):
                loop.subprocess_shell(None, "echo")

    @pytest.mark.asyncio
    async def test_loop_call_soon_allowed(self):
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            loop = asyncio.get_running_loop()
            called = []
            loop.call_soon(called.append, 1)
            await asyncio.sleep(0)  # yield to let call_soon fire
            # call_soon should not raise — we can't easily assert it fired
            # because asyncio.sleep is going through the real loop, but
            # the important thing is call_soon didn't raise.

    @pytest.mark.asyncio
    async def test_loop_create_future_allowed(self):
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            loop = asyncio.get_running_loop()
            fut = loop.create_future()
            assert not fut.done()

    @pytest.mark.asyncio
    async def test_loop_create_task_allowed(self):
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            async def noop():
                pass

            loop = asyncio.get_running_loop()
            task = loop.create_task(noop())
            await task

    @pytest.mark.asyncio
    @pytest.mark.skipif(sys.version_info < (3, 11), reason="TaskGroup requires Python 3.11+")
    async def test_taskgroup_works_in_sandbox(self):
        """asyncio.TaskGroup must work inside the sandbox.

        Regression: TaskGroup.__aenter__ calls current_task() which
        returned None inside the sandbox, causing RuntimeError:
        'TaskGroup cannot determine the parent task'.
        """
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            results = []

            async def worker(value: int) -> None:
                results.append(value)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(worker(1))
                tg.create_task(worker(2))

            assert sorted(results) == [1, 2]

    @pytest.mark.asyncio
    @pytest.mark.skipif(sys.version_info < (3, 11), reason="TaskGroup requires Python 3.11+")
    async def test_taskgroup_cancels_siblings_on_error(self):
        """TaskGroup must correctly cancel siblings when one task fails.

        Regression: when asyncio was a plain passthrough (no restriction
        proxy), CancelledError inside the sandbox was a different class
        than the one used by the C _asyncio extension, so TaskGroup's
        _on_task_done failed with:
            AttributeError: 'NoneType' object has no attribute 'append'
        """
        with workflow_sandbox(random_seed=SEED):
            import asyncio

            cancelled = asyncio.Event()

            async def failing():
                raise ValueError("boom")

            async def slow():
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    cancelled.set()
                    raise

            with pytest.raises(ExceptionGroup) as exc_info:  # noqa: F821
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(slow())
                    tg.create_task(failing())

            assert len(exc_info.value.exceptions) == 1
            assert isinstance(exc_info.value.exceptions[0], ValueError)
            assert cancelled.is_set()


# ═══════════════════════════════════════════════════════════════
#  asyncio loop proxy with uvloop
# ═══════════════════════════════════════════════════════════════


class TestUvloopProxy:
    """Loop proxy must work with uvloop (C-based event loop)."""

    @pytest.fixture(autouse=True)
    def _use_uvloop(self):
        """Run every test in this class on uvloop."""
        uvloop = pytest.importorskip("uvloop")
        loop = uvloop.new_event_loop()
        yield loop
        loop.close()

    def _run_async(self, coro, loop):
        return loop.run_until_complete(coro)

    def test_uvloop_call_later_blocked(self, _use_uvloop):
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                proxy_loop = asyncio.get_running_loop()
                with pytest.raises(SandboxRestrictionError, match="loop.call_later"):
                    proxy_loop.call_later(0, lambda: None)

        self._run_async(go(), loop)

    def test_uvloop_call_at_blocked(self, _use_uvloop):
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                proxy_loop = asyncio.get_running_loop()
                with pytest.raises(SandboxRestrictionError, match="loop.call_at"):
                    proxy_loop.call_at(0, lambda: None)

        self._run_async(go(), loop)

    def test_uvloop_create_connection_blocked(self, _use_uvloop):
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                proxy_loop = asyncio.get_running_loop()
                with pytest.raises(SandboxRestrictionError, match="loop.create_connection"):
                    proxy_loop.create_connection(None, "localhost", 80)

        self._run_async(go(), loop)

    def test_uvloop_subprocess_exec_blocked(self, _use_uvloop):
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                proxy_loop = asyncio.get_running_loop()
                with pytest.raises(SandboxRestrictionError, match="loop.subprocess_exec"):
                    proxy_loop.subprocess_exec(None, "echo")

        self._run_async(go(), loop)

    def test_uvloop_call_soon_allowed(self, _use_uvloop):
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                proxy_loop = asyncio.get_running_loop()
                called = []
                proxy_loop.call_soon(called.append, 1)
                await asyncio.sleep(0)
                # call_soon should not raise

        self._run_async(go(), loop)

    def test_uvloop_create_task_allowed(self, _use_uvloop):
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                async def noop():
                    pass

                proxy_loop = asyncio.get_running_loop()
                task = proxy_loop.create_task(noop())
                await task

        self._run_async(go(), loop)

    def test_uvloop_create_future_allowed(self, _use_uvloop):
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                proxy_loop = asyncio.get_running_loop()
                fut = proxy_loop.create_future()
                assert not fut.done()

        self._run_async(go(), loop)

    def test_uvloop_time_blocked(self, _use_uvloop):
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                proxy_loop = asyncio.get_running_loop()
                with pytest.raises(SandboxRestrictionError, match="loop.time"):
                    proxy_loop.time()

        self._run_async(go(), loop)

    def test_uvloop_proxy_wraps_real_loop(self, _use_uvloop):
        """Proxy loop should delegate allowed attrs to the real uvloop."""
        loop = _use_uvloop

        async def go():
            with workflow_sandbox(random_seed=SEED):
                import asyncio

                proxy_loop = asyncio.get_running_loop()
                assert proxy_loop.is_running()
                assert not proxy_loop.is_closed()

        self._run_async(go(), loop)


# ═══════════════════════════════════════════════════════════════
#  blocked modules (stub modules)
# ═══════════════════════════════════════════════════════════════


class TestBlockedModules:
    def test_subprocess_importable_but_calls_blocked(self):
        """subprocess imports as a stub; calling anything raises."""
        _run_in_sandbox("import subprocess")
        _raises_in_sandbox("import subprocess; subprocess.run(['echo'])")
        _raises_in_sandbox("import subprocess; subprocess.Popen(['echo'])")

    def test_ctypes_blocked(self):
        _run_in_sandbox("import ctypes")
        _raises_in_sandbox("import ctypes; ctypes.CDLL('libc.so.6')")

    def test_multiprocessing_blocked(self):
        _run_in_sandbox("import multiprocessing")
        _raises_in_sandbox("import multiprocessing; multiprocessing.Process()")

    def test_signal_blocked(self):
        _run_in_sandbox("import signal")
        _raises_in_sandbox("import signal; signal.signal(2, lambda *a: None)")

    def test_ssl_blocked(self):
        _run_in_sandbox("import ssl")
        _raises_in_sandbox("import ssl; ssl.create_default_context()")

    def test_sqlite3_blocked(self):
        _run_in_sandbox("import sqlite3")
        _raises_in_sandbox("import sqlite3; sqlite3.connect(':memory:')")

    def test_mmap_blocked(self):
        _run_in_sandbox("import mmap")

    def test_fcntl_blocked(self):
        _run_in_sandbox("import fcntl")

    def test_pty_blocked(self):
        _run_in_sandbox("import pty")

    def test_readline_blocked(self):
        _run_in_sandbox("import readline")


# ═══════════════════════════════════════════════════════════════
#  passthrough modules
# ═══════════════════════════════════════════════════════════════


class TestPassthroughModules:
    """Passthrough modules should be the exact same object as the host."""

    @pytest.mark.parametrize(
        "mod",
        [
            "json",
            "re",
            "math",
            "hashlib",
            "base64",
            "collections",
            "itertools",
            "functools",
            "typing",
            "dataclasses",
            "decimal",
            "enum",
            "copy",
            "io",
            "zlib",
            "abc",
            "pprint",
        ],
    )
    def test_passthrough_identity(self, mod: str):
        host_mod = sys.modules.get(mod) or __import__(mod)
        ns = _run_in_sandbox(f"import {mod}; result = {mod}")
        assert ns["result"] is host_mod

    def test_json_works(self):
        ns = _run_in_sandbox("import json; result = json.loads('{\"a\": 1}')")
        assert ns["result"] == {"a": 1}

    def test_re_works(self):
        ns = _run_in_sandbox("import re; result = re.findall(r'\\d+', 'abc123def456')")
        assert ns["result"] == ["123", "456"]

    def test_math_works(self):
        ns = _run_in_sandbox("import math; result = math.sqrt(16)")
        assert ns["result"] == 4.0

    def test_shutil_works(self):
        _run_in_sandbox("import os; lurr = os.supports_dir_fd")
        _run_in_sandbox("import shutil")

    def test_pathlib_works(self):
        ns = _run_in_sandbox("import pathlib; result = isinstance(0, pathlib.Path)")
        assert not ns["result"]

    def test_collections_counter(self):
        ns = _run_in_sandbox("from collections import Counter; result = dict(Counter('aabbc'))")
        assert ns["result"] == {"a": 2, "b": 2, "c": 1}

    def test_encodings_passthrough_shares_host_codec_modules(self):
        """Regression: the C codec machinery imports ``encodings.<codec>``
        on first use of an encoding and pins the module — plus a freshly
        re-registered ``search_function`` — in interpreter state that can
        never be cleared.  With ``encodings`` sandboxed, every first-use
        codec lookup inside a run leaked a private copy of the package
        (``encodings.aliases`` and the codec module); passthrough reuses
        the host's modules so nothing new is pinned."""
        import encodings
        import importlib

        with workflow_sandbox(random_seed="enc"):
            # Exercise the C lookup path
            assert b"\x81".decode("cp1006")
            mod = sys.modules["encodings.cp1006"]
            seen = {
                name: module
                for name, module in sys.modules.items()
                if name == "encodings" or name.startswith("encodings.")
            }

        assert seen["encodings"] is encodings
        assert seen["encodings.cp1006"] is mod
        for name, module in seen.items():
            assert sys.modules[name] is module

    def test_passthrough_submodule_not_loaded_in_host(self, tmp_path):
        """A submodule of a passthrough package that the host has not
        imported yet must be imported into the host and shared — a fresh
        sandbox import would be grafted onto the shared parent package,
        mutating the host."""
        import importlib

        from vercel._internal.workflow import py_sandbox

        pkg = tmp_path / "pt_pkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("")
        (pkg / "sub.py").write_text("value = 7\n")
        sys.path.insert(0, str(tmp_path))
        py_sandbox._PASSTHROUGHS.add("pt_pkg")
        try:
            host_pkg = importlib.import_module("pt_pkg")
            assert "pt_pkg.sub" not in sys.modules
            with workflow_sandbox(random_seed=SEED):
                sand_pkg = importlib.import_module("pt_pkg")
                sub = importlib.import_module("pt_pkg.sub")
                assert sand_pkg is host_pkg
            assert sys.modules["pt_pkg.sub"] is sub
            assert host_pkg.sub is sub
        finally:
            py_sandbox._PASSTHROUGHS.discard("pt_pkg")
            sys.path.remove(str(tmp_path))
            sys.modules.pop("pt_pkg", None)
            sys.modules.pop("pt_pkg.sub", None)

# ═══════════════════════════════════════════════════════════════
#  SandboxPolicy.passthrough_modules
# ═══════════════════════════════════════════════════════════════


class TestPolicyPassthroughModules:
    """SandboxPolicy.passthrough_modules extends the built-in passthrough set."""

    def test_extra_module_shared_from_host(self):
        import shlex as host_shlex

        policy = SandboxPolicy(passthrough_modules=frozenset({"shlex"}))
        with workflow_sandbox(random_seed=SEED, policy=policy):
            import shlex

            assert shlex is host_shlex

    def test_covers_submodules(self):
        import urllib.parse as host_parse

        policy = SandboxPolicy(passthrough_modules=frozenset({"urllib"}))
        with workflow_sandbox(random_seed=SEED, policy=policy):
            import urllib.parse

            assert urllib.parse is host_parse

    def test_scoped_to_policy(self):
        import shlex as host_shlex

        policy = SandboxPolicy(passthrough_modules=frozenset({"shlex"}))
        with workflow_sandbox(random_seed=SEED, policy=policy):
            import shlex as inside_with_policy
        with workflow_sandbox(random_seed=SEED):
            import shlex as inside_plain

        assert inside_with_policy is host_shlex
        assert inside_plain is not host_shlex


# ═══════════════════════════════════════════════════════════════
#  module isolation
# ═══════════════════════════════════════════════════════════════


class TestModuleIsolation:
    def test_fresh_import_each_sandbox(self):
        """Non-passthrough modules should be freshly imported each time."""
        _run_in_sandbox(
            "import asyncio; "
            "assert 'asyncio' not in __builtins__.__dict__ "
            "if hasattr(__builtins__, '__dict__') else True"
        )

    def test_sandbox_imports_do_not_leak_to_host(self):
        """Imports inside a sandbox land in the run's private table, never the
        host's real module table — the restricted proxy must not replace the
        host's real module."""
        import datetime as real_datetime

        _run_in_sandbox("import datetime; _ = datetime.timedelta")
        # Seen from outside any sandbox, the host module is the real one.
        assert sys.modules["datetime"] is real_datetime

    def test_finder_installed_once_and_inert_outside_sandbox(self):
        """The finder is installed permanently (not per-run) and is a no-op
        outside a sandbox, so ordinary imports are unaffected."""
        import importlib

        from vercel._internal.workflow import py_sandbox

        _run_in_sandbox("pass")  # ensure installed
        finders = [f for f in sys.meta_path if isinstance(f, py_sandbox._SandboxFinder)]
        assert len(finders) == 1
        # Outside the sandbox the finder returns None, so a normal import works.
        assert importlib.import_module("string") is sys.modules["string"]

    def test_sandbox_classes_not_pinned_by_host_typing_caches(self):
        """Regression: ``typing`` is passthrough, so subscripting a generic
        with a sandbox-defined class (``List[C]`` — done implicitly by
        pydantic when processing annotations) inserts a strong reference to
        the class into host typing's lru_caches.  That pinned each run's
        entire module graph until LRU eviction (~128 dead runs).  The
        ``clear_typing_caches`` handler clears those caches on exit."""
        import gc
        import weakref

        def make_ref(seed: str) -> weakref.ref:
            with workflow_sandbox(random_seed=seed, policy=CLEANUP_ALL):
                ns: dict = {"__builtins__": sys.modules["builtins"]}
                exec(  # noqa: S102
                    "import typing\n"
                    "import weakref\n"
                    "class C:\n"
                    "    pass\n"
                    "alias = typing.List[C]\n"
                    "ref = weakref.ref(C)\n",
                    ns,
                )
                return ns["ref"]

        refs = [make_ref(f"seed-{i}") for i in range(3)]
        gc.collect()
        assert [r() for r in refs] == [None, None, None]

    def test_sandbox_classes_not_pinned_by_pydantic_generics_cache(self):
        """Regression: pydantic's ``_GENERIC_TYPES_CACHE`` is a
        WeakValueDictionary whose keys strongly reference the parametrizing
        classes.  The module-global binding of the parametrized model closes
        a strong path from key to value through the cache, so entries never
        self-evict and every run leaks its classes (and, through their
        ``__globals__``, its module graph).  The
        ``clear_pydantic_generics_cache`` handler clears it on exit."""
        import gc
        import weakref

        code = (
            "import typing\n"
            "import weakref\n"
            "import pydantic\n"
            "T = typing.TypeVar('T')\n"
            "class Param(pydantic.BaseModel):\n"
            "    pass\n"
            "class Gen(pydantic.BaseModel, typing.Generic[T]):\n"
            "    x: typing.Optional[T] = None\n"
            "alias = Gen[Param]\n"
            "ref = weakref.ref(Param)\n"
        )

        def make_ref(seed: str) -> weakref.ref:
            with workflow_sandbox(random_seed=seed, policy=CLEANUP_ALL):
                ns: dict = {"__builtins__": sys.modules["builtins"]}
                exec(code, ns)  # noqa: S102
                return ns["ref"]

        refs = [make_ref(f"seed-{i}") for i in range(3)]
        gc.collect()
        assert [r() for r in refs] == [None, None, None]

    @pytest.mark.parametrize("end_barrier", [True, False])
    @pytest.mark.parametrize("start_barrier", [True, False])
    def test_concurrent_thread_sandboxes_have_independent_module_tables(
        self, tmp_path, start_barrier: bool, end_barrier: bool
    ):
        """Regression: two workflow runs on different threads must each get a
        private module table, so a module that mutates a shared singleton on
        import (e.g. a workflow registry) re-imports cleanly per run instead of
        colliding (the "Duplicate step name" failure).

        Reproduces the cross-thread sys.modules race that the global
        clear()/restore design could not survive. Run across the four
        combinations of barriers at sandbox entry and exit, so isolation holds
        whether or not the two sandboxes overlap in time."""
        import importlib
        import textwrap
        import threading

        pkgdir = tmp_path / "regpkg"
        pkgdir.mkdir()
        (pkgdir / "__init__.py").write_text("registry = {}\n")
        (pkgdir / "mod.py").write_text(
            textwrap.dedent("""
                from regpkg import registry
                name = "step//regpkg.mod.work"
                assert name not in registry, "Duplicate step name: " + name
                registry[name] = 1
                """)
        )
        d = str(tmp_path)
        sys.path.insert(0, d)

        barrier = threading.Barrier(2)
        results: dict[str, dict[str, int]] = {}
        errors: list[str] = []

        def run(seed: str) -> None:
            try:
                with workflow_sandbox(random_seed=seed):
                    if start_barrier:
                        barrier.wait()
                    importlib.import_module("regpkg.mod")
                    pkg = importlib.import_module("regpkg")
                    results[seed] = pkg.registry
                    if end_barrier:
                        barrier.wait()
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc))
                # Release a partner waiting on a barrier this thread won't reach.
                barrier.abort()

        try:
            t1 = threading.Thread(target=run, args=("a",))
            t2 = threading.Thread(target=run, args=("b",))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        finally:
            sys.path.remove(d)

        assert not errors, errors
        # Each run imported into its own table -> its own registry instance,
        # each holding exactly the single registration (no duplicate).
        assert results["a"] is not results["b"]
        assert results["a"] == results["b"] == {"step//regpkg.mod.work": 1}


# ═══════════════════════════════════════════════════════════════
#  sandbox policy / teardown cleanups (vercel.workflow.sandbox)
# ═══════════════════════════════════════════════════════════════


class TestSandboxPolicy:
    def test_cleanup_handler_gets_run_module_snapshot(self):
        """Handlers run once on teardown and see the run's module table."""
        calls: list[SandboxCleanupContext] = []
        policy = SandboxPolicy(cleanups=(calls.append,))

        with workflow_sandbox(random_seed=SEED, policy=policy):
            import shlex  # noqa: F401

            assert not calls  # not during the run
        (context,) = calls
        assert "shlex" in context.run_modules

    def test_cleanup_handlers_run_when_body_raises(self):
        calls: list[SandboxCleanupContext] = []
        policy = SandboxPolicy(cleanups=(calls.append,))

        with pytest.raises(ValueError, match="boom"):  # noqa: PT012
            with workflow_sandbox(random_seed=SEED, policy=policy):
                raise ValueError("boom")
        assert len(calls) == 1

    def test_raising_handler_is_isolated(self, caplog):
        """A failing handler is logged; later handlers still run and the
        sandbox exits cleanly."""

        def bad(context: SandboxCleanupContext) -> None:
            raise RuntimeError("handler broke")

        calls: list[SandboxCleanupContext] = []
        policy = SandboxPolicy(cleanups=(bad, calls.append))

        with caplog.at_level("ERROR"):
            with workflow_sandbox(random_seed=SEED, policy=policy):
                pass
        assert len(calls) == 1
        assert any("cleanup handler" in r.message for r in caplog.records)

    def test_no_cleanups_by_default(self):
        """The default policy runs no handlers, so host typing caches pin
        the run's classes — the pinning ``ALL_CLEANUPS`` opts out of."""
        import gc
        import weakref

        from vercel._internal.workflow.py_sandbox import clear_typing_caches

        with workflow_sandbox(random_seed=SEED):
            ns: dict = {"__builtins__": sys.modules["builtins"]}
            exec(  # noqa: S102
                "import typing\nimport weakref\n"
                "class C:\n    pass\n"
                "alias = typing.List[C]\n"
                "ref = weakref.ref(C)\n",
                ns,
            )
            ref: weakref.ref = ns["ref"]
        del ns
        gc.collect()
        try:
            assert ref() is not None
        finally:
            # Don't leave the pinned run behind for other tests.
            clear_typing_caches(SandboxCleanupContext(run_modules={}))
        gc.collect()
        assert ref() is None

    def test_workflows_threads_policy_to_registry(self):
        from vercel._internal.workflow import core

        policy = SandboxPolicy(cleanups=())
        registry = core.Workflows(as_vercel_job=False, sandbox_policy=policy)
        assert registry._sandbox_policy is policy
        assert core.Workflows(as_vercel_job=False)._sandbox_policy == SandboxPolicy()


# ═══════════════════════════════════════════════════════════════
#  workflow_sandbox API
# ═══════════════════════════════════════════════════════════════


class TestWorkflowSandboxAPI:
    def test_random_seed_required(self):
        with pytest.raises(TypeError):
            with workflow_sandbox():  # type: ignore[call-arg]
                pass

    def test_random_seed_must_be_str(self):
        with pytest.raises(TypeError):
            with workflow_sandbox(random_seed=None):  # type: ignore[arg-type]
                pass
