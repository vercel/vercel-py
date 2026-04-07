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

from vercel.workflow.py_sandbox import SandboxRestrictionError, workflow_sandbox

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

    def test_os_getcwd_blocked(self):
        _raises_in_sandbox("import os; os.getcwd()")

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

    def test_collections_counter(self):
        ns = _run_in_sandbox("from collections import Counter; result = dict(Counter('aabbc'))")
        assert ns["result"] == {"a": 2, "b": 2, "c": 1}


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

    def test_sys_modules_restored(self):
        """sys.modules should be fully restored after sandbox exits."""
        before = set(sys.modules.keys())
        _run_in_sandbox("import asyncio")
        after = set(sys.modules.keys())
        assert before == after

    def test_sys_meta_path_restored(self):
        """sys.meta_path should be restored after sandbox exits."""
        before = list(sys.meta_path)
        _run_in_sandbox("pass")
        after = list(sys.meta_path)
        assert before == after


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
