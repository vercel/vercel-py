from __future__ import annotations

import builtins
import contextvars
import dataclasses
import datetime as _dt
import importlib
import os
import random
import sys
import threading
import types
import weakref
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from contextlib import contextmanager
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from importlib.util import spec_from_loader
from typing import Any, NoReturn

# When True, proxy modules enforce restrictions.  When False (default),
# attribute access on proxy modules falls through to the real module.
# This allows concurrent coroutines that are NOT running a workflow to
# use the real module even while the sandbox has replaced sys.modules.
_in_sandbox: contextvars.ContextVar[bool] = contextvars.ContextVar("_in_sandbox", default=False)

# The real, process-wide sys.modules dict, captured before we install the
# dispatching proxy below.  Any context that is not running a workflow reads
# and writes this dict directly, so non-workflow code is unaffected.
_real_sys_modules: dict[str, types.ModuleType] = sys.modules

# Per-execution module table.  A workflow run sets this to its own private dict
# (see workflow_sandbox) so concurrent runs — whether on different asyncio
# tasks or different threads — never share or clobber each other's modules.
# ``None`` means "use the real table".
_sandbox_sys_modules: contextvars.ContextVar[dict[str, types.ModuleType] | None] = (
    contextvars.ContextVar("_sandbox_sys_modules", default=None)
)


class SandboxRestrictionError(RuntimeError):
    """Raised when workflow code calls a non-deterministic function."""


# TODO: We should have a more proper proxy that blocks __call__ and
# returns proxied members but otherwise looks the same.
def _restricted(name: str) -> Callable[..., NoReturn]:
    def _raise(*_args: Any, **_kwargs: Any) -> NoReturn:
        raise SandboxRestrictionError(
            f"Cannot call {name}() inside a workflow. Workflows must be deterministic."
        )

    _raise.__qualname__ = f"<restricted {name}>"
    return _raise


@dataclasses.dataclass(frozen=True)
class _ModulePolicy:
    module_name: str
    overrides: dict[str, Any] = dataclasses.field(default_factory=dict)
    drops: frozenset[str] = dataclasses.field(default_factory=frozenset)
    allowed: frozenset[str] | None = None
    allow_if: Callable[[str], bool] | None = None

    def post_exec(self, *, proxy: _ProxyModule, module: types.ModuleType) -> None:
        pass

    def resolve_attr(self, name: str, real: types.ModuleType) -> Any:
        """Resolve an allowed attribute on the real module.

        Called by ``_ProxyModule.__getattr__`` as the final fallback.
        Subclasses can override to intercept (e.g. per-context random).
        """
        return getattr(real, name)


def _context_restricted(name: str, real_fn: Any) -> Callable[..., Any]:
    """Like ``_restricted`` but falls through to *real_fn* outside sandbox context.

    Used for builtins overrides where CPython's ``LOAD_GLOBAL`` reads
    ``__dict__`` directly, bypassing ``__getattr__``.
    """

    def _wrapper(*_args: Any, **_kwargs: Any) -> Any:
        if _in_sandbox.get(False):
            raise SandboxRestrictionError(
                f"Cannot call {name}() inside a workflow. Workflows must be deterministic."
            )
        return real_fn(*_args, **_kwargs)

    _wrapper.__qualname__ = f"<workflow-context-restricted {name}>"
    return _wrapper


def _blocklist(
    module: str, *attrs: str, drops: list[str] | None = None, **overrides: Any
) -> _ModulePolicy:
    """Restrict specific attributes; everything else passes through."""
    d = {attr: _restricted(f"{module}.{attr}") for attr in attrs}
    d.update(overrides)
    return _ModulePolicy(module_name=module, overrides=d, drops=frozenset(drops or []))


def _allowlist(
    module: str,
    *attrs: str,
    allow_if: Callable[[str], bool] | None = None,
    drops: list[str] | None = None,
    **overrides: Any,
) -> _ModulePolicy:
    """Allow only the listed attributes; everything else is restricted."""
    return _ModulePolicy(
        module_name=module,
        overrides=overrides,
        allowed=frozenset(attrs),
        drops=frozenset(drops or []),
        allow_if=allow_if,
    )


class _RestrictedDatetimeMeta(type):
    def __instancecheck__(cls, instance: Any) -> bool:
        return isinstance(instance, _dt.datetime)

    def __subclasscheck__(cls, subclass: type) -> bool:
        return issubclass(subclass, _dt.datetime)


class _RestrictedDatetime(_dt.datetime, metaclass=_RestrictedDatetimeMeta):
    @classmethod  # type: ignore[override]
    def now(cls, tz: _dt.timezone | None = None) -> NoReturn:  # type: ignore[override]
        _restricted("datetime.datetime.now")()

    @classmethod  # type: ignore[override]
    def utcnow(cls) -> NoReturn:  # type: ignore[override]
        _restricted("datetime.datetime.utcnow")()


class _RestrictedDateMeta(type):
    def __instancecheck__(cls, instance: Any) -> bool:
        return isinstance(instance, _dt.date)

    def __subclasscheck__(cls, subclass: type) -> bool:
        return issubclass(subclass, _dt.date)


class _RestrictedDate(_dt.date, metaclass=_RestrictedDateMeta):
    @classmethod  # type: ignore[override]
    def today(cls) -> NoReturn:  # type: ignore[override]
        _restricted("datetime.date.today")()


class _RestrictedRandomMeta(type):
    def __instancecheck__(cls, instance: Any) -> bool:
        return isinstance(instance, random.Random)

    def __subclasscheck__(cls, subclass: type) -> bool:
        return issubclass(subclass, random.Random)


class _RestrictedRandom(random.Random, metaclass=_RestrictedRandomMeta):
    def seed(self, a=None, **kwargs):
        if a is None:
            _restricted("random.Random.seed")()
        super().seed(a, **kwargs)


# Per-sandbox Random instance, so concurrent sandboxes with different
# seeds don't corrupt each other's random state.
_sandbox_random: contextvars.ContextVar[random.Random | None] = contextvars.ContextVar(
    "_sandbox_random", default=None
)


class _RestrictedRandomPolicy(_ModulePolicy):
    def __init__(self) -> None:
        super().__init__("random", overrides={"Random": _RestrictedRandom})

    def resolve_attr(self, name: str, real: types.ModuleType) -> Any:
        inst = _sandbox_random.get(None)
        if inst is not None:
            method = getattr(inst, name, None)
            if method is not None:
                return method
        return getattr(real, name)


def _wrap_get_loop(real_fn: Callable[..., Any]) -> Callable[..., Any]:
    cache: weakref.WeakKeyDictionary[Any, Any] = weakref.WeakKeyDictionary()

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        real_loop = real_fn(*args, **kwargs)
        if real_loop in cache:
            return cache[real_loop]

        real_loop_cls = type(real_loop)

        class _LoopProxyMeta(type):
            def __instancecheck__(cls, instance: Any) -> bool:
                return isinstance(instance, real_loop_cls)

            def __subclasscheck__(cls, subclass: type) -> bool:
                return issubclass(subclass, real_loop_cls)

        _LOOP_ALLOWED: frozenset[str] = frozenset(
            {
                # Core event loop lifecycle
                "run_forever",
                "run_until_complete",
                "stop",
                "close",
                "is_running",
                "is_closed",
                "shutdown_asyncgens",
                "shutdown_default_executor",
                # Deterministic scheduling
                "call_soon",
                # Task / future creation
                "create_future",
                "create_task",
                "set_task_factory",
                "get_task_factory",
                # Exception handling
                "get_exception_handler",
                "set_exception_handler",
                "default_exception_handler",
                "call_exception_handler",
                # Debug
                "get_debug",
                "set_debug",
                # Timer handle cancellation (internal)
                "_timer_handle_cancelled",
            }
        )

        class _LoopProxy(metaclass=_LoopProxyMeta):
            """Wraps an event loop; only allowlisted methods pass through."""

            def __init__(self, real: Any) -> None:
                self._real = real

            def __getattr__(self, name: str) -> Any:
                if name.startswith("__") and name.endswith("__"):
                    return getattr(self._real, name)
                if name in _LOOP_ALLOWED:
                    return getattr(self._real, name)
                return _restricted(f"loop.{name}")

            def __hash__(self) -> int:
                return hash(self._real)

            def __eq__(self, other: object) -> bool:
                real = self._real
                if hasattr(other, "_real"):
                    return real is other._real
                return real is other

            def __repr__(self) -> str:
                return f"<proxy for {self._real!r}>"

        rv = _LoopProxy(real_loop)
        cache[real_loop] = rv
        return rv

    return wrapper


class _RestrictedAsyncioPolicy(_ModulePolicy):
    """Wraps get_running_loop/get_event_loop to return a _LoopProxy."""

    def post_exec(self, *, proxy: _ProxyModule, module: types.ModuleType, **kwargs: Any) -> None:
        for attr in ("get_running_loop", "get_event_loop"):
            real_fn = getattr(module, attr, None)
            if real_fn is not None:
                proxy.__dict__[attr] = _wrap_get_loop(real_fn)

        # Wrap current_task so that passing a _LoopProxy works.
        # The C implementation uses internal identity-based lookup that
        # does not honour __hash__/__eq__, so we unwrap the proxy first.
        real_current_task = getattr(module, "current_task", None)
        if real_current_task is not None:

            def _current_task(loop: Any = None) -> Any:
                if loop is not None and hasattr(loop, "_real"):
                    loop = loop._real
                return real_current_task(loop)

            proxy.__dict__["current_task"] = _current_task


_RESTRICTIONS: dict[str, _ModulePolicy] = {
    "builtins": _blocklist("builtins", "open", "input", "breakpoint", "eval", "exec", "compile"),
    "datetime": _blocklist("datetime", datetime=_RestrictedDatetime, date=_RestrictedDate),
    "os": _allowlist(
        "os",
        "path",
        "sep",
        "altsep",
        "extsep",
        "pathsep",
        "curdir",
        "pardir",
        "devnull",
        "linesep",
        "name",
        "fsdecode",
        "fsencode",
        "fspath",
        # These are deterministic enough if the functions that change
        # them are blocked...
        "getenv",
        "getcwd",
        "_get_exports_list",
        "PathLike",
        environ=os.environ.copy(),
        allow_if=str.isupper,
        drops=["fork", "register_at_fork"],
    ),
    "random": _RestrictedRandomPolicy(),
    "time": _allowlist(
        "time",
        "mktime",
        "strptime",
        "get_clock_info",
        "clock_getres",
        "struct_time",
        allow_if=str.isupper,
    ),
    "socket": _allowlist(
        "socket",
        # byte-order conversions
        "htonl",
        "htons",
        "ntohl",
        "ntohs",
        # address conversions
        "inet_aton",
        "inet_ntoa",
        "inet_ntop",
        "inet_pton",
        # exception types (needed to catch errors from allowed code paths)
        "error",
        "gaierror",
        "herror",
        "timeout",
        # all-caps constants (AF_*, SOCK_*, SOL_*, SO_*, IPPROTO_*, etc.)
        allow_if=str.isupper,
    ),
    "asyncio": _RestrictedAsyncioPolicy("asyncio"),
    "_asyncio": _RestrictedAsyncioPolicy("asyncio.events"),
    "threading": _blocklist(
        "threading",
        # thread creation
        "Thread",
        "Timer",
        # global trace/profile hooks (affect all threads including host)
        "settrace",
        "settrace_all_threads",
        "setprofile",
        "setprofile_all_threads",
    ),
}

_BLOCKED: set[str] = {
    "subprocess",
    "ssl",  # needs socket.socket class for inheritance; network I/O
    "ctypes",  # arbitrary C calls bypass all Python-level restrictions
    "multiprocessing",  # process creation via C-level fork/exec
    # C extensions with direct syscalls that bypass Python-level restrictions
    "signal",  # process-level signal handlers
    "fcntl",  # fd operations (flock, ioctl)
    "mmap",  # map files into memory
    "sqlite3",  # direct file I/O for databases
    "pty",  # pseudo-terminal creation + fork
    "termios",  # terminal control
    "resource",  # process resource limits
    "faulthandler",  # write to arbitrary fds
    "syslog",  # write to system log
    "readline",  # terminal input
}

_PASSTHROUGHS: set[str] = {
    # Carefully selected stdlib modules that do not import any restricted modules
    "abc",
    "array",
    "ast",
    "asyncio",
    "base64",
    "binascii",
    "bisect",
    "cmath",
    "codecs",
    "collections",
    "contextvars",
    "copy",
    "copyreg",
    "csv",
    "dataclasses",
    "decimal",
    "difflib",
    "dis",
    "enum",
    "errno",
    "fractions",
    "functools",
    "graphlib",
    "hashlib",
    "heapq",
    "html",
    "io",
    "ipaddress",
    "itertools",
    "json",
    "keyword",
    "ntpath",
    "logging",
    "math",
    "numbers",
    "operator",
    "posixpath",
    "pprint",
    "quopri",
    "re",
    "statistics",
    "string",
    "stringprep",
    "struct",
    "textwrap",
    "token",
    "tomllib",
    "traceback",
    "types",
    "typing",
    "unicodedata",
    "weakref",
    "zlib",
    # C extension with per-module state (PEP 489 multi-phase init).
    # Must share the host instance so the task registry is not lost.
    "_asyncio",
    # SDK internals — must share the singleton registries, runtime, etc.
    "vercel",
    # Common third-party deps that are side-effect-free
    "pydantic",
    "pydantic_core",
    "anyio",
    "sniffio",
    "typing_extensions",
    "annotated_types",
}


class _ProxyModule(types.ModuleType):
    """A module proxy that intercepts specific attributes.

    Attribute access first checks ``policy.overrides``, then — when
    an allowlist is active — blocks anything not in the allowlist.
    Everything else falls through to the wrapped real module.

    When *copy_dict* is ``True`` (needed for ``builtins``), the real
    module's entire ``__dict__`` is copied into the proxy so that
    CPython's ``LOAD_GLOBAL`` bytecode — which reads
    ``builtins.__dict__`` directly — sees the overridden values.
    """

    def __init__(
        self,
        real: types.ModuleType,
        policy: _ModulePolicy,
        *,
        copy_dict: bool = False,
    ) -> None:
        super().__init__(real.__name__)
        object.__setattr__(self, "_proxy_real", real)
        object.__setattr__(self, "_proxy_policy", policy)
        # Copy module metadata.
        for attr in ("__package__", "__path__", "__file__", "__spec__", "__loader__", "__doc__"):
            val = getattr(real, attr, None)
            if val is not None:
                self.__dict__[attr] = val
        if copy_dict:
            self.__dict__.update(real.__dict__)
            # For dict-based lookups (builtins), overrides must be
            # context-aware so concurrent coroutines outside the sandbox
            # can still call the real functions.
            for key, val in policy.overrides.items():
                real_fn = real.__dict__.get(key)
                if real_fn is not None and callable(val):
                    self.__dict__[key] = _context_restricted(f"{policy.module_name}.{key}", real_fn)
                else:
                    self.__dict__[key] = val
        else:
            # Overrides go into __dict__ so they are found by direct
            # dict lookup (important for builtins).
            self.__dict__.update(policy.overrides)

    def __getattr__(self, name: str) -> Any:
        real = object.__getattribute__(self, "_proxy_real")
        # Outside a sandbox context, delegate everything to the real module
        # so concurrent coroutines are not affected by the global proxy.
        if not _in_sandbox.get(False):
            return getattr(real, name)
        policy = object.__getattribute__(self, "_proxy_policy")
        if name in policy.drops:
            raise AttributeError(name)
        if name in policy.overrides:
            return policy.overrides[name]
        # Allowlist mode: block anything not explicitly allowed
        # (dunders always pass through for introspection / import machinery).
        if (
            policy.allowed is not None
            and name not in policy.allowed
            and not (name.startswith("__") and name.endswith("__"))
            and not (policy.allow_if is not None and policy.allow_if(name))
            # Only restrict callables (which will include classes).
            and callable(policy.resolve_attr(name, real))
        ):
            # Return a restricted callable instead of raising immediately.
            # This allows module init code like ``from os import urandom``
            # to succeed — the error fires when the function is *called*.
            return _restricted(f"{policy.module_name}.{name}")
        return policy.resolve_attr(name, real)

    def __repr__(self) -> str:
        real = object.__getattribute__(self, "_proxy_real")
        return f"<proxy for {real!r}>"


class _StubModule(types.ModuleType):
    """A stub module where every attribute access returns a restricted callable."""

    def __init__(self, name: str, real: types.ModuleType | None = None) -> None:
        super().__init__(name)
        object.__setattr__(self, "_stub_real", real)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        if not _in_sandbox.get(False):
            real = object.__getattribute__(self, "_stub_real")
            if real is not None:
                return getattr(real, name)
        return _restricted(f"{self.__name__}.{name}")


class _SandboxFinder(MetaPathFinder):
    """A MetaPathFinder that controls module loading inside the sandbox.

    For every ``import X`` inside the sandbox:

    1. If ``X`` **has restrictions** — return a ``_ProxyModule`` that
       intercepts the restricted attributes.
    2. If ``X`` **is already imported in the host and matches the
       passthrough set** — return the host module as-is (shared).
    3. Otherwise — return ``None`` for a fresh re-import.
    """

    def __init__(
        self,
        *,
        host_modules: dict[str, types.ModuleType],
        passthrough: set[str],
        restrictions: dict[str, _ModulePolicy] | None = None,
        blocked: set[str] | None = None,
    ) -> None:
        self._host = host_modules
        self._passthrough = passthrough
        self._restrictions = restrictions or {}
        self._blocked = blocked or set()

    def _is_passthrough(self, name: str) -> bool:
        for prefix in self._passthrough:
            if name == prefix or name.startswith(prefix + "."):
                return True
        return False

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: types.ModuleType | None = None,
    ) -> ModuleSpec | None:
        # If we aren't actually in a sandbox, defer to the normal finders.
        if not _in_sandbox.get(False):
            return None
        if fullname in self._blocked:
            # Return a stub module instead of raising — other modules may
            # ``import subprocess`` at module level but never call it.
            # Every attribute access on the stub returns a restricted callable.
            real = self._host.get(fullname)
            return spec_from_loader(
                fullname, _PreloadedLoader(_StubModule(fullname, real)), origin="blocked"
            )
        if fullname in self._restrictions:
            policy = self._restrictions[fullname]
            # If the module is also in the passthrough set and already
            # loaded in the host, wrap the existing module with a proxy
            # instead of re-importing (avoids issues with packages that
            # have complex init like asyncio).
            if fullname in self._host and self._is_passthrough(fullname):
                proxy = _ProxyModule(self._host[fullname], policy)
                policy.post_exec(
                    proxy=proxy,
                    module=self._host[fullname],
                )
                return spec_from_loader(fullname, _PreloadedLoader(proxy), origin="sandbox")
            real_spec = self._find_real_spec(fullname, path, target)
            if real_spec is None:
                raise ModuleNotFoundError(f"No module named {fullname!r}", name=fullname)
            return ModuleSpec(
                fullname,
                _RestrictedLoader(
                    real_spec,
                    self._restrictions[fullname],
                ),
                origin=real_spec.origin,
                is_package=real_spec.submodule_search_locations is not None,
            )
        if fullname in self._host and self._is_passthrough(fullname):
            # Unrestricted passthrough — serve from host as-is.
            return spec_from_loader(
                fullname,
                _PreloadedLoader(self._host[fullname]),
                origin="sandbox",
            )
        return None

    def _find_real_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: types.ModuleType | None,
    ) -> ModuleSpec | None:
        if self in sys.meta_path:
            for finder in sys.meta_path[sys.meta_path.index(self) + 1 :]:
                if hasattr(finder, "find_spec"):
                    spec = finder.find_spec(fullname, path, target)
                    if spec is not None:
                        return spec
        return None


class _RestrictedLoader(Loader):
    def __init__(self, real_spec: ModuleSpec, policy: _ModulePolicy) -> None:
        self._real_spec = real_spec
        self._policy = policy

    def create_module(self, spec: ModuleSpec) -> types.ModuleType | None:
        loader = self._real_spec.loader
        if loader is not None and hasattr(loader, "create_module"):
            return loader.create_module(self._real_spec)
        return None

    def exec_module(self, module: types.ModuleType) -> None:
        loader = self._real_spec.loader
        if loader is not None:
            loader.exec_module(module)
        proxy = sys.modules[module.__name__] = _ProxyModule(module, self._policy)
        self._policy.post_exec(proxy=proxy, module=module)


class _PreloadedLoader(Loader):
    """A Loader that returns an already-loaded module."""

    def __init__(self, module: types.ModuleType) -> None:
        self._module = module

    def create_module(self, spec: ModuleSpec) -> types.ModuleType:
        return self._module

    def exec_module(self, module: types.ModuleType) -> None:
        # Module is already fully initialized — nothing to do.
        pass


class _DispatchingSysModules(MutableMapping[str, types.ModuleType]):
    """Installed once as ``sys.modules``.

    Every read/write dispatches to the current context's module table: a
    workflow run's private dict while a sandbox is active in this context
    (asyncio task or thread), or the real process table otherwise.  The real
    dict object is never cleared, so concurrent workflows on different tasks or
    threads can neither corrupt each other nor the host — which the previous
    ``sys.modules.clear()`` approach could not guarantee.
    """

    __slots__ = ()

    @property
    def _current(self) -> dict[str, types.ModuleType]:
        table = _sandbox_sys_modules.get()
        return _real_sys_modules if table is None else table

    def __getitem__(self, key: str) -> types.ModuleType:
        return self._current[key]

    def __setitem__(self, key: str, value: types.ModuleType) -> None:
        self._current[key] = value

    def __delitem__(self, key: str) -> None:
        del self._current[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._current)

    def __len__(self) -> int:
        return len(self._current)

    def __contains__(self, key: object) -> bool:
        return key in self._current

    def __repr__(self) -> str:
        where = "sandbox table" if _sandbox_sys_modules.get() is not None else "host"
        return f"<dispatching sys.modules -> {where} ({len(self._current)} modules)>"

    # dict-only methods the import system / inspect call on sys.modules but
    # which MutableMapping does not provide.
    def copy(self) -> dict[str, types.ModuleType]:
        return self._current.copy()

    def __or__(self, other: Any) -> dict[str, types.ModuleType]:
        return self._current | other

    def __ror__(self, other: Any) -> dict[str, types.ModuleType]:
        return other | self._current

    def __ior__(self, other: Any) -> _DispatchingSysModules:
        self._current.update(other)
        return self

    @classmethod
    def fromkeys(cls, *args: Any, **kwargs: Any) -> dict[str, types.ModuleType]:
        return dict.fromkeys(*args, **kwargs)


def _install_linecache_patch() -> None:
    """Make ``traceback.format_exc()`` work inside the sandbox, permanently.

    Python 3.14 moved linecache's ``os``/``tokenize`` imports into function
    bodies (lazy imports for startup perf).  Inside the sandbox those ``import``
    statements resolve through ``sys.modules`` and pick up restricted proxies.
    We replace ``updatecache`` with a version that uses a captured host
    ``open`` and ``checkcache`` with a no-op — but only while a sandbox is
    active in this context, so non-workflow code is unaffected and the patch is
    race-free (installed once, gated on ``_in_sandbox``).
    """
    import linecache

    host_open = open
    cache = linecache.cache
    orig_updatecache = linecache.updatecache
    orig_checkcache = linecache.checkcache

    def updatecache(filename: str, module_globals: Any = None) -> list[str]:
        if not _in_sandbox.get(False):
            return orig_updatecache(filename, module_globals)
        if filename in cache:
            del cache[filename]
        try:
            with host_open(filename, "rb") as f:
                data = f.read()
            lines = data.decode("utf-8", errors="replace").splitlines(True)
        except OSError:
            return []
        cache[filename] = (len(data), 0, lines, filename)
        return lines

    def checkcache(filename: str | None = None) -> None:
        if not _in_sandbox.get(False):
            orig_checkcache(filename)
        # Inside the sandbox: no-op — files don't change during a run.

    linecache.updatecache = updatecache
    linecache.checkcache = checkcache


def _install_import_hook() -> None:
    """Route ``import`` statements through a pure Python importer while sandboxed.

    builtins.__import__ directly uses the interpreter-level modules
    dict, which is not overridden when sys.modules is changed.

    importlib has its own pure-Python reimplementation of __import__
    that *does* use sys.modules, so we redirect to that when in a
    sandbox.
    """
    real_import = builtins.__import__

    def _sandbox_import(
        name: str,
        globals: Mapping[str, object] | None = None,
        locals: Mapping[str, object] | None = None,
        fromlist: Sequence[str] | None = (),
        level: int = 0,
    ) -> types.ModuleType:
        if not _in_sandbox.get(False):
            return real_import(name, globals, locals, fromlist or (), level)
        return importlib.__import__(name, globals, locals, fromlist or (), level)

    builtins.__import__ = _sandbox_import


_install_lock = threading.Lock()
_installed = False


def _ensure_installed() -> None:
    """Install the dispatching ``sys.modules`` and the sandbox finder once.

    Both are permanent and inert outside a sandbox context, so there is no
    per-run mutation of any shared global to race on.
    """
    global _installed
    if _installed:
        return
    with _install_lock:
        if _installed:
            return
        finder = _SandboxFinder(
            host_modules=_real_sys_modules,
            passthrough=_PASSTHROUGHS,
            restrictions={k: v for k, v in _RESTRICTIONS.items() if k != "builtins"},
            blocked=_BLOCKED,
        )
        sys.meta_path.insert(0, finder)
        _install_linecache_patch()
        _install_import_hook()
        sys.modules = _DispatchingSysModules()  # type: ignore[assignment]
        _installed = True


def _new_sandbox_table() -> dict[str, types.ModuleType]:
    """A fresh per-run module table seeded with the bootstrap essentials.

    Everything else is served on demand by ``_SandboxFinder`` (passthrough
    from the host, a restricted proxy, or a fresh re-import into this table).
    """
    table: dict[str, types.ModuleType] = {"sys": sys}
    # Snapshot atomically (list() over the view is a single C op) so a
    # concurrent import in another thread can't trip "dict changed size".
    for key, mod in list(_real_sys_modules.items()):
        if key == "importlib" or key.startswith("importlib."):
            table[key] = mod
    builtins_policy = _RESTRICTIONS.get("builtins")
    if builtins_policy is not None:
        table["builtins"] = _ProxyModule(
            _real_sys_modules["builtins"], builtins_policy, copy_dict=True
        )
    else:
        table["builtins"] = _real_sys_modules["builtins"]
    return table


# TODO: we probably want to support some form of sandbox caching
@contextmanager
def workflow_sandbox(*, random_seed: str) -> Iterator[None]:
    """Activate the workflow sandbox for the current context.

    Gives this context its own private ``sys.modules`` table, marks it as
    in-sandbox so proxy modules enforce restrictions, and provides a seeded
    ``Random``.  All three are ContextVars, so concurrent runs are isolated
    without touching any shared global.
    """
    if not isinstance(random_seed, str):
        raise TypeError("random_seed must be a str")

    _ensure_installed()
    table_token = _sandbox_sys_modules.set(_new_sandbox_table())
    sandbox_token = _in_sandbox.set(True)
    random_token = _sandbox_random.set(random.Random(random_seed))
    try:
        yield
    finally:
        _sandbox_random.reset(random_token)
        _in_sandbox.reset(sandbox_token)
        _sandbox_sys_modules.reset(table_token)


def in_sandbox() -> bool:
    return _in_sandbox.get()
