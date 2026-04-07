from __future__ import annotations

import dataclasses
import datetime as _dt
import os
import random
import sys
import types
import weakref
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from importlib.util import spec_from_loader
from typing import Any, NoReturn


class SandboxRestrictionError(RuntimeError):
    """Raised when workflow code calls a non-deterministic function."""


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

    def post_exec(self, *, proxy: _ProxyModule, module: types.ModuleType, random_seed: str) -> None:
        pass


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


class _RestrictedRandomPolicy(_ModulePolicy):
    def __init__(self) -> None:
        super().__init__("random", overrides={"Random": _RestrictedRandom})

    def post_exec(self, *, proxy: _ProxyModule, module: types.ModuleType, random_seed: str) -> None:
        module.seed(random_seed)


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
        "_get_exports_list",
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
    "logging",
    "math",
    "numbers",
    "operator",
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
        # Overrides go into __dict__ so they are found by direct
        # dict lookup (important for builtins).
        self.__dict__.update(policy.overrides)

    def __getattr__(self, name: str) -> Any:
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
        ):
            # Return a restricted callable instead of raising immediately.
            # This allows module init code like ``from os import urandom``
            # to succeed — the error fires when the function is *called*.
            return _restricted(f"{policy.module_name}.{name}")
        real = object.__getattribute__(self, "_proxy_real")
        return getattr(real, name)

    def __repr__(self) -> str:
        real = object.__getattribute__(self, "_proxy_real")
        return f"<proxy for {real!r}>"


class _StubModule(types.ModuleType):
    """A stub module where every attribute access returns a restricted callable."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        return _restricted(f"{self.__name__}.{name}")


class _SandboxFinder(MetaPathFinder):
    """A MetaPathFinder that controls module loading inside the sandbox.

    For every ``import X`` inside the sandbox:

    1. If ``X`` **has restrictions** — return a ``_ProxyModule`` that
       intercepts the restricted attributes.
    2. If ``X`` **is in the host snapshot and matches the passthrough
       set** — return the host module as-is (shared).
    3. Otherwise — return ``None`` for a fresh re-import.
    """

    def __init__(
        self,
        *,
        host_modules: dict[str, types.ModuleType],
        passthrough: set[str],
        restrictions: dict[str, _ModulePolicy] | None = None,
        blocked: set[str] | None = None,
        random_seed: str,
    ) -> None:
        self._host = host_modules
        self._passthrough = passthrough
        self._restrictions = restrictions or {}
        self._blocked = blocked or set()
        self._random_seed = random_seed

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
        if fullname in self._blocked:
            # Return a stub module instead of raising — other modules may
            # ``import subprocess`` at module level but never call it.
            # Every attribute access on the stub returns a restricted callable.
            return spec_from_loader(
                fullname, _PreloadedLoader(_StubModule(fullname)), origin="blocked"
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
                    proxy=proxy, module=self._host[fullname], random_seed=self._random_seed
                )
                return spec_from_loader(fullname, _PreloadedLoader(proxy), origin="sandbox")
            real_spec = self._find_real_spec(fullname, path, target)
            if real_spec is None:
                raise ModuleNotFoundError(f"No module named {fullname!r}", name=fullname)
            return ModuleSpec(
                fullname,
                _RestrictedLoader(
                    real_spec, self._restrictions[fullname], random_seed=self._random_seed
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
    def __init__(self, real_spec: ModuleSpec, policy: _ModulePolicy, *, random_seed: str) -> None:
        self._real_spec = real_spec
        self._policy = policy
        self._random_seed = random_seed

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
        self._policy.post_exec(proxy=proxy, module=module, random_seed=self._random_seed)


class _PreloadedLoader(Loader):
    """A Loader that returns an already-loaded module."""

    def __init__(self, module: types.ModuleType) -> None:
        self._module = module

    def create_module(self, spec: ModuleSpec) -> types.ModuleType:
        return self._module

    def exec_module(self, module: types.ModuleType) -> None:
        # Module is already fully initialized — nothing to do.
        pass


@contextmanager
def workflow_sandbox(*, random_seed: str) -> Iterator[None]:
    """Activate the sandbox.

    1. Snapshots ``sys.modules`` and replaces its contents in-place
       (CPython's C-level import uses ``interp->modules`` which is
       the *same dict object* — replacing ``sys.modules`` with a
       new dict would not affect ``IMPORT_NAME`` bytecode).
    2. Installs a ``_SandboxFinder`` at the front of
       ``sys.meta_path`` — restricted modules are wrapped in
       ``_ProxyModule`` to block non-deterministic calls;
       passthrough modules are served from the host as-is.
    3. Sets fresh workflow/step registries via ContextVar.
    4. Seeds the global ``random`` module for determinism.
    5. On exit, restores everything.
    """
    if not isinstance(random_seed, str):
        raise TypeError("random_seed must be a str")

    # Snapshot the original contents so we can restore later.
    orig_modules = dict(sys.modules)

    # Build a proxy builtins whose __dict__ has the restricted
    # entries so that CPython's LOAD_GLOBAL sees them.
    builtins_policy = _RESTRICTIONS.get("builtins")
    if builtins_policy is not None:
        proxy_builtins: types.ModuleType = _ProxyModule(
            sys.modules["builtins"],
            builtins_policy,
            copy_dict=True,
        )
    else:
        proxy_builtins = sys.modules["builtins"]

    # Non-builtins restrictions are handled by the finder.
    module_restrictions = {k: v for k, v in _RESTRICTIONS.items() if k != "builtins"}
    finder = _SandboxFinder(
        host_modules=orig_modules,
        passthrough=_PASSTHROUGHS,
        restrictions=module_restrictions,
        blocked=_BLOCKED,
        random_seed=random_seed,
    )

    # Mutate sys.modules IN-PLACE so interp->modules sees the change.
    sys.modules.clear()
    sys.modules["sys"] = sys
    sys.modules["builtins"] = proxy_builtins
    sys.meta_path.insert(0, finder)
    try:
        yield
    finally:
        if sys.meta_path is not None:
            sys.meta_path.remove(finder)
        # Restore original sys.modules contents.
        sys.modules.clear()
        sys.modules.update(orig_modules)
