"""Curated facade for the experimental Vercel SDK surface."""

from __future__ import annotations

from importlib import import_module

from vercel._internal.unstable import sandbox_proxy as _sandbox_proxy
from vercel._internal.unstable.default import (
    get_default_session,
    setup_default_session,
    use_session,
)
from vercel._internal.unstable.errors import VercelError
from vercel._internal.unstable.session import Session, SessionOptions, SyncSession

# Load the domain submodule before binding the facade proxy so later
# `import vercel.unstable.sandbox` does not replace the curated attribute.
import_module("vercel.unstable.sandbox")

sandbox = _sandbox_proxy.sandbox

__all__ = [
    "Session",
    "SessionOptions",
    "SyncSession",
    "VercelError",
    "get_default_session",
    "sandbox",
    "setup_default_session",
    "use_session",
]
