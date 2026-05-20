"""Curated facade for the experimental Vercel SDK surface."""

from __future__ import annotations

from vercel._internal.unstable.default import (
    get_default_session,
    setup_default_session,
    use_session,
)
from vercel._internal.unstable.errors import VercelError
from vercel._internal.unstable.sandbox_proxy import sandbox
from vercel._internal.unstable.session import Session, SessionOptions, SyncSession

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
