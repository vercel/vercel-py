"""Transitional alias for coroutine support now owned by internal core."""

from vercel.internal.core.iter_coroutine import iter_coroutine

__all__ = ["iter_coroutine"]
