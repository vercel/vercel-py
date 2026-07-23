"""Transitional aliases for HTTP retry types now owned by internal core."""

from vercel.internal.core.http.retry import RetryPolicy, SleepFn

__all__ = ["RetryPolicy", "SleepFn"]
