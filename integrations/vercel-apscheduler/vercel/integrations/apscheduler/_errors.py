"""Errors raised by the integration."""

from __future__ import annotations

__all__ = ["APSchedulerConfigurationError"]


class APSchedulerConfigurationError(RuntimeError):
    """Raised when a scheduler uses something a Vercel schedule can't run."""
