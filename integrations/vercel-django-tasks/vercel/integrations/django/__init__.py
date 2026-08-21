"""Django task integration for Vercel Queue Service."""

import sys

if sys.version_info >= (3, 12):
    pass
else:  # pragma: no cover - dependency marker mirrors this gate.
    raise RuntimeError(
        "vercel.integrations.django requires Python 3.12 or newer because Django 6 "
        "does not support earlier Python versions."
    )

from ._backend import (
    VercelQueuesBackend,
    install_vercel_django_task_integration,
)
from .version import __version__

__all__ = [
    "VercelQueuesBackend",
    "__version__",
    "install_vercel_django_task_integration",
]
