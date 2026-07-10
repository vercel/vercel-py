"""APScheduler integration for Vercel Queues."""

from ._adapter import (
    PublishedWakeup,
    SchedulerAdapter,
    WakeupProcessingResult,
    adopt_scheduler,
    install_vercel_apscheduler_integration,
    maybe_auto_install_from_env,
    seed_next_wakeup,
)
from ._executor import VercelInlineExecutor
from ._options import VercelAPSchedulerOptions
from ._payload import MemoryCursor, WakeupPayload
from ._subscriber import get_asgi_app, register_scheduler
from .version import __version__

__all__ = [
    "MemoryCursor",
    "PublishedWakeup",
    "SchedulerAdapter",
    "VercelAPSchedulerOptions",
    "VercelInlineExecutor",
    "WakeupPayload",
    "WakeupProcessingResult",
    "__version__",
    "adopt_scheduler",
    "get_asgi_app",
    "install_vercel_apscheduler_integration",
    "maybe_auto_install_from_env",
    "register_scheduler",
    "seed_next_wakeup",
]
