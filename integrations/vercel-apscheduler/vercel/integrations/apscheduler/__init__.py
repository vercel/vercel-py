"""APScheduler integration for Vercel Queues."""

from ._adapter import install_vercel_apscheduler_integration
from ._driver import APSchedulerConfigurationError
from .version import __version__

__all__ = [
    "APSchedulerConfigurationError",
    "__version__",
    "install_vercel_apscheduler_integration",
]
