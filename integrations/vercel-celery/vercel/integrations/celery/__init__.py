"""Celery integration for Vercel Queue Service."""

from kombu.transport import TRANSPORT_ALIASES, virtual

from celery.app import backends as celery_backends
from celery.app.defaults import DEFAULTS as CELERY_DEFAULTS

from ._broker import (
    AutoChannel,
    PollChannel,
    PushChannel,
    _install_app_finalize_hook,
    _register_existing_app_queues,
    register_celery_app_queues,
)
from ._result_backend import DEFAULT_RESULT_BACKEND_ALIAS, VercelRuntimeCacheBackend
from .version import __version__


class VercelQueueTransport(virtual.Transport):
    """Kombu transport that auto-selects Vercel Queue Service delivery mode.

    The transport uses push delivery when running on Vercel and polling otherwise. It is registered
    as ``vercel://`` by ``install_vercel_celery_integration``.
    """

    Channel = AutoChannel
    driver_type = "vercel"
    driver_name = "Vercel Queue Service"


class VercelQueuePollTransport(virtual.Transport):
    """Kombu transport that always polls Vercel Queue Service.

    It is registered as ``vercel-poll://`` by ``install_vercel_celery_integration`` and is intended
    for workers that cannot receive Vercel Queue push deliveries.
    """

    Channel = PollChannel
    driver_type = "vercel"
    driver_name = "Vercel Queue Service (poll)"


class VercelQueuePushTransport(virtual.Transport):
    """Kombu transport that receives Vercel Queue Service push deliveries.

    It is registered as ``vercel-push://`` by ``install_vercel_celery_integration`` and requires the
    app's queues to be registered as Vercel Queue topic subscribers.
    """

    Channel = PushChannel
    driver_type = "vercel"
    driver_name = "Vercel Queue Service (push)"


def install_vercel_celery_integration(
    *,
    register_queues: bool = True,
    set_default_broker: bool = True,
    set_default_result_backend: bool = True,
) -> None:
    """Register Vercel Queue Service as a Celery broker integration.

    Register ``vercel://``. ``vercel-poll://``, and ``vercel-push://`` as valid Kombu transport
    aliases for automatic push/poll, always-poll, and always-push modes correspondingly. When
    *set_default_broker* is ``True`` (default), also set Celery default broker URL to ``vercel://``
    when no broker is configured already. When *set_default_result_backend* is ``True`` (default),
    set Celery's default result backend to ``vercel-runtime-cache://`` when no result backend is
    configured already. When *register_queues* is `True` (default), automatically register all
    Celery queues as Vercel Queues topic subscribers.

    This also registers ``vercel-runtime-cache://`` as the Vercel Runtime Cache result backend
    alias.
    """
    TRANSPORT_ALIASES["vercel"] = (
        f"{VercelQueueTransport.__module__}:{VercelQueueTransport.__name__}"
    )
    TRANSPORT_ALIASES["vercel-poll"] = (
        f"{VercelQueuePollTransport.__module__}:{VercelQueuePollTransport.__name__}"
    )
    TRANSPORT_ALIASES["vercel-push"] = (
        f"{VercelQueuePushTransport.__module__}:{VercelQueuePushTransport.__name__}"
    )
    celery_backends.BACKEND_ALIASES[DEFAULT_RESULT_BACKEND_ALIAS] = (
        f"{VercelRuntimeCacheBackend.__module__}:{VercelRuntimeCacheBackend.__name__}"
    )
    if set_default_broker and CELERY_DEFAULTS.get("broker_url") is None:
        CELERY_DEFAULTS["broker_url"] = "vercel://"
    if set_default_result_backend and CELERY_DEFAULTS.get("result_backend") is None:
        CELERY_DEFAULTS["result_backend"] = f"{DEFAULT_RESULT_BACKEND_ALIAS}://"
    if register_queues:
        _install_app_finalize_hook()
        _register_existing_app_queues()


__all__ = [
    "VercelQueuePollTransport",
    "VercelQueuePushTransport",
    "VercelQueueTransport",
    "VercelRuntimeCacheBackend",
    "__version__",
    "install_vercel_celery_integration",
    "register_celery_app_queues",
]
