"""Celery integration for Vercel Queue Service."""

from kombu.transport import TRANSPORT_ALIASES, virtual

from celery.app import backends as celery_backends
from celery.app.defaults import DEFAULTS as CELERY_DEFAULTS
from vercel.queue import CommunicationError, TokenResolutionError, UnauthorizedError

from ._broker import (
    AutoChannel,
    PollChannel,
    PushChannel,
    _configure_existing_app_defaults,
    _install_app_finalize_hook,
    _install_connection_transport_options_hook,
    _register_existing_app_queues,
    _set_default_broker_set_by_installer,
    register_celery_app_queues,
)
from ._result_backend import DEFAULT_RESULT_BACKEND_ALIAS, VercelRuntimeCacheBackend
from .version import __version__

_DEFAULT_CONNECTION_PARAMS = {"hostname": None, "port": None}
# Token resolution depends on request-scoped OIDC context that may be
# momentarily unavailable on Celery-owned threads (and refreshes with the next
# push delivery). A cached token can also expire between local resolution and the
# remote request, producing UnauthorizedError once before the next resolution
# evicts it. CommunicationError is ordinary network trouble. Treat all three as
# recoverable connection failures so the Celery consumer restarts instead of
# shutting the worker down permanently. Kombu's own connection_errors use
# amqp.exceptions.ConnectionError, not the builtin, so CommunicationError must be
# listed explicitly.
_CONNECTION_ERRORS = (
    *virtual.Transport.connection_errors,
    TokenResolutionError,
    UnauthorizedError,
    CommunicationError,
)


class VercelQueueTransport(virtual.Transport):
    """Kombu transport that auto-selects Vercel Queue Service delivery mode.

    The transport uses push delivery when running on Vercel and polling otherwise. It is registered
    as ``vercel://`` by ``install_vercel_celery_integration``.
    """

    Channel = AutoChannel
    driver_type = "vercel"
    driver_name = "Vercel Queue Service"
    default_connection_params = _DEFAULT_CONNECTION_PARAMS
    connection_errors = _CONNECTION_ERRORS


class VercelQueuePollTransport(virtual.Transport):
    """Kombu transport that always polls Vercel Queue Service.

    It is registered as ``vercel-poll://`` by ``install_vercel_celery_integration`` and is intended
    for workers that cannot receive Vercel Queue push deliveries.
    """

    Channel = PollChannel
    driver_type = "vercel"
    driver_name = "Vercel Queue Service (poll)"
    default_connection_params = _DEFAULT_CONNECTION_PARAMS
    connection_errors = _CONNECTION_ERRORS


class VercelQueuePushTransport(virtual.Transport):
    """Kombu transport that receives Vercel Queue Service push deliveries.

    It is registered as ``vercel-push://`` by ``install_vercel_celery_integration`` and requires the
    app's queues to be registered as Vercel Queue topic subscribers.
    """

    Channel = PushChannel
    driver_type = "vercel"
    driver_name = "Vercel Queue Service (push)"
    default_connection_params = _DEFAULT_CONNECTION_PARAMS
    connection_errors = _CONNECTION_ERRORS


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
    default_broker_url = "vercel://" if set_default_broker else None
    default_result_backend = (
        f"{DEFAULT_RESULT_BACKEND_ALIAS}://" if set_default_result_backend else None
    )
    if set_default_broker and CELERY_DEFAULTS.get("broker_url") is None:
        CELERY_DEFAULTS["broker_url"] = default_broker_url
        _set_default_broker_set_by_installer(value=True)
    if set_default_result_backend and CELERY_DEFAULTS.get("result_backend") is None:
        CELERY_DEFAULTS["result_backend"] = default_result_backend
    _install_connection_transport_options_hook()
    _install_app_finalize_hook(register_queues=register_queues)
    _configure_existing_app_defaults(
        broker_url=default_broker_url,
        result_backend=default_result_backend,
    )
    if register_queues:
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
