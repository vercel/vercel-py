"""Standalone queue development server."""

from __future__ import annotations

from ._internal.devserver import (
    EmbeddedQueueDevServer,
    QueueClientAsgiDevServer,
    embedded_queue_dev_server,
    main,
    queue_client_asgi_dev_server,
)

# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "EmbeddedQueueDevServer",
    "QueueClientAsgiDevServer",
    "embedded_queue_dev_server",
    "main",
    "queue_client_asgi_dev_server",
)


if __name__ == "__main__":
    raise SystemExit(main())
