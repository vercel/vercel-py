"""Standalone queue development server."""

from __future__ import annotations

from ._internal.devserver import (
    EmbeddedQueueDevServer,
    embedded_queue_dev_server,
    main,
)

# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = (
    "EmbeddedQueueDevServer",
    "embedded_queue_dev_server",
    "main",
)


if __name__ == "__main__":
    raise SystemExit(main())
