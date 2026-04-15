from __future__ import annotations

from datetime import timedelta

# Public sandbox lifecycle default for wait/stop helpers.
DEFAULT_SANDBOX_WAIT_TIMEOUT = timedelta(seconds=30)

# Public sandbox lifecycle polling cadence for wait/stop helpers.
DEFAULT_SANDBOX_WAIT_POLL_INTERVAL = timedelta(milliseconds=500)

# Shared PTY bootstrap budget used while waiting for detached server metadata.
DEFAULT_PTY_CONNECTION_TIMEOUT = timedelta(seconds=30)

# Internal operational budget for downloading the PTY helper binary.
PTY_BINARY_DOWNLOAD_TIMEOUT = timedelta(seconds=60)

# Public snapshot invariant: non-zero expirations must be at least 24 hours.
MIN_SNAPSHOT_EXPIRATION = timedelta(hours=24)
