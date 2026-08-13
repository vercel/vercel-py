from __future__ import annotations


class FatalError(Exception):
    """A step failure that will not be retried."""
