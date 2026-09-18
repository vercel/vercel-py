"""Immutable mapping of cookie names to values."""

from __future__ import annotations

from ._params import Params

__all__ = ["Cookies"]


class Cookies(Params):
    """Immutable mapping of cookie names to values parsed from the ``Cookie`` header.

    Inherits the full :class:`Params` interface: case-sensitive lookup,
    iteration, and :meth:`~Params.get` with optional default.
    """

    __slots__ = ()
