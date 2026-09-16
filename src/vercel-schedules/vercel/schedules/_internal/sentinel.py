"""Sentinel for "argument not supplied", where `None` is itself a valid value."""


class UnsetType:
    """Type of `UNSET`, for annotating parameters that accept it."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNSET"


UNSET = UnsetType()


__all__ = ["UNSET", "UnsetType"]
