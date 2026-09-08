"""Explicit success and error outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, NoReturn, Protocol, TypeAlias, TypeVar

_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)
_T = TypeVar("_T")


class FutureLike(Protocol[_T_contra]):
    """A future whose outcome can be set."""

    def set_result(self, result: _T_contra, /) -> None: ...

    def set_exception(self, exception: BaseException, /) -> None: ...


@dataclass(frozen=True, slots=True)
class Value(Generic[_T_co]):
    """A successful outcome."""

    value: _T_co

    def unwrap(self) -> _T_co:
        return self.value

    def set_future(self, future: FutureLike[_T_co]) -> None:
        future.set_result(self.value)


@dataclass(frozen=True, slots=True)
class Error:
    """A failed outcome."""

    error: BaseException

    def unwrap(self) -> NoReturn:
        raise self.error

    def set_future(self, future: FutureLike[_T]) -> None:
        future.set_exception(self.error)


Outcome: TypeAlias = Value[_T_co] | Error
"""The result of a computation."""


__all__ = ("Error", "Outcome", "FutureLike", "Value")
