"""Custom value types on the wire, on `@workflow/core`'s ``Instance`` rail.

devalue encodes a value it has no built-in form for as a tagged pair,
``["<tag>", payload]``, and both ends have to agree on the tag. That tag set
belongs to `@workflow/core`, which composes it internally — so adding a tag is
a change to *that package*, not something either SDK can do on its own. The one
tag it leaves open is ``Instance``, carrying ``{classId, data}``, where the
class is found by ``classId`` in a registry each side keeps.

Riding that rail rather than minting a Python-only tag buys two things.

A JavaScript reader that has never heard of the class still renders the
payload: the CLI and the dashboard revive ``Instance`` through
``observabilityRevivers``, which builds a display reference instead of
consulting the registry, so `workflow inspect` shows ``Decimal@decimal '1.50'``
and the fields beside it survive. An unknown *tag*, by contrast, makes
`devalue.parse` throw, and o11y hydration catches that per payload — one
unreadable value would cost the whole run input.

And when a JavaScript peer does want the real object back, it registers the
same ``classId`` from userland — no change to `@workflow/core`, and no change
to the bytes.

``classId`` is ``class//<module>//<qualname>``, matching the ``step//`` and
``workflow//`` ids this SDK already mints, and the ``class//`` ids the
TypeScript compiler plugin generates.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import operator
import pathlib
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

CLASS_ID_PREFIX = "class//"

T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class _Registration:
    class_id: str
    serialize: Callable[[Any], Any]
    deserialize: Callable[[Any], Any]


_by_class_id: dict[str, _Registration] = {}
_by_class: dict[type, _Registration] = {}
# Types devalue already carries that a registration on a base class would
# otherwise capture. Walking the MRO stops here, declining the value.
_NATIVE: set[type] = {datetime.datetime}
# `type(value)` -> the registration that applies to it, or None. Cleared
# whenever a registration lands, since a new one can change the answer.
_resolved: dict[type, _Registration | None] = {}


def default_class_id(cls: type) -> str:
    """The ``classId`` a class gets when none is given."""
    return f"{CLASS_ID_PREFIX}{cls.__module__}//{cls.__qualname__}"


def register_serializable(
    cls: type[T],
    *,
    class_id: str | None = None,
    serialize: Callable[[T], Any] | None = None,
    deserialize: Callable[[Any], T] | None = None,
) -> None:
    """Teach the wire format about *cls*.

    *serialize* turns an instance into something devalue can already carry;
    *deserialize* turns that back into an instance. Both default to the
    ``_workflow_serialize`` / ``_workflow_deserialize`` methods on the
    class, and for an `enum.Enum` subclass to its member value and back.

    Pass *class_id* to pin the id — necessary when pairing with a class on the
    TypeScript side, and when the concrete type varies (`pathlib.Path` is
    `PosixPath` or `WindowsPath` depending on the host).

    Re-registering a ``classId`` replaces the previous entry. That is not a
    guard against collisions but a requirement: the sandbox re-imports the
    workflow's module, so a decorated class is registered once per import with
    a fresh class object each time.
    """
    if serialize is None:
        if hasattr(cls, "_workflow_serialize"):
            serialize = operator.methodcaller("_workflow_serialize")
        elif issubclass(cls, enum.Enum):
            serialize = operator.attrgetter("value")
        else:
            raise TypeError(
                f"{cls.__qualname__} needs a _workflow_serialize method, or a "
                f"serialize= function, to be put on the wire"
            )
    if deserialize is None:
        hook = getattr(cls, "_workflow_deserialize", None)
        if hook is not None:
            deserialize = hook
        elif issubclass(cls, enum.Enum):
            deserialize = cls
        else:
            raise TypeError(
                f"{cls.__qualname__} needs a _workflow_deserialize classmethod, "
                f"or a deserialize= function, to be read back"
            )

    registration = _Registration(
        class_id=class_id or default_class_id(cls),
        serialize=serialize,
        deserialize=deserialize,
    )
    _by_class_id[registration.class_id] = registration
    _by_class[cls] = registration
    _resolved.clear()


def serializable(cls: type[T] | None = None, *, class_id: str | None = None) -> Any:
    """Register a class for the wire, as a decorator.

    Usable bare or with a ``class_id``::

        @serializable
        class Point:
            def _workflow_serialize(self) -> dict[str, int]:
                return {"x": self.x, "y": self.y}

            @classmethod
            def _workflow_deserialize(cls, data: dict[str, int]) -> "Point":
                return cls(**data)

        @serializable
        class Tier(enum.Enum):   # an Enum needs no methods
            PRO = "pro"
    """

    def decorate(target: type[T]) -> type[T]:
        register_serializable(target, class_id=class_id)
        return target

    return decorate if cls is None else decorate(cls)


def _resolve(tp: type) -> _Registration | None:
    """The registration that applies to *tp*, by method resolution order.

    The MRO is what makes a base-class registration cover its subclasses —
    `pathlib.Path` is registered once and matches `PosixPath` — and it settles
    which of several applicable registrations wins without any ordering rule
    of our own.
    """
    if tp in _resolved:
        return _resolved[tp]
    found: _Registration | None = None
    for base in tp.__mro__:
        if base in _NATIVE:
            break
        if base in _by_class:
            found = _by_class[base]
            break
    _resolved[tp] = found
    return found


def reduce_instance(value: Any) -> Any:
    """devalue reducer for the ``Instance`` tag. Falsy declines the value."""
    registration = _resolve(type(value))
    if registration is None:
        return False
    try:
        data = registration.serialize(value)
    except Exception as error:
        # A registered serializer is code this module does not own, so its
        # failure is attributed here rather than surfacing from inside the
        # codec with nothing naming the class that caused it.
        raise ValueError(f"{registration.class_id} could not be written: {error}") from error
    return {"classId": registration.class_id, "data": data}


def revive_instance(value: Any) -> Any:
    """devalue reviver for the ``Instance`` tag.

    Raises `ValueError` for a class this side has not registered, which
    :func:`vercel._internal.workflow.serialization.hydrate` reports with the
    name of the payload it came from. Returning a stand-in instead would hand
    the workflow something that is not the value it was sent.
    """
    if not isinstance(value, dict) or not isinstance(value.get("classId"), str):
        raise ValueError(f"malformed Instance payload: {value!r}")
    class_id = value["classId"]
    registration = _by_class_id.get(class_id)
    if registration is None:
        raise ValueError(
            f"unknown class {class_id!r}; register it with @serializable to read "
            f"a payload that carries one"
        )
    try:
        return registration.deserialize(value.get("data"))
    except Exception as error:
        # Payload data is untrusted, and a registered deserializer may raise
        # anything at all -- `Decimal("garbage")` raises `InvalidOperation`,
        # which is not even an error the codec would recognize.
        raise ValueError(f"{class_id} could not be read back: {error}") from error


def registration_hint(value: Any) -> str:
    """A sentence naming the way out, for a value nothing can serialize."""
    if isinstance(value, enum.Enum):
        # The class is user-defined, so no built-in registration can cover it.
        # `StrEnum`/`IntEnum` need none: they are `str`/`int` on the wire.
        return (
            f" Register {type(value).__qualname__} with @serializable, or derive it "
            f"from enum.StrEnum / enum.IntEnum to send the bare member value."
        )
    # A class object rather than an instance is a different thing to send --
    # TypeScript has a separate `Class` tag for it -- so no hint applies.
    if value is not None and not isinstance(value, type):
        return f" Register {type(value).__qualname__} with @serializable to send it."
    return ""


REDUCERS: dict[str, Callable[[Any], Any]] = {"Instance": reduce_instance}
REVIVERS: dict[str, Callable[[Any], Any]] = {"Instance": revive_instance}


# ═══════════════════════════════════════════════════════════════════════════
# built-in registrations
# ═══════════════════════════════════════════════════════════════════════════
#
# Standard-library types that devalue rejects outright and that turn up in
# ordinary payloads. Types it already carries are left alone -- `datetime` is
# a `Date`, `bytes` an `ArrayBuffer`, `set` a `Set` -- as are the ones it
# narrows rather than rejects (`tuple` to a list, `frozenset` to a set,
# `OrderedDict` to an object): those already cross the wire, and wrapping them
# would take a plain JavaScript array or Set away from a JavaScript reader for
# the sake of a Python distinction.
#
# An `enum.Enum` cannot be covered here because the class is the user's; it is
# one `@serializable` away, and `registration_hint` says so.

register_serializable(
    decimal.Decimal,
    serialize=str,
    deserialize=decimal.Decimal,
)
register_serializable(
    uuid.UUID,
    serialize=str,
    deserialize=uuid.UUID,
)
register_serializable(
    # `datetime` is a `date` subclass and devalue carries it as a JS `Date`
    # already; `_NATIVE` is what keeps this registration off it.
    datetime.date,
    serialize=lambda value: value.isoformat(),
    deserialize=datetime.date.fromisoformat,
)
register_serializable(
    datetime.time,
    serialize=lambda value: value.isoformat(),
    deserialize=datetime.time.fromisoformat,
)
register_serializable(
    datetime.timedelta,
    # Exact rather than `total_seconds()`, which is a float and rounds.
    serialize=lambda value: {
        "days": value.days,
        "seconds": value.seconds,
        "microseconds": value.microseconds,
    },
    deserialize=lambda data: datetime.timedelta(**data),
)
register_serializable(
    pathlib.PurePath,
    # The concrete class is host-dependent (`PosixPath` / `WindowsPath`), so
    # the id names the portable one and reading picks the local flavour.
    class_id=f"{CLASS_ID_PREFIX}pathlib//Path",
    serialize=str,
    deserialize=pathlib.Path,
)
