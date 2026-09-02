"""Serialization and deserialization based on function signatures, using Pydantic

The core goal here is to allow Pydantic models and dataclasses to be
transparently passed and returned to steps and workflows. A nice side
effect is doing some typechecking on other values.

SignatureCodec does its dumping and validation in python mode with
arbitrary_types_allowed, so types like `datetime`, `UUID`, `set`, etc
as well as classes it doesn't know about will get passed through
untouched.

SignatureCodec is layered on top of the lower-level serialization done
by the `serialization` module, which handles anything that has been
passed through. TypeCodec is the per-annotation unit it is made of.

===========  ==========================================================
outbound     caller -> SignatureCodec.dump -> PayloadEncoder.encode
inbound      serialization.hydrate -> SignatureCodec.validate -> callee
===========  ==========================================================

Unannotated parameters, `Any`, and annotations that Pydantic can't
build a schema for are passed through unchanged.
"""

from __future__ import annotations

import inspect
import logging
import typing
from collections.abc import Callable, Mapping
from typing import Any

import pydantic

from vercel._internal.core import typeutils

from .errors import FatalError

logger = logging.getLogger("vercel.workflow")

# We set arbitrary_types_allowed so that types that pydantic doesn't
# recognize (maybe ones using @serializable) aren't rejected.
_ARBITRARY = pydantic.ConfigDict(arbitrary_types_allowed=True)


class TypeValidationError(FatalError):
    """A value did not match the annotation it arrived for.

    Fatal, because a retry can't possibly help.
    """


def _build_adapter(annotation: Any) -> pydantic.TypeAdapter[Any] | None:
    """An adapter for *annotation*, or `None` to pass values through."""
    if (
        annotation is Any
        or annotation is inspect.Parameter.empty
        or typeutils.annotation_needs_resolution(annotation)
    ):
        return None
    try:
        adapter = pydantic.TypeAdapter(annotation, config=_ARBITRARY)
    except pydantic.PydanticUserError:
        # Either the annotation already has a config or no schema can
        # be built for it at all. Try with no config in case that
        # works.
        try:
            adapter = pydantic.TypeAdapter(annotation)
        except Exception:
            return None
    except Exception:
        return None
    return adapter if adapter.pydantic_complete else None


class TypeCodec:
    """Dump and validate values against one annotation.

    An annotation pydantic can build no schema for -- or `Any`, or one that
    still needs resolving -- passes values through untouched in both
    directions.
    """

    def __init__(self, annotation: Any) -> None:
        self.annotation = annotation
        self._adapter = _build_adapter(annotation)

    def dump(self, value: Any) -> Any:
        """*value*, as the annotation puts it on the wire."""
        if self._adapter is None:
            return value
        # serialize_as_any so a subclass keeps the fields the annotation does
        # not know about, rather than being silently truncated in transit.
        # warnings=False because validation belongs to the receiving side; an
        # actual serialization failure, however, must prevent writing bytes the
        # receiver cannot reconstruct.
        return self._adapter.dump_python(value, serialize_as_any=True, warnings=False)

    def validate(self, value: Any, *, what: str) -> Any:
        """*value* as the annotation reads it, or `TypeValidationError` naming *what*."""
        if self._adapter is None:
            return value
        try:
            return self._adapter.validate_python(value)
        except pydantic.ValidationError as error:
            raise TypeValidationError(f"{what} does not match: {error}") from error


class SignatureCodec:
    """The adapters for one workflow's or step's signature.

    Built lazily because workflows and steps are registered mid-import, before
    the types they refer to our registered.
    """

    def __init__(
        self, func: Callable[..., Any], signature: inspect.Signature, qualname: str
    ) -> None:
        self._func = func
        self._signature = signature
        self._qualname = qualname
        self._hints: Mapping[str, Any] | None = None
        self._codecs: dict[str, TypeCodec] = {}

    def _resolved_hints(self) -> Mapping[str, Any]:
        if self._hints is None:
            try:
                # include_extras keeps `Annotated[int, Field(gt=0)]` intact.
                self._hints = typing.get_type_hints(self._func, include_extras=True)
            except Exception as error:
                # A `TYPE_CHECKING`-only import, a forward reference to
                # something that never arrived.
                logger.debug(
                    "[Workflows] cannot resolve annotations of %s, "
                    "passing its payloads through untouched: %s",
                    self._qualname,
                    error,
                )
                self._hints = {}
        return self._hints

    def _codec(self, name: str) -> TypeCodec:
        try:
            return self._codecs[name]
        except KeyError:
            pass
        annotation = self._resolved_hints().get(name, inspect.Parameter.empty)
        codec = TypeCodec(annotation)
        self._codecs[name] = codec
        return codec

    # ═══════════════════════════════════════════════════════════════════════
    # outbound
    # ═══════════════════════════════════════════════════════════════════════

    def dump(self, name: str, value: Any) -> Any:
        """*value*, as the parameter named *name* puts it on the wire."""
        return self._codec(name).dump(value)

    def dump_arguments(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """A bound call, with each value dumped by its parameter annotation."""
        mapping = _invert_bindings(self._signature, args, kwargs)

        dumped_args = tuple(self.dump(mapping[index], value) for index, value in enumerate(args))
        dumped_kwargs = {key: self.dump(mapping[key], value) for key, value in kwargs.items()}
        return dumped_args, dumped_kwargs

    def dump_return(self, value: Any) -> Any:
        return self.dump("return", value)

    # ═══════════════════════════════════════════════════════════════════════
    # inbound
    # ═══════════════════════════════════════════════════════════════════════

    def _validate(self, name: str, value: Any, *, what: str) -> Any:
        return self._codec(name).validate(value, what=f"{self._qualname}: {what}")

    def validate_return(self, value: Any) -> Any:
        return self._validate("return", value, what="the return value")

    def validate_arguments(
        self, args: list[Any], kwargs: dict[str, Any]
    ) -> tuple[list[Any], dict[str, Any]]:
        """A decoded call, with each value validated against its parameter.

        Bind first so malformed cross-language payloads fail with Python's
        ordinary argument error before any values are validated.
        """
        mapping = _invert_bindings(self._signature, args, kwargs)

        validated_args = [
            self._validate(mapping[index], value, what=f"argument {mapping[index]!r}")
            for index, value in enumerate(args)
        ]

        validated_kwargs = {
            key: self._validate(mapping[key], value, what=f"argument {key!r}")
            for key, value in kwargs.items()
        }

        return validated_args, validated_kwargs


def _invert_bindings(
    sig: inspect.Signature, args: tuple[Any, ...] | list[Any], kwargs: dict[str, Any]
) -> dict[int | str, str]:
    bound = sig.bind(*tuple(range(len(args))), **{key: key for key in kwargs})

    names = {}
    for name, index in bound.arguments.items():
        if isinstance(index, (tuple, dict)):
            for item in index:
                names[item] = name
        else:
            names[index] = name

    return names
