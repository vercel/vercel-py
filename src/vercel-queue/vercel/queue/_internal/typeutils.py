from __future__ import annotations

from typing import (
    Annotated,
    Any,
    ClassVar,
    Final,
    ForwardRef,
    Literal,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import inspect
import types
from dataclasses import dataclass
from types import FrameType

_T = TypeVar("_T")
_TYPE_VAR_TYPE = type(_T)


class TypeAnnotationResolutionError(TypeError):
    """Raised when a runtime annotation cannot be resolved."""


@dataclass(frozen=True)
class ResolvedAnnotation:
    annotation: Any
    localns: dict[str, Any] | None = None


def is_generic_alias(tp: Any) -> bool:
    return get_origin(tp) is not None


def is_annotated(tp: Any) -> bool:
    return get_origin(tp) is Annotated


def strip_annotated(tp: Any) -> Any:
    while is_annotated(tp):
        tp = get_args(tp)[0]
    return tp


def is_type_var(tp: Any) -> bool:
    return isinstance(tp, _TYPE_VAR_TYPE)


def is_classvar(tp: Any) -> bool:
    return get_origin(tp) is ClassVar


def is_final(tp: Any) -> bool:
    return get_origin(tp) is Final


def is_union_type(tp: Any) -> bool:
    return get_origin(tp) in {Union, types.UnionType}


def origin_is(tp: Any, *origins: Any) -> bool:
    return get_origin(tp) in origins


def args(tp: Any) -> tuple[Any, ...]:
    return get_args(tp)


def annotation_needs_resolution(annotation: Any) -> bool:
    if isinstance(annotation, str | ForwardRef):
        return True
    if is_type_var(annotation):
        return False
    if origin_is(annotation, Literal):
        return False
    if origin_is(annotation, Annotated):
        annotation_args = args(annotation)
        return bool(annotation_args) and annotation_needs_resolution(annotation_args[0])
    return any(annotation_needs_resolution(item) for item in args(annotation))


def _normalize_forward_refs(annotation: Any) -> Any:
    if isinstance(annotation, str):
        return ForwardRef(annotation)
    if isinstance(annotation, ForwardRef) or is_type_var(annotation):
        return annotation

    annotation_args = args(annotation)
    if not annotation_args or origin_is(annotation, Literal):
        return annotation

    origin = get_origin(annotation)
    if origin is Annotated:
        first_arg = _normalize_forward_refs(annotation_args[0])
        if first_arg is annotation_args[0]:
            return annotation
        return Annotated.__class_getitem__((first_arg, *annotation_args[1:]))

    normalized_args = tuple(_normalize_forward_refs(item) for item in annotation_args)
    if normalized_args == annotation_args:
        return annotation

    if isinstance(annotation, types.GenericAlias):
        return origin[normalized_args]
    copy_with = getattr(annotation, "copy_with", None)
    if copy_with is not None:
        return copy_with(normalized_args)
    return annotation


def _resolve_annotation_fully(
    annotation: Any,
    *,
    globalns: dict[str, Any],
    localns: dict[str, Any] | None = None,
) -> ResolvedAnnotation:
    resolved = resolve_annotation(annotation, globalns=globalns, localns=localns)
    if not annotation_needs_resolution(resolved):
        return ResolvedAnnotation(resolved, localns)

    normalized = _normalize_forward_refs(resolved)
    return ResolvedAnnotation(
        resolve_annotation(normalized, globalns=globalns, localns=localns),
        localns,
    )


def _call_stack_localns() -> dict[str, Any]:
    frame: FrameType | None = inspect.currentframe()
    localns: dict[str, Any] = {}
    try:
        frame = frame.f_back if frame is not None else None
        while frame is not None:
            localns = {**frame.f_locals, **localns}
            frame = frame.f_back
    finally:
        del frame
    return localns


def resolve_annotation(
    annotation: Any,
    *,
    globalns: dict[str, Any],
    localns: dict[str, Any] | None = None,
) -> Any:
    class AnnotationShim:
        pass

    AnnotationShim.__annotations__ = {"value": annotation}
    return get_type_hints(
        AnnotationShim,
        globalns=globalns,
        localns=localns,
        include_extras=True,
    )["value"]


def resolve_annotation_from_call_stack(
    annotation: Any,
    *,
    globalns: dict[str, Any],
) -> Any:
    return resolve_annotation_with_namespace_from_call_stack(
        annotation,
        globalns=globalns,
    ).annotation


def resolve_annotation_with_namespace_from_call_stack(
    annotation: Any,
    *,
    globalns: dict[str, Any],
) -> ResolvedAnnotation:
    if annotation is Any or not annotation_needs_resolution(annotation):
        return ResolvedAnnotation(annotation, _call_stack_localns())

    resolution_error: BaseException | None = None
    try:
        return _resolve_annotation_fully(annotation, globalns=globalns)
    except (NameError, TypeError, AttributeError) as exc:
        resolution_error = exc

    localns = _call_stack_localns()

    try:
        return _resolve_annotation_fully(
            annotation,
            globalns=globalns,
            localns=localns,
        )
    except (NameError, TypeError, AttributeError) as exc:
        resolution_error = exc

    if not annotation_needs_resolution(annotation):
        return ResolvedAnnotation(annotation)

    raise TypeAnnotationResolutionError from resolution_error
