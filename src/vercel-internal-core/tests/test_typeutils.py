from __future__ import annotations

from collections.abc import Iterable
from typing import Annotated, ClassVar, Final, ForwardRef, TypeVar, Union

from vercel._internal.core import typeutils


def test_generic_alias_detection() -> None:
    assert typeutils.is_generic_alias(list[int])
    assert typeutils.is_generic_alias(Iterable[bytes])
    assert not typeutils.is_generic_alias(int)


def test_annotated_detection_and_stripping() -> None:
    annotation = Annotated[Annotated[list[int], "inner"], "outer"]

    assert typeutils.is_annotated(annotation)
    assert typeutils.strip_annotated(annotation) == list[int]
    assert typeutils.strip_annotated(str) is str


def test_special_form_detection() -> None:
    T = TypeVar("T")

    assert typeutils.is_type_var(T)
    assert not typeutils.is_type_var(int)
    assert typeutils.is_classvar(ClassVar[int])
    assert not typeutils.is_classvar(int)
    assert typeutils.is_final(Final[int])
    assert not typeutils.is_final(int)


def test_union_detection_supports_typing_and_pep604() -> None:
    assert typeutils.is_union_type(Union[int, str])  # noqa: UP007
    assert typeutils.is_union_type(int | str)
    assert not typeutils.is_union_type(list[int])


def test_origin_and_args_helpers() -> None:
    assert typeutils.origin_is(list[int], list)
    assert typeutils.origin_is(Iterable[bytes], Iterable)
    assert not typeutils.origin_is(dict[str, int], list)
    assert typeutils.args(dict[str, int]) == (str, int)


def test_resolve_annotation_handles_forward_refs_inside_generics() -> None:
    class Payload:
        pass

    list_annotation = list.__class_getitem__(ForwardRef("Payload"))
    annotation = dict.__class_getitem__((str, list_annotation))

    assert typeutils.annotation_needs_resolution(annotation)
    assert (
        typeutils.resolve_annotation(
            annotation,
            globalns={"Payload": Payload},
        )
        == dict[str, list[Payload]]
    )
