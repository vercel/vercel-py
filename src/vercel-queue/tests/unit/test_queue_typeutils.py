from __future__ import annotations

from typing import Annotated, Any, ClassVar, Final, ForwardRef, TypeVar, Union

import importlib.util
import sys
from collections.abc import Iterable
from pathlib import Path


def _load_typeutils() -> Any:
    root = Path(__file__).parents[2]
    path = root / "vercel" / "queue" / "_internal" / "typeutils.py"
    spec = importlib.util.spec_from_file_location("queue_typeutils_for_tests", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


typeutils = _load_typeutils()


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


if __name__ == "__main__":
    test_generic_alias_detection()
    test_annotated_detection_and_stripping()
    test_special_form_detection()
    test_union_detection_supports_typing_and_pep604()
    test_origin_and_args_helpers()
    test_resolve_annotation_handles_forward_refs_inside_generics()
