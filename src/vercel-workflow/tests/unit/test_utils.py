from typing import Annotated

import pydantic
import pytest

from vercel.workflow._internal.utils import Utf16MaxLength, utf16_code_unit_length


@pytest.mark.parametrize(
    "value,expected",
    [
        pytest.param("", 0, id="empty"),
        pytest.param("hello", 5, id="ascii"),
        pytest.param("中文", 2, id="bmp"),
        pytest.param("\uffff", 1, id="last-bmp-code-point"),
        pytest.param("\U00010000", 2, id="first-non-bmp-code-point"),
        pytest.param("😀", 2, id="emoji"),
        pytest.param("a中😀", 4, id="mixed"),
        pytest.param("\ud800", 1, id="lone-high-surrogate"),
        pytest.param("\udc00", 1, id="lone-low-surrogate"),
        pytest.param("\ud83d\ude00", 2, id="explicit-surrogate-pair"),
    ],
)
def test_utf16_code_unit_length(value: str, expected: int) -> None:
    assert utf16_code_unit_length(value) == expected


@pytest.mark.parametrize(
    "value",
    ["", "ab", "中文", "😀", "\ud800", "\udc00", "\ud83d\ude00"],
)
def test_utf16_max_length_accepts_within_limit(value: str) -> None:
    adapter = pydantic.TypeAdapter[str](Annotated[str, Utf16MaxLength(code_units=2)])
    assert adapter.validate_python(value) == value


@pytest.mark.parametrize("value", ["abc", "中文名", "😀x", "😀😀", "\ud83d\ude00x"])
def test_utf16_max_length_rejects_over_limit(value: str) -> None:
    adapter = pydantic.TypeAdapter[str](Annotated[str, Utf16MaxLength(2)])
    with pytest.raises(pydantic.ValidationError, match="at most 2 UTF-16 code units"):
        adapter.validate_python(value)


def test_utf16_max_length_optional_field() -> None:
    class Model(pydantic.BaseModel):
        value: Annotated[str, Utf16MaxLength(2)] | None = None

    assert Model().value is None
    assert Model(value=None).value is None
    assert Model.model_validate_json('{"value": "😀"}').model_dump() == {"value": "😀"}
    with pytest.raises(pydantic.ValidationError, match="at most 2 UTF-16 code units"):
        Model.model_validate_json('{"value": "😀x"}')
    with pytest.raises(pydantic.ValidationError, match="valid string"):
        Model.model_validate({"value": 123})


def test_utf16_max_length_zero() -> None:
    adapter = pydantic.TypeAdapter[str](Annotated[str, Utf16MaxLength(0)])
    assert adapter.validate_python("") == ""
    with pytest.raises(pydantic.ValidationError, match="at most 0 UTF-16 code units"):
        adapter.validate_python("x")


def test_utf16_max_length_negative_limit() -> None:
    with pytest.raises(ValueError, match="code_units must be non-negative"):
        Utf16MaxLength(-1)


def test_utf16_max_length_json_schema() -> None:
    adapter = pydantic.TypeAdapter[str](Annotated[str, Utf16MaxLength(2)])
    # JSON Schema's maxLength counts code points, not UTF-16 code units.
    assert adapter.json_schema() == {"type": "string"}
