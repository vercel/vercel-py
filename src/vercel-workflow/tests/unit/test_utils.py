import pytest

from vercel.workflow._internal.utils import utf16_code_unit_length


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
