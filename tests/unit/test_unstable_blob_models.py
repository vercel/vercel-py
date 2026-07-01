import pytest
from hypothesis import given, strategies as st

from vercel._internal.unstable.blob.models import _parse_file_mode

VALID_FILE_MODES = {
    "r",
    "rb",
    "w",
    "wb",
    "x",
    "xb",
    "a",
    "ab",
    "r+",
    "r+b",
    "rb+",
    "w+",
    "w+b",
    "wb+",
    "x+",
    "x+b",
    "xb+",
    "a+",
    "a+b",
    "ab+",
}


@given(st.text(alphabet="rwaxb+t", min_size=0, max_size=5))
def test_parse_file_mode_accepts_public_open_modes(mode: str) -> None:
    if mode not in VALID_FILE_MODES:
        with pytest.raises(ValueError, match="invalid mode"):
            _parse_file_mode(mode)
        return

    parsed = _parse_file_mode(mode)
    operation = mode[0]
    updating = "+" in mode
    appending = operation == "a"

    assert parsed.value == mode
    assert parsed.binary is ("b" in mode)
    assert parsed.reading is (operation == "r" or updating)
    assert parsed.writing is (operation != "r" or updating)
    assert parsed.exclusive is (operation == "x")
    assert parsed.appending is appending
    assert parsed.updating is updating
    assert parsed.truncating is (operation == "w")
    assert parsed.requires_staging is (appending or updating)
