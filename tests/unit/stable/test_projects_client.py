from __future__ import annotations

from hypothesis import given, strategies as st

from vercel._internal.stable.sdk.projects import _snake_to_camel

# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------

_snake_ident = st.from_regex(r"[a-z][a-z0-9]*(_[a-z][a-z0-9]*)*", fullmatch=True).filter(
    lambda s: len(s) <= 80
)


@given(value=_snake_ident)
def test_prop_snake_to_camel_preserves_structure(value: str) -> None:
    result = _snake_to_camel(value)
    assert "_" not in result
    # First character case is preserved from the input head segment
    assert result[0] == value[0]
