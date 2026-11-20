from __future__ import annotations

from collections.abc import Mapping

import pytest
from hypothesis import given, strategies as st

from vercel.board import (
    BoardValidationError,
    Can,
    Permission,
    parse_can,
)

_SEGMENTS = st.sampled_from(["dev", "ci", "ops", "prod", "security", "api", "a1", "b_2", "c-3"])
_CHANNEL_PATHS = st.lists(_SEGMENTS, min_size=1, max_size=3).map("/".join)
_PATTERNS = st.one_of(
    st.just("*"),
    _CHANNEL_PATHS,
    _CHANNEL_PATHS.map(lambda p: f"{p}/"),
)
_PERMISSIONS = st.sampled_from(list(Permission))
_GRANT = st.one_of(
    st.just("all"),
    st.just("none"),
    st.lists(_PERMISSIONS, max_size=len(Permission)),
)
_CAN_STRATEGY = st.one_of(
    st.just("all"),
    st.just("none"),
    st.lists(_PERMISSIONS, max_size=len(Permission)),
    st.dictionaries(_PATTERNS, _GRANT, max_size=6),
).map(Can.from_input)


@given(a=_CAN_STRATEGY, b=_CAN_STRATEGY, perm=_PERMISSIONS, channel=_CHANNEL_PATHS)
def test_narrow_is_exact_channel_intersection_and_monotonic(
    a: Can,
    b: Can,
    perm: Permission,
    channel: str,
) -> None:
    narrowed = a.narrow(b)
    expected = a.allows(perm, channel) and b.allows(perm, channel)
    assert narrowed.allows(perm, channel) is expected
    if narrowed.allows(perm, channel):
        assert a.allows(perm, channel)
        assert b.allows(perm, channel)


@given(a=_CAN_STRATEGY, b=_CAN_STRATEGY)
def test_narrow_algebraic_laws(a: Can, b: Can) -> None:
    assert a.narrow(a) == a
    assert a.narrow(b) == b.narrow(a)
    assert a.narrow(Can.full()) == a
    assert a.narrow(Can.empty()) == Can.empty()


@given(can=_CAN_STRATEGY)
def test_can_header_roundtrip(can: Can) -> None:
    header = can.to_header()
    parsed = parse_can(header)
    assert parsed == can
    assert parsed.to_header() == header
    assert Can.parse(header) == can


def test_permission_implications() -> None:
    post_only = Can.from_input([Permission.POST])
    assert post_only.allows(Permission.POST, "general")
    assert not post_only.allows(Permission.READ, "general")

    for implied_reader in (Permission.REACT, Permission.LABEL, Permission.SYNC):
        can = Can.from_input([implied_reader])
        assert can.allows(implied_reader, "general")
        assert can.allows(Permission.READ, "general")
        assert not can.allows(Permission.POST, "general")

    manager = Can.from_input([Permission.MANAGE])
    for perm in Permission:
        assert manager.allows(perm, "general")
    assert manager == Can.full()


def test_pattern_specificity_precedence() -> None:
    can = Can.from_input(
        {
            "*": [Permission.READ],
            "dev/": [Permission.READ, Permission.POST],
            "dev/secret/": "none",
            "dev/secret/public": [Permission.READ],
        }
    )
    assert can.allows(Permission.READ, "general")
    assert not can.allows(Permission.POST, "general")

    assert can.allows(Permission.POST, "dev")
    assert can.allows(Permission.POST, "dev/ci")

    assert not can.allows(Permission.READ, "dev/secret")
    assert not can.allows(Permission.READ, "dev/secret/keys")

    assert can.allows(Permission.READ, "dev/secret/public")
    assert not can.allows(Permission.POST, "dev/secret/public")

    assert can.at(Permission.POST, "dev/")
    assert not can.at(Permission.READ, "dev/secret/")
    assert not can.everywhere(Permission.READ)
    assert can.anywhere(Permission.POST)


def test_summary_is_canonical_and_immutable() -> None:
    can = Can.from_input(
        {
            "dev/": [Permission.POST, Permission.READ],
            "*": [Permission.READ],
            "ci/": [Permission.READ],  # redundant with *, should be pruned
        }
    )
    summary = can.summary()
    assert isinstance(summary, Mapping)
    assert list(summary.keys()) == ["*", "dev/"]
    assert summary["*"] == (Permission.READ,)
    assert summary["dev/"] == (Permission.READ, Permission.POST)
    with pytest.raises(TypeError):
        summary["*"] = ()  # type: ignore[index]


def test_to_header_uses_compact_all_and_enforces_can_max() -> None:
    assert Can.full().to_header() == "*=all"
    assert Can.from_input([Permission.REACT]).to_header() == "*=react"
    assert Can.from_input([Permission.READ, Permission.LABEL]).to_header() == "*=label"

    compact_header = ";".join(f"x{i}=all" for i in range(150))
    parsed = parse_can(compact_header)
    assert parsed.to_header().count("=all") == 150
    assert parse_can(parsed.to_header()) == parsed

    react_header = ";".join(f"x{i}=react" for i in range(330))
    parsed_react = parse_can(react_header)
    assert parsed_react.to_header().count("=react") == 330
    assert parse_can(parsed_react.to_header()) == parsed_react

    oversized = Can.from_input({f"chan-{i:04d}": [Permission.READ] for i in range(400)})
    with pytest.raises(BoardValidationError) as exc_info:
        oversized.to_header()
    assert exc_info.value.code == "invalid_can"
    assert "chan-0000=read" in str(oversized)
    assert repr(oversized).startswith("Can(")


@pytest.mark.parametrize(
    "bad_input",
    [
        "read",
        "dev/=read",
        {"dev/": "read"},
        {"dev/": "read,post"},
        {"dev/*": [Permission.READ]},
        {"UPPER": [Permission.READ]},
        {"dev//ci": [Permission.READ]},
        {"dev\n": [Permission.READ]},
        {"dev/\n": [Permission.READ]},
        {"": [Permission.READ]},
        {"dev/": ["unknown"]},
    ],
)
def test_can_from_input_rejects_invalid_values(bad_input: object) -> None:
    with pytest.raises(BoardValidationError) as exc_info:
        Can.from_input(bad_input)  # type: ignore[arg-type]
    assert exc_info.value.code == "invalid_can"


@pytest.mark.parametrize(
    "bad_header",
    [
        "",
        "   ;  ",
        "dev/",
        "dev/=unknown",
        "dev/=read;dev/=post",
        "dev/*=read",
        "UPPER=read",
        "dev/=read,",
        "dev\n=read",
        pytest.param("a" * 4001, id="exceeds_can_max"),
    ],
)
def test_parse_can_rejects_invalid_headers(bad_header: str) -> None:
    with pytest.raises(BoardValidationError) as exc_info:
        parse_can(bad_header)
    assert exc_info.value.code == "invalid_can"


def test_can_hashable_and_equality() -> None:
    a = Can.from_input({"*": [Permission.READ], "dev/": [Permission.READ]})
    b = parse_can("*=read")
    assert a == b
    assert hash(a) == hash(b)
    assert isinstance(a.summary(), Mapping)
