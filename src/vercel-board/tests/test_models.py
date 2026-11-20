from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

import pytest
from hypothesis import given, strategies as st

from vercel.board import (
    Actor,
    ActorKind,
    Author,
    BoardValidationError,
    BoardValueModel,
    Can,
    Discovery,
    DiscoverySync,
    Link,
    LinkFile,
    LinkSnapshot,
    Me,
    Mentioned,
    MentionedActor,
    Message,
    Permission,
    Profile,
    Via,
)

_PROFILE_NAME_RE = re.compile(r"\A[A-Za-z][A-Za-z0-9-]{0,31}\Z")
_VALID_PROFILE_NAMES = st.from_regex(_PROFILE_NAME_RE, fullmatch=True)
_INVALID_PROFILE_NAMES = st.text(max_size=40).filter(
    lambda s: _PROFILE_NAME_RE.fullmatch(s) is None
)


@given(name=_VALID_PROFILE_NAMES)
def test_profile_accepts_valid_names(name: str) -> None:
    profile = Profile(name=name)
    assert profile.name == name
    assert profile.avatar is None


@given(name=_INVALID_PROFILE_NAMES)
def test_profile_rejects_invalid_names(name: str) -> None:
    with pytest.raises(BoardValidationError):
        Profile(name=name)


def test_profile_rejects_trailing_newline() -> None:
    with pytest.raises(BoardValidationError):
        Profile(name="fixer\n")


def test_board_value_model_validation_errors_are_consistent() -> None:
    with pytest.raises(BoardValidationError):
        Profile(name="1invalid")
    with pytest.raises(BoardValidationError):
        Profile.model_validate({"name": "1invalid"})
    with pytest.raises(BoardValidationError):
        Profile.model_validate_json('{"name": "1invalid"}')
    with pytest.raises(BoardValidationError):
        Profile.model_validate_strings({"name": "1invalid"})
    with pytest.raises(BoardValidationError):
        Profile.model_validate({"name": "fixer", "extra_field": True})
    with pytest.raises(BoardValidationError):
        Profile.model_validate_json('{"name": "fixer", "extra_field": true}')
    profile = Profile(name="fixer")
    with pytest.raises(BoardValidationError):
        del profile.name


def test_profile_is_frozen_and_forbids_extras() -> None:
    profile = Profile(name="fixer", avatar="https://example.com/fixer.png")
    with pytest.raises(BoardValidationError):
        profile.name = "other"  # type: ignore[misc]
    with pytest.raises(BoardValidationError):
        Profile(name="fixer", unknown="nope")  # type: ignore[call-arg]


def test_message_immutability_and_frozen_mappings() -> None:
    created = datetime(2026, 11, 20, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    msg = Message(
        id="42",
        channel="errors-www",
        text="deploy failed",
        thread="42",
        labels={"status": "investigating"},
        reactions={"eyes": ("vercel:user:u_1",)},
        undelivered={"slack:#incidents": "timeout"},
        by=Author(actor="vercel:user:u_1+fixer", principal="vercel:user:u_1", profile="fixer"),
        author=Actor(
            id="vercel:user:u_1+fixer",
            kind=ActorKind.AGENT,
            handle="julian/fixer",
            name="fixer",
            via=Via(name="julian", environment=None),
            verified=True,
        ),
        created_at=created,
        links=(
            Link(
                url="https://slack.com/archives/C1/p1",
                message="10",
                snapshot=LinkSnapshot(
                    author="alice",
                    text="earlier context",
                    created_at=created,
                    files=(LinkFile(name="log.txt", url="https://files.example/log.txt"),),
                ),
            ),
        ),
        mentions=(
            Mentioned(
                written="@alice",
                actor=MentionedActor(id="vercel:user:u_2", handle="alice", name="Alice"),
            ),
        ),
        version=3,
        revision=1,
    )

    assert isinstance(msg, BoardValueModel)
    assert msg.created_at == datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
    assert msg.created_at.tzinfo is UTC
    assert msg.reactions["eyes"] == ("vercel:user:u_1",)

    with pytest.raises(BoardValidationError):
        msg.text = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        msg.labels["status"] = "resolved"  # type: ignore[index]
    with pytest.raises(TypeError):
        msg.reactions["eyes"] = ()  # type: ignore[index]
    with pytest.raises(TypeError):
        msg.undelivered["slack:#incidents"] = "ok"  # type: ignore[index]

    dumped = msg.model_dump()
    assert dumped["labels"] == {"status": "investigating"}
    assert dumped["reactions"] == {"eyes": ("vercel:user:u_1",)}


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(BoardValidationError):
        Message(
            id="1",
            channel="general",
            text="hi",
            thread="1",
            by=Author(actor="vercel:user:u_1", principal="vercel:user:u_1"),
            created_at=datetime(2026, 11, 20, 12, 0),
        )


def test_me_and_discovery_models() -> None:
    expires = datetime(2026, 11, 20, 15, 0, tzinfo=UTC)
    me = Me(
        actor="vercel:user:u_1+fixer",
        principal="vercel:user:u_1",
        kind=ActorKind.AGENT,
        handle="julian/fixer",
        name="fixer",
        profile="fixer",
        via=Via(name="www", environment="preview"),
        expires_at=expires,
        can={"dev/": [Permission.READ, Permission.POST]},
    )
    assert isinstance(me.can, Can)
    assert me.can.allows(Permission.POST, "dev/ci")
    assert not me.can.allows(Permission.READ, "ops")
    assert me.model_dump()["can"] == {"dev/": ["read", "post"]}

    discovery = Discovery(
        version="1",
        url="https://board.example.com",
        audience="https://board.example.com",
        team_id="team_123",
        project_id="prj_456",
        name="General",
        sync=(DiscoverySync(key="slack", platform="slack"),),
        wake_by_path=True,
    )
    assert discovery.sync[0].key == "slack"
    assert discovery.wake_by_path is True

    schema = Me.model_json_schema()
    assert "can" in schema["properties"]
