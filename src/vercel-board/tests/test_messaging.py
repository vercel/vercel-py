"""Behavioral and property-based tests for core Board messaging writes."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

import httpx2 as httpx
import pytest
from hypothesis import given, strategies as st

import vendor.respx as respx
from vercel.api import session
from vercel.board import (
    ActorKind,
    BoardApiError,
    BoardConflictError,
    BoardLabelConflictError,
    BoardNewerMessageError,
    BoardNotFoundError,
    BoardPermissionError,
    BoardResponseError,
    BoardRevisionConflictError,
    BoardServiceOptions,
    BoardValidationError,
    Message,
    Permission,
    board,
    static_credentials_factory,
    sync as board_sync,
)

HTTP_BOARD_URL = "https://board.example.test/api/board"
HOSTED_API_URL = "https://api.vercel.com"

UUIDV7_RE = re.compile(r"\A[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")

THREAD_UUID = "11111111-2222-3333-4444-555555555555"
THREAD_ID = f"thr_{THREAD_UUID}"
ROOT_MSG_ID = f"msg_{THREAD_UUID}_1"
REPLY_MSG_ID = f"msg_{THREAD_UUID}_2"
SECOND_REPLY_ID = f"msg_{THREAD_UUID}_3"
VALID_ATT_UUID = "att_11111111-2222-3333-4444-555555555555"
VALID_ATT_B64 = "AbCdEfGhIjKlMnOpQrStUv"

HTTP_MESSAGE_JSON: dict[str, Any] = {
    "id": "m_1",
    "channel": "errors-www",
    "text": "deploy failed on @[vercel:user:u_alice]",
    "re": None,
    "replyTo": None,
    "thread": "t_1",
    "labels": {"status": "investigating"},
    "reactions": {"eyes": ["vercel:user:u_alice"]},
    "by": {
        "actor": "vercel:user:u_123+fixer",
        "principal": "vercel:user:u_123",
        "profile": "fixer",
    },
    "author": {
        "id": "vercel:user:u_123+fixer",
        "kind": "agent",
        "handle": "fixer",
        "name": "Fixer",
        "avatar": "https://example.com/fixer.png",
        "via": {"name": "alice", "environment": None},
        "verified": True,
    },
    "noSync": False,
    "from": None,
    "createdAt": "2026-11-20T12:00:00Z",
    "editedAt": None,
    "deleted": False,
    "undelivered": {},
    "links": [
        {
            "url": "https://slack.com/archives/C1/p1",
            "message": None,
            "snapshot": {
                "author": "alice",
                "text": "alert",
                "createdAt": "2026-11-20T11:59:00Z",
                "files": [{"name": "trace.log", "url": "https://files.example/trace.log"}],
            },
            "unavailable": None,
        }
    ],
    "version": 1,
    "revision": 1,
}

HTTP_MENTIONS_JSON: list[dict[str, Any]] = [
    {
        "written": "@alice",
        "actor": {"id": "vercel:user:u_alice", "handle": "alice", "name": "Alice"},
        "candidates": [],
    },
    {
        "written": "@bob",
        "actor": None,
        "candidates": [
            {"id": "vercel:user:u_bob1", "handle": "bob", "name": "Bob 1"},
            {"id": "vercel:user:u_bob2", "handle": "bob", "name": "Bob 2"},
        ],
    },
]

HOSTED_ACTOR_JSON: dict[str, Any] = {
    "id": "vercel:user:u_123+fixer",
    "principal": {"type": "user", "userId": "u_123"},
    "profile": {"name": "fixer", "avatar": "https://example.com/fixer.png"},
}


def _hosted_message_json(
    *,
    msg_id: str = ROOT_MSG_ID,
    thread_id: str = THREAD_ID,
    sequence: int = 1,
    reply_to: int | None = None,
    text: str = "deploy failed",
    labels: dict[str, str] | None = None,
    no_sync: bool = False,
    version: int = 1,
    revision: int = 1,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": msg_id,
        "threadId": thread_id,
        "sequence": sequence,
        "text": text,
        "version": version,
        "revision": revision,
        "createdAt": 1795176000000,
        "createdBy": HOSTED_ACTOR_JSON,
        "reactions": {"eyes": 1},
        "reactors": {"eyes": ["vercel:user:u_alice"]},
        "links": [
            {
                "url": "https://github.com/vercel/board/pull/1",
                "message": None,
                "snapshot": None,
                "unavailable": None,
            }
        ],
    }
    if reply_to is not None:
        payload["replyTo"] = reply_to
    if labels is not None:
        payload["labels"] = labels
    if no_sync:
        payload["noSync"] = True
    return payload


@pytest.fixture
def clean_board_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "BOARD_URL",
        "BOARD_TOKEN",
        "BOARD_PROFILE",
        "BOARD_AVATAR",
        "VERCEL",
        "VERCEL_URL",
        "VERCEL_BRANCH_URL",
        "VERCEL_PROJECT_PRODUCTION_URL",
        "VERCEL_OIDC_TOKEN",
        "VERCEL_ORG_ID",
        "VERCEL_AUTH_TOKEN",
        "VERCEL_TOKEN",
        "VERCEL_AUTOMATION_BYPASS_SECRET",
        "ACTIONS_ID_TOKEN_REQUEST_URL",
        "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
    ):
        monkeypatch.delenv(var, raising=False)


# ---------------------------------------------------------------------------
# HTTP (BOARD_URL) protocol — all 7 methods (async + sync)
# ---------------------------------------------------------------------------


@respx.mock
async def test_http_protocol_write_loop_async(clean_board_env: None) -> None:
    post_route = respx.post(f"{HTTP_BOARD_URL}/messages").mock(
        side_effect=[
            httpx.Response(
                200,
                json={"message": HTTP_MESSAGE_JSON, "mentions": HTTP_MENTIONS_JSON},
            ),
            httpx.Response(
                200,
                json={
                    "message": {
                        **HTTP_MESSAGE_JSON,
                        "id": "m_2",
                        "text": "rolled back",
                        "re": "m_1",
                        "replyTo": "m_1",
                        "noSync": True,
                    },
                },
            ),
        ]
    )
    edit_route = respx.post(f"{HTTP_BOARD_URL}/messages/m_1/edit").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    delete_route = respx.post(f"{HTTP_BOARD_URL}/messages/m_2/delete").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    react_route = respx.post(f"{HTTP_BOARD_URL}/messages/m_1/reactions").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    label_route = respx.post(f"{HTTP_BOARD_URL}/messages/m_1/labels").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    posted_route = respx.get(f"{HTTP_BOARD_URL}/posts/deploy-a1b2c3").mock(
        return_value=httpx.Response(200, json={"message": HTTP_MESSAGE_JSON})
    )

    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("http-tok"),
    )
    async with session(service_options=[opts]):
        handle = board().as_profile(
            "fixer",
            avatar="https://example.com/fixer.png",
            can={
                "errors-www": [Permission.READ, Permission.POST, Permission.REACT, Permission.LABEL]
            },
        )

        # 1. post
        posted = await handle.post(
            "errors-www",
            "deploy failed on @alice @bob",
            labels={"status": "investigating"},
            attachments=[VALID_ATT_UUID],
            sync=["slack:#incidents"],
            idempotency_key="deploy-a1b2c3",
        )
        assert isinstance(posted, Message)
        assert posted.id == "m_1"
        assert posted.channel == "errors-www"
        assert posted.created_at == datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
        assert isinstance(posted.labels, MappingProxyType)
        assert posted.labels["status"] == "investigating"
        assert posted.reactions["eyes"] == ("vercel:user:u_alice",)
        assert posted.author is not None and posted.author.kind is ActorKind.AGENT
        assert len(posted.links) == 1
        assert posted.links[0].snapshot is not None
        assert posted.links[0].snapshot.files[0].name == "trace.log"
        assert len(posted.mentions) == 2
        assert posted.mentions[0].written == "@alice"
        assert posted.mentions[0].actor is not None
        assert posted.mentions[0].actor.id == "vercel:user:u_alice"
        assert posted.mentions[1].actor is None
        assert len(posted.mentions[1].candidates) == 2

        with pytest.raises(BoardValidationError):
            posted.text = "mutated"  # type: ignore[misc]

        post_req = post_route.calls[0].request
        assert post_req.headers["authorization"] == "Bearer http-tok"
        assert post_req.headers["x-board-profile"] == 'fixer;avatar="https://example.com/fixer.png"'
        assert post_req.headers["x-board-can"] == "errors-www=post,react,label"
        assert json.loads(post_req.content) == {
            "channel": "errors-www",
            "text": "deploy failed on @alice @bob",
            "labels": {"status": "investigating"},
            "attachments": [VALID_ATT_UUID],
            "sync": ["slack:#incidents"],
            "id": "deploy-a1b2c3",
        }

        # 2. reply
        replied = await handle.reply(
            "m_1",
            "rolled back",
            labels={"action": "rollback"},
            attachments=[VALID_ATT_B64],
            no_sync=True,
            idempotency_key="reply-1",
            after="m_1",
        )
        assert replied.id == "m_2"
        assert replied.re == "m_1"
        assert replied.no_sync is True
        assert json.loads(post_route.calls[1].request.content) == {
            "re": "m_1",
            "text": "rolled back",
            "labels": {"action": "rollback"},
            "attachments": [VALID_ATT_B64],
            "noSync": True,
            "id": "reply-1",
            "after": "m_1",
        }

        # 3. edit
        await handle.edit("m_1", "deploy failed (resolved)", revision=1)
        assert json.loads(edit_route.calls.last.request.content) == {
            "text": "deploy failed (resolved)",
            "revision": 1,
        }

        # 4. delete
        await handle.delete("m_2", revision=1)
        assert json.loads(delete_route.calls.last.request.content) == {"revision": 1}

        # 5. react (normalizes emoji + remove flag)
        await handle.react("m_1", "👍")
        assert json.loads(react_route.calls[0].request.content) == {"emoji": "thumbs_up"}
        await handle.react("m_1", ":eyes:", remove=True)
        assert json.loads(react_route.calls[1].request.content) == {
            "emoji": "eyes",
            "remove": True,
        }

        # 6. label
        await handle.label(
            "m_1",
            set={"status": "resolved"},
            unset=["needs_triage"],
            if_labels={"status": "investigating", "stale": None},
        )
        assert json.loads(label_route.calls.last.request.content) == {
            "set": {"status": "resolved"},
            "unset": ["needs_triage"],
            "if": {"status": "investigating", "stale": None},
        }

        # 7. get_posted
        fetched = await handle.get_posted("deploy-a1b2c3")
        assert fetched.id == "m_1"
        assert posted_route.called


@respx.mock
def test_http_protocol_write_loop_sync(clean_board_env: None) -> None:
    respx.post(f"{HTTP_BOARD_URL}/messages").mock(
        side_effect=[
            httpx.Response(200, json={"message": HTTP_MESSAGE_JSON}),
            httpx.Response(
                200,
                json={"message": {**HTTP_MESSAGE_JSON, "id": "m_2", "re": "m_1"}},
            ),
        ]
    )
    respx.post(f"{HTTP_BOARD_URL}/messages/m_1/edit").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    respx.post(f"{HTTP_BOARD_URL}/messages/m_2/delete").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    respx.post(f"{HTTP_BOARD_URL}/messages/m_1/reactions").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    respx.post(f"{HTTP_BOARD_URL}/messages/m_1/labels").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    respx.get(f"{HTTP_BOARD_URL}/posts/deploy-sync").mock(
        return_value=httpx.Response(200, json={"message": HTTP_MESSAGE_JSON})
    )

    opts = board_sync.BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=board_sync.static_credentials_factory("sync-tok"),
    )
    with session(service_options=[opts]):
        handle = board_sync.board().as_profile("fixer")
        posted = handle.post("errors-www", "hello sync", idempotency_key="deploy-sync")
        assert posted.id == "m_1"
        replied = handle.reply(posted.id, "reply sync")
        assert replied.id == "m_2"
        handle.edit(posted.id, "updated sync")
        handle.delete(replied.id)
        handle.react(posted.id, "+1")
        handle.label(posted.id, set={"status": "done"})
        found = handle.get_posted("deploy-sync")
        assert found.id == "m_1"


# ---------------------------------------------------------------------------
# Vercel Hosted (/v1/messageboard) protocol — all 7 methods (async + sync)
# ---------------------------------------------------------------------------


@respx.mock
async def test_hosted_protocol_write_loop_async(clean_board_env: None) -> None:
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/channels/chn_1").mock(
        return_value=httpx.Response(200, json={"channel": {"id": "chn_1", "name": "errors-www"}})
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/actors").mock(
        return_value=httpx.Response(
            200,
            json={
                "actors": [
                    {
                        "id": "vercel:user:u_alice",
                        "kind": "person",
                        "handle": "alice",
                        "name": "Alice",
                    },
                    {
                        "id": "vercel:user:u_bob1",
                        "kind": "person",
                        "handle": "bob",
                        "name": "Bob 1",
                    },
                    {
                        "id": "vercel:user:u_bob2",
                        "kind": "person",
                        "handle": "bob",
                        "name": "Bob 2",
                    },
                ],
                "cursor": None,
            },
        )
    )
    post_thread_route = respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads").mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": _hosted_message_json(
                    text="deploy failed on @[vercel:user:u_alice] @bob",
                    labels={"status": "investigating"},
                    no_sync=True,
                ),
            },
        )
    )
    get_msg_route = respx.get(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/messages/{ROOT_MSG_ID}"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": _hosted_message_json(),
                "channel": {"id": "chn_1", "name": "errors-www"},
            },
        )
    )
    reply_route = respx.post(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": _hosted_message_json(
                    msg_id=REPLY_MSG_ID, sequence=2, text="rolled back"
                ),
            },
        )
    )
    reply_label_route = respx.post(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/2/labels"
    ).mock(
        return_value=httpx.Response(
            200,
            json={"meta": {"labels": {"action": "rollback"}, "version": 2}},
        )
    )
    edit_route = respx.patch(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/1"
    ).mock(return_value=httpx.Response(200, json={"ok": True}))
    delete_route = respx.post(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/2/delete"
    ).mock(return_value=httpx.Response(200, json={"ok": True}))
    react_route = respx.post(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/1/reactions"
    ).mock(return_value=httpx.Response(200, json={"ok": True}))
    label_route = respx.post(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/1/labels"
    ).mock(return_value=httpx.Response(200, json={"ok": True}))
    posted_route = respx.get(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/posts/deploy-a1b2c3"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": _hosted_message_json(),
            },
        )
    )

    opts = BoardServiceOptions(
        team="team_abc",
        credentials_factory=static_credentials_factory("hosted-tok"),
    )
    async with session(service_options=[opts]):
        handle = board().as_profile(
            "fixer",
            avatar="https://example.com/fixer.png",
            can={
                "errors-www": [Permission.READ, Permission.POST, Permission.REACT, Permission.LABEL]
            },
        )

        # 1. post (with real @handle mention settlement against hosted /actors)
        posted = await handle.post(
            "errors-www",
            "deploy failed on @alice @bob",
            labels={"status": "investigating"},
            attachments=[VALID_ATT_UUID],
            no_sync=True,
            idempotency_key="deploy-a1b2c3",
        )
        assert posted.id == ROOT_MSG_ID
        assert posted.channel == "errors-www"
        assert posted.thread == THREAD_ID
        assert posted.re is None
        assert posted.no_sync is True
        assert posted.labels["status"] == "investigating"
        assert posted.reactions["eyes"] == ("vercel:user:u_alice",)
        assert posted.created_at.tzinfo is UTC
        assert len(posted.mentions) == 2
        assert posted.mentions[0].written == "@alice"
        assert posted.mentions[0].actor is not None
        assert posted.mentions[0].actor.id == "vercel:user:u_alice"
        assert posted.mentions[1].written == "@bob"
        assert posted.mentions[1].actor is None
        assert len(posted.mentions[1].candidates) == 2

        post_req = post_thread_route.calls.last.request
        assert post_req.url.params["teamId"] == "team_abc"
        assert post_req.headers["x-board-profile"] == "fixer"
        assert post_req.headers["x-board-avatar"] == "https://example.com/fixer.png"
        assert post_req.headers["x-board-can"] == "errors-www=post,react,label"
        post_body = json.loads(post_req.content)
        assert UUIDV7_RE.fullmatch(post_body["operationId"]) is not None
        assert post_body["channel"] == "errors-www"
        assert post_body["text"] == "deploy failed on @[vercel:user:u_alice] @bob"
        assert post_body["labels"] == {"status": "investigating"}
        assert post_body["attachments"] == [VALID_ATT_UUID]
        assert post_body["noSync"] is True
        assert post_body["clientId"] == "deploy-a1b2c3"

        # 2. reply (with labels & after)
        replied = await handle.reply(
            ROOT_MSG_ID,
            "rolled back",
            labels={"action": "rollback"},
            after=ROOT_MSG_ID,
            idempotency_key="reply-key-1",
        )
        assert replied.id == REPLY_MSG_ID
        assert replied.re == ROOT_MSG_ID
        assert replied.reply_to is None
        assert replied.labels["action"] == "rollback"
        assert replied.version == 2
        assert get_msg_route.calls.last.request.url.params["include"] == "channel"
        reply_body = json.loads(reply_route.calls.last.request.content)
        assert UUIDV7_RE.fullmatch(reply_body["operationId"]) is not None
        assert reply_body["text"] == "rolled back"
        assert reply_body["after"] == 1
        assert reply_body["clientId"] == "reply-key-1"
        assert "replyTo" not in reply_body
        reply_label_body = json.loads(reply_label_route.calls.last.request.content)
        assert UUIDV7_RE.fullmatch(reply_label_body["operationId"]) is not None
        assert reply_label_body["set"] == {"action": "rollback"}

        # 3. edit
        await handle.edit(ROOT_MSG_ID, "updated text", revision=1)
        edit_body = json.loads(edit_route.calls.last.request.content)
        assert UUIDV7_RE.fullmatch(edit_body["operationId"]) is not None
        assert edit_body["text"] == "updated text"
        assert edit_body["revision"] == 1

        # 4. delete
        await handle.delete(REPLY_MSG_ID, revision=1)
        del_body = json.loads(delete_route.calls.last.request.content)
        assert UUIDV7_RE.fullmatch(del_body["operationId"]) is not None
        assert del_body["revision"] == 1

        # 5. react
        await handle.react(ROOT_MSG_ID, "eyes", remove=True)
        react_body = json.loads(react_route.calls.last.request.content)
        assert UUIDV7_RE.fullmatch(react_body["operationId"]) is not None
        assert react_body["emoji"] == "eyes"
        assert react_body["remove"] is True

        # 6. label
        await handle.label(
            ROOT_MSG_ID,
            set={"status": "resolved"},
            unset=["wip"],
            if_labels={"status": "investigating"},
        )
        lbl_body = json.loads(label_route.calls.last.request.content)
        assert UUIDV7_RE.fullmatch(lbl_body["operationId"]) is not None
        assert lbl_body["set"] == {"status": "resolved"}
        assert lbl_body["unset"] == ["wip"]
        assert lbl_body["if"] == {"status": "investigating"}

        # 7. get_posted (uses cached channel chn_1 -> errors-www)
        fetched = await handle.get_posted("deploy-a1b2c3")
        assert fetched.id == ROOT_MSG_ID
        assert fetched.channel == "errors-www"
        assert posted_route.called


@respx.mock
async def test_hosted_protocol_sync_rejected_501_and_blind_post(clean_board_env: None) -> None:
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads").mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID},
                "message": {"id": ROOT_MSG_ID, "threadId": THREAD_ID, "sequence": 1},
            },
        )
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/me").mock(
        return_value=httpx.Response(
            200,
            json={
                "teamId": "team_abc",
                "actor": {
                    "id": "vercel:user:u_123",
                    "principal": {"type": "user", "userId": "u_123"},
                },
            },
        )
    )

    opts = BoardServiceOptions(
        team="team_abc",
        credentials_factory=static_credentials_factory("hosted-tok"),
    )
    async with session(service_options=[opts]):
        handle = board().as_profile("FixerBot")

        with pytest.raises(BoardApiError) as exc_info:
            await handle.post("errors-www", "hello", sync=["slack:#incidents"])
        assert exc_info.value.status_code == 501
        assert exc_info.value.code == "not_supported"

        blind_msg = await handle.post("errors-www", "write-only post", labels={"env": "prod"})
        assert blind_msg.id == ROOT_MSG_ID
        assert blind_msg.blind is True
        assert blind_msg.text == "write-only post"
        assert blind_msg.labels["env"] == "prod"
        assert blind_msg.by.actor == "vercel:user:u_123+fixerbot"
        assert blind_msg.by.profile == "FixerBot"


@respx.mock
def test_hosted_protocol_write_loop_sync(clean_board_env: None) -> None:
    import hashlib

    non_ascii_key = "café key / with spaces"
    expected_client_id = f"h.{hashlib.sha256(non_ascii_key.encode('utf-8')).hexdigest()[:64]}"

    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    post_route = respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads").mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": _hosted_message_json(),
            },
        )
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/messages/{ROOT_MSG_ID}").mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": _hosted_message_json(),
                "channel": {"id": "chn_1", "name": "errors-www"},
            },
        )
    )
    respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages").mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": _hosted_message_json(msg_id=REPLY_MSG_ID, sequence=2, text="sync reply"),
            },
        )
    )
    respx.patch(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/1"
    ).mock(return_value=httpx.Response(200, json={"ok": True}))
    respx.post(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/2/delete"
    ).mock(return_value=httpx.Response(200, json={"ok": True}))
    respx.post(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/1/reactions"
    ).mock(return_value=httpx.Response(200, json={"ok": True}))
    respx.post(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/1/labels"
    ).mock(return_value=httpx.Response(200, json={"ok": True}))
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/channels/chn_1").mock(
        return_value=httpx.Response(200, json={"channel": {"id": "chn_1", "name": "errors-www"}})
    )
    hashed_route = respx.get(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/posts/{expected_client_id}"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": _hosted_message_json(),
            },
        )
    )

    opts = board_sync.BoardServiceOptions(
        team="team_abc",
        credentials_factory=board_sync.static_credentials_factory("hosted-sync-tok"),
    )
    with session(service_options=[opts]):
        handle = board_sync.board()
        msg = handle.post("errors-www", "hello hosted sync", idempotency_key=non_ascii_key)
        assert msg.id == ROOT_MSG_ID
        assert json.loads(post_route.calls.last.request.content)["clientId"] == expected_client_id
        replied = handle.reply(msg.id, "sync reply")
        assert replied.id == REPLY_MSG_ID
        handle.edit(msg.id, "edited hosted sync", revision=1)
        handle.delete(replied.id, revision=1)
        handle.react(msg.id, "eyes")
        handle.label(msg.id, set={"status": "done"})
        found = handle.get_posted(non_ascii_key)
        assert found.id == ROOT_MSG_ID
        assert hashed_route.called


# ---------------------------------------------------------------------------
# Conflict, not-found, permission errors & 429 retry on writes
# ---------------------------------------------------------------------------


@respx.mock
async def test_write_error_mapping_and_429_retry_reuses_keys(
    clean_board_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("vercel.board._internal.api_client.BASE_RETRY_DELAY_SECONDS", 0.001)

    # Hosted 429 retry on post must reuse the exact same operationId and clientId
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/channels/chn_1").mock(
        return_value=httpx.Response(200, json={"channel": {"id": "chn_1", "name": "errors-www"}})
    )
    post_route = respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads").mock(
        side_effect=[
            httpx.Response(
                429, headers={"retry-after": "1"}, json={"error": {"code": "rate_limited"}}
            ),
            httpx.Response(
                200,
                json={
                    "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                    "message": _hosted_message_json(),
                },
            ),
        ]
    )
    opts = BoardServiceOptions(
        team="team_abc",
        credentials_factory=static_credentials_factory("hosted-tok"),
    )
    async with session(service_options=[opts]):
        msg = await board().post("errors-www", "retry me", idempotency_key="retry-key")
        assert msg.id == ROOT_MSG_ID
        assert len(post_route.calls) == 2
        first_body = json.loads(post_route.calls[0].request.content)
        second_body = json.loads(post_route.calls[1].request.content)
        assert first_body["operationId"] == second_body["operationId"]
        assert first_body["clientId"] == second_body["clientId"] == "retry-key"


@respx.mock
@pytest.mark.parametrize(
    ("status", "code", "expected_cls"),
    [
        (409, "revision_conflict", BoardRevisionConflictError),
        (409, "newer_message", BoardNewerMessageError),
        (409, "label_conflict", BoardLabelConflictError),
        (409, "duplicate_id", BoardConflictError),
        (404, "message_not_found", BoardNotFoundError),
        (404, "post_not_found", BoardNotFoundError),
        (403, "forbidden", BoardPermissionError),
        (403, "board_not_granted", BoardPermissionError),
    ],
)
async def test_write_error_subclass_mapping(
    clean_board_env: None,
    status: int,
    code: str,
    expected_cls: type[BoardApiError],
) -> None:
    respx.post(f"{HTTP_BOARD_URL}/messages").mock(
        return_value=httpx.Response(
            status, json={"error": {"code": code, "message": f"err {code}"}}
        )
    )
    opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        with pytest.raises(expected_cls) as exc_info:
            await board().post("errors-www", "hello")
        assert exc_info.value.code == code
        assert exc_info.value.status_code == status


# ---------------------------------------------------------------------------
# Client-side validation (before any I/O) & Hypothesis property tests
# ---------------------------------------------------------------------------


async def test_validation_rejects_invalid_write_arguments_before_io(
    clean_board_env: None,
) -> None:
    # No respx mock & no credentials — any I/O would fail differently than BoardValidationError
    opts = BoardServiceOptions(url=HTTP_BOARD_URL)
    async with session(service_options=[opts]):
        handle = board()

        # sync + no_sync together
        with pytest.raises(BoardValidationError):
            await handle.post("dev", "hello", sync=["slack:#inc"], no_sync=True)

        # empty/whitespace text without attachments
        with pytest.raises(BoardValidationError):
            await handle.post("dev", "   ")
        with pytest.raises(BoardValidationError):
            await handle.reply("m_1", "")
        with pytest.raises(BoardValidationError):
            await handle.edit("m_1", "  \t ")

        # revision validation (reject bool, 0, and negative)
        with pytest.raises(BoardValidationError):
            await handle.edit("m_1", "ok", revision=True)
        with pytest.raises(BoardValidationError):
            await handle.edit("m_1", "ok", revision=0)
        with pytest.raises(BoardValidationError):
            await handle.edit("m_1", "ok", revision=-1)
        with pytest.raises(BoardValidationError):
            await handle.delete("m_1", revision=False)
        with pytest.raises(BoardValidationError):
            await handle.delete("m_1", revision=0)
        with pytest.raises(BoardValidationError):
            await handle.delete("m_1", revision=-2)

        # attachment ID validation (must match upstream 22-char base64url or att_<uuid>)
        for bad_att in ("att_1", "att_not-a-uuid", "short", f"{VALID_ATT_B64}\n"):
            with pytest.raises(BoardValidationError):
                await handle.post("dev", "hi", attachments=[bad_att])
            with pytest.raises(BoardValidationError):
                await handle.reply("m_1", "hi", attachments=[bad_att])

        # label() requires set or unset
        with pytest.raises(BoardValidationError):
            await handle.label("m_1")
        with pytest.raises(BoardValidationError):
            await handle.label("m_1", set={}, unset=[])

        # empty message_id / idempotency_key / emoji
        with pytest.raises(BoardValidationError):
            await handle.reply("", "hi")
        with pytest.raises(BoardValidationError):
            await handle.get_posted("   ")
        with pytest.raises(BoardValidationError):
            await handle.react("m_1", "not an emoji!")
        with pytest.raises(BoardValidationError):
            await handle.react("m_1", "eyes\n")


_VALID_SEGMENT = st.from_regex(r"\A[a-z0-9][a-z0-9_-]{0,12}\Z", fullmatch=True)
_VALID_CHANNEL = st.lists(_VALID_SEGMENT, min_size=1, max_size=4).map("/".join)


@given(channel=_VALID_CHANNEL)
def test_property_channel_path_rejects_trailing_newline(channel: str) -> None:
    handle = board_sync.board()
    with session(service_options=[board_sync.BoardServiceOptions(url=HTTP_BOARD_URL)]):
        with pytest.raises(BoardValidationError):
            handle.post(f"{channel}\n", "hello")


@given(
    suffix=st.from_regex(r"\A[a-z0-9_.-]{1,20}\Z", fullmatch=True),
    value=st.text(
        alphabet=st.characters(blacklist_categories=("Cc", "Cs")),
        min_size=1,
        max_size=20,
    ),
)
def test_property_reserved_board_label_prefix_rejected(suffix: str, value: str) -> None:
    reserved_key = f"board.{suffix}"
    handle = board_sync.board()
    with session(service_options=[board_sync.BoardServiceOptions(url=HTTP_BOARD_URL)]):
        with pytest.raises(BoardValidationError):
            handle.post("dev", "hello", labels={reserved_key: value})
        with pytest.raises(BoardValidationError):
            handle.label("m_1", set={reserved_key: value})
        with pytest.raises(BoardValidationError):
            handle.label("m_1", unset=[reserved_key])


@given(raw=st.text(min_size=1, max_size=20))
def test_property_bare_string_rejected_for_sequence_args(raw: str) -> None:
    handle = board_sync.board()
    with session(service_options=[board_sync.BoardServiceOptions(url=HTTP_BOARD_URL)]):
        with pytest.raises(BoardValidationError):
            handle.post("dev", "hello", attachments=raw)  # type: ignore[arg-type]
        with pytest.raises(BoardValidationError):
            handle.post("dev", "hello", sync=raw)  # type: ignore[arg-type]
        with pytest.raises(BoardValidationError):
            handle.label("m_1", unset=raw)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Review-hardening behavioral tests (path traversal, hosted fallbacks,
# reactions pagination, malformed 200 responses, epoch-ms timestamps)
# ---------------------------------------------------------------------------


async def test_dot_segments_rejected_before_io(clean_board_env: None) -> None:
    # No respx mocks: any I/O attempt would fail instead of raising BoardValidationError
    for opts in (
        BoardServiceOptions(url=HTTP_BOARD_URL),
        BoardServiceOptions(team="team_abc"),
    ):
        async with session(service_options=[opts]):
            handle = board()
            for dot in (".", ".."):
                with pytest.raises(BoardValidationError):
                    await handle.post("dev", "hello", idempotency_key=dot)
                with pytest.raises(BoardValidationError):
                    await handle.reply("m_1", "hello", idempotency_key=dot)
                with pytest.raises(BoardValidationError):
                    await handle.get_posted(dot)
                with pytest.raises(BoardValidationError):
                    await handle.reply(dot, "hello")
                with pytest.raises(BoardValidationError):
                    await handle.reply("m_1", "hello", after=dot)
                with pytest.raises(BoardValidationError):
                    await handle.edit(dot, "hello")
                with pytest.raises(BoardValidationError):
                    await handle.delete(dot)
                with pytest.raises(BoardValidationError):
                    await handle.react(dot, "eyes")
                with pytest.raises(BoardValidationError):
                    await handle.label(dot, set={"status": "open"})


@respx.mock
async def test_hosted_mentions_normalize_imported_external_actors(clean_board_env: None) -> None:
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/actors").mock(
        return_value=httpx.Response(
            200,
            json={
                "actors": [
                    {
                        "id": "vercel:project:prj_sup:production+slack:U1",
                        "kind": "external",
                        "handle": "Malte Ubl",
                        "name": "Malte Ubl",
                        "via": {"name": "support", "environment": "production"},
                    },
                    {
                        "id": "vercel:project:prj_sup:production+slack:U2",
                        "kind": "external",
                        "handle": "A\u20ddB",
                        "name": "AB",
                        "via": {"name": "support", "environment": "production"},
                    },
                    {
                        "id": "vercel:user:u_ada",
                        "kind": "person",
                        "handle": "ada",
                        "name": "Ada",
                    },
                    {
                        "id": "vercel:user:u_kay",
                        "kind": "person",
                        "handle": "kay",
                        "name": "Kay",
                    },
                ],
                "cursor": None,
            },
        )
    )
    post_route = respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                    "message": _hosted_message_json(
                        text="ping @malte-ubl and @[vercel:project:prj_sup:production+slack:U1]"
                    ),
                },
            ),
            httpx.Response(
                200,
                json={
                    "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                    "message": _hosted_message_json(
                        text=(
                            "cc @\u212aay 你好@[vercel:user:u_ada] "
                            "@[vercel:user:u_ada]:\u00a0thanks "
                            "@ada:\u0085thanks "
                            "@[vercel:project:prj_sup:production+slack:U2]"
                        )
                    ),
                },
            ),
        ]
    )

    opts = BoardServiceOptions(
        team="team_abc",
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        posted = await board().post("errors-www", "ping @malte-ubl and @support/malte-ubl")
        sent_body = json.loads(post_route.calls[0].request.content)
        assert (
            sent_body["text"] == "ping @malte-ubl and @[vercel:project:prj_sup:production+slack:U1]"
        )
        assert len(posted.mentions) == 2
        assert posted.mentions[0].written == "@malte-ubl"
        assert posted.mentions[0].actor is None
        assert len(posted.mentions[0].candidates) == 1
        assert posted.mentions[0].candidates[0].handle == "support/malte-ubl"
        assert posted.mentions[1].written == "@support/malte-ubl"
        assert posted.mentions[1].actor is not None
        assert posted.mentions[1].actor.id == "vercel:project:prj_sup:production+slack:U1"

        # ASCII mention boundaries, Kelvin sign untouched, NBSP vs NEL after colon, Me mark stripped
        posted_unicode = await board().post(
            "errors-www",
            "cc @\u212aay 你好@ada @ada:\u00a0thanks @ada:\u0085thanks @support/ab",
        )
        sent_unicode = json.loads(post_route.calls[1].request.content)["text"]
        assert sent_unicode == (
            "cc @\u212aay 你好@[vercel:user:u_ada] "
            "@[vercel:user:u_ada]:\u00a0thanks "
            "@ada:\u0085thanks "
            "@[vercel:project:prj_sup:production+slack:U2]"
        )
        assert [m.written for m in posted_unicode.mentions] == ["@ada", "@support/ab"]


@respx.mock
async def test_hosted_mentions_mask_tilde_blocks_and_enforce_utf16_length(
    clean_board_env: None,
) -> None:
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/actors").mock(
        return_value=httpx.Response(
            200,
            json={
                "actors": [
                    {
                        "id": "vercel:user:u_ada",
                        "kind": "person",
                        "handle": "ada",
                        "name": "Ada",
                    }
                ],
                "cursor": None,
            },
        )
    )
    # 1. Tilde-fenced structured block is masked; prose @ada outside is rewritten
    block_text = 'hi @ada\n~~~json#example@1\n{"owner":"@ada"}\n~~~'
    expected_block_text = 'hi @[vercel:user:u_ada]\n~~~json#example@1\n{"owner":"@ada"}\n~~~'

    # 2. Astral emoji (2 UTF-16 code units each): 4997 * 2 = 9994 + len(" @ada") (5) = 9999 UTF-16
    #    Rewriting @ada -> @[vercel:user:u_ada] adds 16 units -> 10015 > 10000 -> keep original!
    near_limit_text = ("🚀" * 4997) + " @ada"
    assert len(near_limit_text.encode("utf-16-le")) // 2 == 9999

    post_route = respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                    "message": _hosted_message_json(text=expected_block_text),
                },
            ),
            httpx.Response(
                200,
                json={
                    "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                    "message": _hosted_message_json(text=near_limit_text),
                },
            ),
        ]
    )

    opts = BoardServiceOptions(
        team="team_abc",
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        m1 = await board().post("errors-www", block_text)
        assert json.loads(post_route.calls[0].request.content)["text"] == expected_block_text
        assert m1.mentions[0].actor is not None
        assert m1.mentions[0].actor.id == "vercel:user:u_ada"

        m2 = await board().post("errors-www", near_limit_text)
        assert json.loads(post_route.calls[1].request.content)["text"] == near_limit_text
        assert len(m2.mentions) == 1
        assert m2.mentions[0].actor is None
        assert len(m2.mentions[0].candidates) == 1
        assert m2.mentions[0].candidates[0].id == "vercel:user:u_ada"


@respx.mock
async def test_hosted_reply_include_channel_fallback_and_best_effort_landed_channel(
    clean_board_env: None,
) -> None:
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    # 1. Committed keyed post succeeds, but ancillary channel lookup fails with 500 or ConnectError
    respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads").mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_transient"},
                "message": _hosted_message_json(),
            },
        )
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/channels/chn_transient").mock(
        side_effect=[
            httpx.Response(500, json={"error": {"code": "internal_error"}}),
            httpx.ConnectError("connection reset"),
        ]
    )

    # 2. Reply: include=channel returns 400, fallback without include succeeds + resolves channel
    msg_route = respx.get(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/messages/{ROOT_MSG_ID}"
    ).mock(
        side_effect=[
            httpx.Response(
                400, json={"error": {"code": "bad_request", "message": "unknown include"}}
            ),
            httpx.Response(
                200,
                json={
                    "thread": {"id": THREAD_ID, "channelId": "chn_2"},
                    "message": _hosted_message_json(),
                },
            ),
        ]
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/channels/chn_2").mock(
        return_value=httpx.Response(200, json={"channel": {"id": "chn_2", "name": "ops"}})
    )
    respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages").mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_2"},
                "message": _hosted_message_json(msg_id=REPLY_MSG_ID, sequence=2, text="ack"),
            },
        )
    )

    opts = BoardServiceOptions(
        team="team_abc",
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        posted = await board().post("errors-www", "committed", idempotency_key="key-transient")
        assert posted.channel == "errors-www"
        posted_conn_err = await board().post(
            "errors-www", "committed-2", idempotency_key="key-transient-2"
        )
        assert posted_conn_err.channel == "errors-www"

        replied = await board().reply(ROOT_MSG_ID, "ack")
        assert replied.channel == "ops"
        assert len(msg_route.calls) == 2
        assert msg_route.calls[0].request.url.params.get("include") == "channel"
        assert "include" not in msg_route.calls[1].request.url.params


@respx.mock
async def test_hosted_reactions_exceeding_inline_cap_fetches_full_reactors(
    clean_board_env: None,
) -> None:
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/channels/chn_1").mock(
        return_value=httpx.Response(200, json={"channel": {"id": "chn_1", "name": "errors-www"}})
    )
    msg_with_truncated_reactors = {
        **_hosted_message_json(),
        "reactions": {"eyes": 2},
        "reactors": {"eyes": ["vercel:user:u_1"]},
    }
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/posts/many-reacts").mock(
        return_value=httpx.Response(
            200,
            json={
                "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                "message": msg_with_truncated_reactors,
            },
        )
    )
    reactions_route = respx.get(
        f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads/{THREAD_ID}/messages/1/reactions"
    ).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "reactions": [
                        {
                            "emoji": "eyes",
                            "actor": {
                                "id": "vercel:user:u_1",
                                "principal": {"type": "user", "userId": "u_1"},
                            },
                        }
                    ],
                    "cursor": "cur_2",
                },
            ),
            httpx.Response(
                200,
                json={
                    "reactions": [
                        {
                            "emoji": "eyes",
                            "actor": {
                                "id": "vercel:user:u_2",
                                "principal": {"type": "user", "userId": "u_2"},
                            },
                        }
                    ],
                    "cursor": None,
                },
            ),
        ]
    )

    opts = BoardServiceOptions(
        team="team_abc",
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[opts]):
        fetched = await board().get_posted("many-reacts")
        assert fetched.reactions["eyes"] == ("vercel:user:u_1", "vercel:user:u_2")
        assert len(reactions_route.calls) == 2


@respx.mock
async def test_malformed_200_raises_board_response_error_and_hosted_uses_epoch_ms(
    clean_board_env: None,
) -> None:
    # HTTP 200 with invalid or out-of-range ISO createdAt -> BoardResponseError with .response
    respx.post(f"{HTTP_BOARD_URL}/messages").mock(
        side_effect=[
            httpx.Response(
                200,
                json={"message": {**HTTP_MESSAGE_JSON, "createdAt": "not-a-timestamp"}},
            ),
            httpx.Response(
                200,
                json={"message": {**HTTP_MESSAGE_JSON, "createdAt": "0001-01-01T00:00:00+01:00"}},
            ),
        ]
    )
    http_opts = BoardServiceOptions(
        url=HTTP_BOARD_URL,
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[http_opts]):
        with pytest.raises(BoardResponseError) as exc_info:
            await board().post("errors-www", "hello")
        assert exc_info.value.response is not None
        assert exc_info.value.status_code == 200

        with pytest.raises(BoardResponseError) as iso_overflow_exc:
            await board().post("errors-www", "hello overflow")
        assert iso_overflow_exc.value.response is not None
        assert iso_overflow_exc.value.status_code == 200

    # Hosted numeric createdAt is explicitly epoch milliseconds (1000 ms == 1970-01-01T00:00:01Z),
    # and out-of-range epoch-ms (10**40) raises BoardResponseError with .response attached.
    respx.get(f"{HOSTED_API_URL}/v1/messageboard/boards").mock(
        return_value=httpx.Response(200, json={"defaultBoardId": "brd_1", "boards": []})
    )
    respx.post(f"{HOSTED_API_URL}/v1/messageboard/boards/brd_1/threads").mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                    "message": {**_hosted_message_json(), "createdAt": 1000},
                },
            ),
            httpx.Response(
                200,
                json={
                    "thread": {"id": THREAD_ID, "channelId": "chn_1"},
                    "message": {**_hosted_message_json(), "createdAt": 10**40},
                },
            ),
        ]
    )
    hosted_opts = BoardServiceOptions(
        team="team_abc",
        credentials_factory=static_credentials_factory("tok"),
    )
    async with session(service_options=[hosted_opts]):
        msg = await board().post("errors-www", "epoch ms test")
        assert msg.created_at == datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC)

        with pytest.raises(BoardResponseError) as overflow_exc:
            await board().post("errors-www", "overflow timestamp")
        assert overflow_exc.value.response is not None
        assert overflow_exc.value.status_code == 200
