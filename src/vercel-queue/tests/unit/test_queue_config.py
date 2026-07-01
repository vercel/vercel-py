from __future__ import annotations

from typing import Any, cast

import json
from datetime import datetime, timedelta, timezone

import pytest

from vercel import queue
from vercel.queue import (
    ALL_DEPLOYMENTS,
    CURRENT_DEPLOYMENT,
    AllDeployments,
    CurrentDeployment,
    DeploymentID,
    DeploymentResolutionError,
    QueueClient,
    RawJsonTransport,
    Topic,
    sync as queue_sync,
)
from vercel.queue._internal.config import resolve_base_url, resolve_deployment
from vercel.queue.devserver import EmbeddedQueueDevServer
from vercel.queue.sync import QueueClient as SyncQueueClient

from .helpers import (
    one_chunk,
)


def test_region_and_deployment_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL_QUEUE_BASE_URL", raising=False)
    monkeypatch.delenv("VERCEL_QUEUE_BASE_PATH", raising=False)
    monkeypatch.delenv("VERCEL_REGION", raising=False)
    monkeypatch.delenv("VERCEL_DEPLOYMENT_ID", raising=False)
    monkeypatch.delenv("VERCEL_QUEUE_TOKEN", raising=False)
    with pytest.raises(ValueError, match="Queue region is required"):
        resolve_base_url()
    monkeypatch.setenv("VERCEL_REGION", "sfo1")
    assert resolve_base_url() == "https://sfo1.vercel-queue.com"
    with pytest.raises(ValueError, match="Invalid queue region"):
        resolve_base_url(region="bad-region")
    with pytest.raises(DeploymentResolutionError):
        resolve_deployment()
    monkeypatch.setenv("VERCEL_DEPLOYMENT_ID", "dpl_env")
    assert resolve_deployment() == "dpl_env"
    assert resolve_deployment(ALL_DEPLOYMENTS) is None
    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "vc-dev-token")
    assert resolve_deployment("dpl_explicit") == "dpl_explicit"


def test_base_url_resolution_matches_vqs_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VERCEL_QUEUE_BASE_URL", raising=False)
    monkeypatch.delenv("VERCEL_QUEUE_BASE_PATH", raising=False)

    assert resolve_base_url(region="iad1") == "https://iad1.vercel-queue.com"
    assert (
        resolve_base_url("https://proxy.example/queues/{region}/", region="sfo1")
        == "https://proxy.example/queues/sfo1"
    )
    assert resolve_base_url("http://localhost:3000/", region="sfo1") == "http://localhost:3000"
    assert (
        resolve_base_url(lambda region: f"https://{region}.queue.test/root/", region="fra1")
        == "https://fra1.queue.test/root"
    )

    monkeypatch.setenv("VERCEL_QUEUE_BASE_URL", "https://env.example/{region}/")
    assert resolve_base_url(region="hnd1") == "https://env.example/hnd1"

    monkeypatch.setenv("VERCEL_QUEUE_BASE_PATH", "/ignored")
    assert resolve_base_url(region="iad1") == "https://env.example/iad1"


def test_queue_client_has_no_base_path_api() -> None:
    kwargs: dict[str, Any] = {"base_path": "/queue"}
    with pytest.raises(TypeError, match="base_path"):
        QueueClient(
            token="token",
            region="iad1",
            deployment=ALL_DEPLOYMENTS,
            **kwargs,
        )


def test_deployment_sentinels_are_public() -> None:
    deployment: DeploymentID = "dpl_1"

    assert deployment == "dpl_1"
    assert isinstance(CURRENT_DEPLOYMENT, CurrentDeployment)
    assert isinstance(ALL_DEPLOYMENTS, AllDeployments)
    assert queue.CURRENT_DEPLOYMENT is CURRENT_DEPLOYMENT
    assert queue.ALL_DEPLOYMENTS is ALL_DEPLOYMENTS
    assert queue.DeploymentID is str
    assert hasattr(queue, "DeploymentOption")


def test_module_close_helpers_are_not_public() -> None:
    assert not hasattr(queue, "close")
    assert not hasattr(queue, "close_sync")
    assert "close" not in queue.__all__
    assert "close_sync" not in queue.__all__


def test_module_client_factories_are_not_public() -> None:
    assert not hasattr(queue, "get_async_client")
    assert not hasattr(queue_sync, "get_client")
    assert "get_async_client" not in queue.__all__
    assert "get_client" not in queue_sync.__all__


def test_sync_module_reexports_common_public_api() -> None:
    async_only = {"poll_and_handle"}
    missing = set(queue.__all__) - set(queue_sync.__all__) - async_only

    assert missing == set()
    assert queue_sync.QueueClient is SyncQueueClient
    assert queue_sync.subscribe is queue.subscribe
    assert queue_sync.Topic is queue.Topic


def test_topic_runtime_specialization_preserves_name_behavior() -> None:
    typed = Topic[bytes]("emails")
    untyped: Topic[Any] = Topic("emails")

    assert type(typed) is Topic[bytes]
    assert Topic[bytes] is Topic[bytes]
    assert type(typed).__topic_origin__ is Topic
    assert type(typed).__topic_payload_type__ is bytes
    assert not hasattr(typed, "__orig_class__")
    assert type(untyped).__topic_origin__ is None
    assert typed.name == "emails"
    assert repr(typed) == "Topic(name='emails')"
    assert typed == untyped
    assert hash(typed) == hash(untyped)

    with pytest.raises(ValueError, match="Invalid queue topic"):
        Topic[bytes]("bad.topic")


def test_deployment_resolution_and_opt_out_headers(
    eqs: EmbeddedQueueDevServer,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("VERCEL_DEPLOYMENT_ID", raising=False)
    monkeypatch.delenv("VERCEL_QUEUE_TOKEN", raising=False)
    monkeypatch.setenv("VERCEL_REGION", "iad1")

    with pytest.raises(DeploymentResolutionError):
        SyncQueueClient(token="token").send("emails", {"ok": True})

    all_client = eqs.get_sync_client(token="token", deployment=ALL_DEPLOYMENTS)
    all_message_id = all_client.send("emails", {"ok": True})
    assert all_message_id is not None
    assert eqs.state.by_id[all_message_id].deployment == "__all__"

    deployment_client = eqs.get_sync_client(token="token", deployment="dpl_1")
    deployment_message_id = deployment_client.send("emails", {"ok": True})
    assert deployment_message_id is not None
    assert eqs.state.by_id[deployment_message_id].deployment == "dpl_1"

    monkeypatch.setenv("VERCEL_QUEUE_TOKEN", "vc-dev-token")
    dev_client = eqs.get_sync_client(token=None, deployment="dpl_1")
    dev_message_id = dev_client.send("emails", {"ok": True})
    assert dev_message_id is not None
    assert eqs.state.by_id[dev_message_id].deployment == "dpl_1"

    explicit_token_client = eqs.get_sync_client(token="token", deployment="dpl_1")
    explicit_token_message_id = explicit_token_client.send("emails", {"ok": True})
    assert explicit_token_message_id is not None
    assert eqs.state.by_id[explicit_token_message_id].deployment == "dpl_1"


def test_queue_client_timeout_accepts_duration() -> None:
    client = SyncQueueClient(
        token="token",
        deployment=ALL_DEPLOYMENTS,
        timeout=timedelta(seconds=2.5),
    )
    assert client.timeout == pytest.approx(2.5)


def test_queue_client_timeout_rejects_invalid_duration() -> None:
    with pytest.raises(ValueError, match="timeout must be non-negative"):
        SyncQueueClient(
            token="token",
            deployment=ALL_DEPLOYMENTS,
            timeout=timedelta(seconds=-1),
        )


def test_topic_rejects_invalid_name() -> None:
    with pytest.raises(ValueError, match="Invalid queue topic"):
        Topic("emails.v1")


def test_name_helpers_are_public() -> None:
    sanitized = queue.SanitizedName("test-group_1")
    assert str(sanitized) == "test-group_1"
    assert queue.sanitize_name("team/email_high") == "team_Semail__high"
    assert queue.sanitize_name("plain-queue_1") == "plain-queue__1"
    assert queue.sanitize_name(sanitized) == "test-group_1"
    assert queue.sanitize_name("") == "queue"
    assert queue.sanitize_name("", fallback="consumer_group") == "consumer_group"
    assert queue.sanitize_name("emails.DQ") == "emails_DDQ"
    assert "SanitizedName" in queue.__all__
    assert "sanitize_name" in queue.__all__
    assert "validate_topic_name" not in queue.__all__
    assert "sanitize_topic_name" not in queue.__all__
    assert "validate_consumer_group_name" not in queue.__all__
    assert "sanitize_consumer_group_name" not in queue.__all__
    assert not hasattr(queue, "validate_topic_name")
    assert not hasattr(queue, "sanitize_topic_name")
    assert not hasattr(queue, "validate_consumer_group_name")
    assert not hasattr(queue, "sanitize_consumer_group_name")

    with pytest.raises(ValueError, match="name must be a non-empty string"):
        queue.SanitizedName("")
    with pytest.raises(ValueError, match="Invalid queue name"):
        queue.SanitizedName("test-group.v1")


def test_raw_json_transport_default_encoder_requires_json_serializable_payload() -> None:
    with pytest.raises(TypeError):
        RawJsonTransport[dict[str, object]]().serialize({"ts": datetime.now(timezone.utc)})


def test_raw_json_transport_accepts_explicit_custom_encoder() -> None:
    class CustomEncoder(json.JSONEncoder):
        def default(self, o: object) -> object:
            if isinstance(o, datetime):
                return o.isoformat()
            return super().default(o)

    dt = datetime.now(timezone.utc)
    serialized = RawJsonTransport[dict[str, object]](json_encoder=CustomEncoder).serialize({
        "ts": dt
    })

    assert json.loads(serialized) == {"ts": dt.isoformat()}


@pytest.mark.anyio
async def test_raw_json_transport_accepts_explicit_custom_decoder() -> None:
    class CustomDecoder(json.JSONDecoder):
        def __init__(self) -> None:
            super().__init__(object_hook=self._decode_object)

        def _decode_object(self, value: dict[str, object]) -> dict[str, object]:
            if isinstance(value.get("ts"), str):
                value["ts"] = datetime.fromisoformat(cast("str", value["ts"]))
            return value

    dt = datetime.now(timezone.utc)
    payload = await RawJsonTransport[dict[str, object]](json_decoder=CustomDecoder).deserialize(
        one_chunk(json.dumps({"ts": dt.isoformat()}).encode()), content_type=""
    )

    assert payload == {"ts": dt}
