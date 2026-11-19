"""run_started reuses its replay log, including after a partial response."""

from datetime import UTC, datetime

import cbor2
import httpx2 as httpx
import pytest

import vendor.respx as respx
from tests.payloads import PLAIN_ENCODER
from vercel.headers import HeadersContext
from vercel.workflow._internal import runtime, world as w
from vercel.workflow._internal.worlds import event_frames as frames
from vercel.workflow._internal.worlds.local import LocalWorld
from vercel.workflow._internal.worlds.vercel import VercelWorld


def _meta(kind, index, **extra):
    return {
        "eventType": kind,
        "eventId": f"evnt_{index}",
        "runId": "wrun_test",
        "createdAt": f"2026-01-01T00:00:0{index}Z",
        "specVersion": 5,
    } | extra


def _created():
    return frames.encode_frame(
        _meta(
            "run_created",
            1,
            eventData={
                "deploymentId": "dpl_test",
                "workflowName": "test",
                "attributes": {"initial": "yes"},
                "executionContext": {"language": "python"},
                "encryptionPublicKey": "public-key",
            },
        ),
        b"zstd\x00opaque-input",
    )


def _started():
    return frames.encode_frame(_meta("run_started", 2))


def _end(index=2, *, more=False):
    return frames.encode_frame({"_end": 1, "next": f"eid:evnt_{index}", "hasMore": more})


def _response(body, max_events="25000"):
    headers = {"content-type": frames.CONTENT_TYPE}
    if max_events is not None:
        headers["x-wf-max-events"] = max_events
    return httpx.Response(200, content=body, headers=headers)


# @lat: [[vercel-workflow#Worlds#Replay preloading]]
@respx.mock
async def test_started_stream_reconstructs_run_by_type_and_folds_attributes():
    world = VercelWorld(token="test")
    attr = frames.encode_frame(
        _meta(
            "attr_set",
            3,
            eventData={
                "changes": [{"key": "initial", "value": None}, {"key": "later", "value": "yes"}],
                "writer": {"type": "workflow"},
            },
        )
    )
    # Lifecycle events are found by type, rather than by their array position.
    route = respx.post(f"{world._base_url}/v4/runs/wrun_test/events/run_started").mock(
        return_value=_response(_started() + _created() + attr + _end(3))
    )
    with HeadersContext({"x-vercel-id": " iad1::request "}).use():
        result = await world.events_create(
            "wrun_test",
            w.RunStartedEvent(
                spec_version=5,
                event_data=w.RunStartedEventData(
                    deployment_id="dpl_test", workflow_name="test", input=b"gzip\x00input"
                ),
            ),
        )
    assert result.run is not None
    assert result.run.input == b"zstd\x00opaque-input"
    assert result.run.attributes == {"later": "yes"}
    assert result.run.encryption_public_key == "public-key"
    assert result.run.execution_context == {"language": "python"}
    assert result.run.started_at == datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC)
    assert result.run.updated_at == datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC)
    assert result.max_events == 25000 and result.has_more is False
    assert result.cursor == "eid:evnt_3"
    request = route.calls[0].request
    raw = request.content
    meta_size = int.from_bytes(raw[:4], "big")
    meta = cbor2.loads(raw[4 : 4 + meta_size])
    assert meta["vercelId"] == "iad1::request" and meta["specVersion"] == 5
    assert meta["deploymentId"] == "dpl_test" and "eventData" not in meta
    assert raw[8 + meta_size :] == b"gzip\x00input"
    assert request.headers["accept"] == frames.CONTENT_TYPE
    assert route.call_count == 1


@respx.mock
@pytest.mark.parametrize("truncated", [False, True])
async def test_partial_post_completes_via_get_without_repeating_the_write(truncated):
    world = VercelWorld(token="test")
    post = respx.post(f"{world._base_url}/v4/runs/wrun_test/events/run_started").mock(
        return_value=_response(_created() + (b"\x00\x00" if truncated else _end(1, more=True)))
    )
    get = respx.get(url__startswith=f"{world._base_url}/v4/runs/wrun_test/events").mock(
        return_value=_response(_started() + _end())
    )
    result = await world.events_create("wrun_test", w.RunStartedEvent())
    assert result.run is not None and result.events is not None and len(result.events) == 2
    assert get.calls[0].request.url.params["cursor"] == "eid:evnt_1"
    assert get.calls[0].request.url.params["returnAll"] == "true"
    assert post.call_count == get.call_count == 1


@respx.mock
@pytest.mark.parametrize("max_events", [None, "0", "-1", "invalid", "1.5"])
async def test_started_response_requires_positive_service_event_ceiling(max_events):
    world = VercelWorld(token="test")
    route = respx.post(f"{world._base_url}/v4/runs/wrun_test/events/run_started").mock(
        return_value=_response(_created() + _started() + _end(), max_events)
    )
    with pytest.raises(w.WorkflowWorldError, match="max-events"):
        await world.events_create("wrun_test", w.RunStartedEvent())
    assert route.call_count == 1


@respx.mock
async def test_started_stream_without_lifecycle_events_is_rejected():
    world = VercelWorld(token="test")
    respx.post(f"{world._base_url}/v4/runs/wrun_test/events/run_started").mock(
        return_value=_response(_started() + _end())
    )
    with pytest.raises(w.WorkflowWorldError, match="lifecycle"):
        await world.events_create("wrun_test", w.RunStartedEvent())


@pytest.fixture
def local(tmp_path, monkeypatch):
    monkeypatch.setenv("WORKFLOW_LOCAL_DATA_DIR", str(tmp_path))
    world = LocalWorld()
    w.set_world(world)
    yield world
    w.set_world(None)


async def _create(world):
    result = await world.events_create(
        None,
        w.RunCreatedEvent(
            event_data=w.RunCreatedEventData(
                workflow_name="test", deployment_id="dpl_test", input=PLAIN_ENCODER.encode([])
            )
        ),
    )
    return result.run.run_id


@pytest.mark.parametrize("retry", [False, True])
async def test_local_start_and_idempotent_retry_preload_the_log(local, monkeypatch, retry):
    run_id = await _create(local)
    first = await local.events_create(run_id, w.RunStartedEvent())
    result = await local.events_create(run_id, w.RunStartedEvent()) if retry else first

    async def unexpected(*args, **kwargs):
        raise AssertionError("complete preload must not call events_list")

    monkeypatch.setattr(local, "events_list", unexpected)
    loaded = await runtime.get_all_workflow_run_events(run_id, preloaded=result)
    assert [e.event_type for e in loaded.events] == ["run_created", "run_started"]
    assert loaded.cursor == result.cursor and result.cursor is not None


async def test_partial_local_preload_reads_only_the_suffix(local, monkeypatch):
    run_id = await _create(local)
    monkeypatch.setenv("WORKFLOW_MAX_EVENTS", "1")
    result = await local.events_create(run_id, w.RunStartedEvent())
    assert result.has_more and result.events is not None and len(result.events) == 1
    original = local.events_list
    cursors = []

    async def record(run_id, *, pagination=None):
        cursors.append(pagination.cursor)
        return await original(run_id, pagination=pagination)

    monkeypatch.setattr(local, "events_list", record)
    loaded = await runtime.get_all_workflow_run_events(run_id, preloaded=result)
    assert cursors == [result.cursor]
    assert [e.event_type for e in loaded.events] == ["run_created", "run_started"]


async def test_legacy_preload_without_flags_falls_back_to_full_load(local):
    run_id = await _create(local)
    result = await local.events_create(run_id, w.RunStartedEvent())
    result.has_more = None
    loaded = await runtime.get_all_workflow_run_events(run_id, preloaded=result)
    assert [e.event_type for e in loaded.events] == ["run_created", "run_started"]


async def test_partial_preload_requires_a_cursor(local):
    with pytest.raises(w.WorkflowWorldError, match="no cursor"):
        await runtime.get_all_workflow_run_events(
            "wrun_test", preloaded=w.EventResult(events=[], has_more=True)
        )


async def test_complete_preload_drops_seals_but_retains_the_world_cursor(local):
    seal = w.NoopEvent(
        server_props=w.ServerProps(
            run_id="wrun_test", event_id="evnt_seal", created_at=datetime.now(UTC)
        )
    )
    loaded = await runtime.get_all_workflow_run_events(
        "wrun_test", preloaded=w.EventResult(events=[seal], has_more=False, cursor="eid:evnt_seal")
    )
    assert loaded.events == [] and loaded.cursor == "eid:evnt_seal"
