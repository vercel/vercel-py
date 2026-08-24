"""Tests for the models that carry run creation data through the queue.

``start()`` writes ``run_created`` and pushes the queue message without ordering
them, so a consumer can be handed a run id whose row does not exist yet. The
recovery data rides the message as ``runInput`` and is echoed into the
``run_started`` event; these tests pin the wire shape both directions, because
the producer is the TypeScript SDK and a dropped field is silent.
"""

from __future__ import annotations

import os

from vercel.workflow._internal import world as w
from vercel.workflow._internal.worlds import vercel as vercel_mod

from ..world_stubs import NoStreams

# What `@workflow/core`'s start() puts on the queue at specVersion 6, keyed the
# way it appears on the wire.
RUN_INPUT_WIRE = {
    "input": b"devl\x00payload",
    "deploymentId": "dpl_1",
    "workflowName": "greet",
    "specVersion": 6,
    "executionContext": {"queueNamespace": "alpha"},
    "attributes": {"$rootRunId": "wrun_root", "tier": "pro"},
    "allowReservedAttributes": True,
    "encryptionPublicKey": "cHVibGljLWtleQ==",
    "environment": "production",
}


def test_run_input_round_trips_every_field() -> None:
    """A field we drop here is a field the recreated run loses permanently."""
    payload = w.WorkflowInvokePayload.from_wire({"runId": "wrun_1", "runInput": RUN_INPUT_WIRE})

    assert payload.run_input is not None
    assert payload.run_input.model_dump() == RUN_INPUT_WIRE


def test_run_input_absent_on_re_enqueues() -> None:
    """Re-enqueues and pre-v3 producers send no `runInput`, and that is normal."""
    payload = w.WorkflowInvokePayload.from_wire({"runId": "wrun_1"})

    assert payload.run_input is None
    # Omitted rather than serialized as null: the TS reader types it `undefined`.
    assert "runInput" not in payload.model_dump()


def test_run_input_optional_fields_may_all_be_absent() -> None:
    """Only the four creation essentials are required of a producer."""
    run_input = w.RunInput.from_wire(
        {
            "input": b"devl\x00payload",
            "deploymentId": "dpl_1",
            "workflowName": "greet",
            "specVersion": 3,
        }
    )

    assert run_input.execution_context is None
    assert run_input.attributes is None
    assert run_input.allow_reserved_attributes is None
    assert run_input.encryption_public_key is None
    assert run_input.environment is None


def test_run_started_event_data_forwards_the_whole_run_input() -> None:
    """The echo into `run_started` is where a dropped field would be lost."""
    run_input = w.RunInput.from_wire(RUN_INPUT_WIRE)

    data = w.RunStartedEventData.from_run_input(run_input)

    assert data.model_dump() == {
        "input": RUN_INPUT_WIRE["input"],
        "deploymentId": "dpl_1",
        "workflowName": "greet",
        "executionContext": {"queueNamespace": "alpha"},
        "attributes": {"$rootRunId": "wrun_root", "tier": "pro"},
        "allowReservedAttributes": True,
        "encryptionPublicKey": "cHVibGljLWtleQ==",
    }
    # `environment` and `specVersion` are deliberately not among them:
    # the former is for the consumer's own guard, the latter rides the event.
    assert "environment" not in data.model_dump()
    assert "specVersion" not in data.model_dump()


def test_run_started_carries_the_creating_client_spec_version() -> None:
    """A run recreated at our own CURRENT would be mislabelled: the row inherits
    the event's version, and the creator wrote 6, not 2."""
    run_input = w.RunInput.from_wire(RUN_INPUT_WIRE)

    event = w.RunStartedEventData.from_run_input(run_input).into_event(
        spec_version=run_input.spec_version
    )

    assert event.spec_version == 6
    assert event.model_dump()["specVersion"] == 6


def test_bare_run_started_serializes_without_event_data() -> None:
    """No `runInput` on the message means no `eventData` on the event at all —
    not an empty object, which the world would try to create a run from."""
    event = w.RunStartedEvent()

    assert event.event_data is None
    assert "eventData" not in event.model_dump()


def test_run_started_payloads_expose_the_carried_input() -> None:
    """`payloads()` is what decides whether the run needs a decryption key, so
    a resilient start's input has to be visible to it."""
    run_input = w.RunInput.from_wire(RUN_INPUT_WIRE)

    with_data = w.RunStartedEventData.from_run_input(run_input).into_event()

    assert with_data.payloads() == (RUN_INPUT_WIRE["input"],)
    assert w.RunStartedEvent().payloads() == ()


def test_run_started_accepts_a_server_echo_of_event_data() -> None:
    """The world strips `eventData` from the stored row, but a reader must not
    choke if one comes back."""
    event = w.EventAdaptor.from_wire(
        {
            "eventType": "run_started",
            "runId": "wrun_1",
            "eventId": "evnt_1",
            "createdAt": "2026-08-11T00:00:00.000Z",
            "specVersion": 6,
            "eventData": {"deploymentId": "dpl_1", "workflowName": "greet"},
        }
    )

    assert isinstance(event, w.RunStartedEvent)
    assert event.event_data is not None
    assert event.event_data.deployment_id == "dpl_1"
    assert event.event_data.input is None


class TestGetEnvironment:
    """`get_environment()` feeds the cross-environment guard, so it has to report
    what the backend will actually attribute our writes to."""

    def test_default_world_has_no_environment_dimension(self) -> None:
        class _World(NoStreams, w.World):
            get_deployment_id = None  # type: ignore[assignment]
            queue = None  # type: ignore[assignment]
            create_queue_handler = None  # type: ignore[assignment]
            runs_get = None  # type: ignore[assignment]
            steps_get = None  # type: ignore[assignment]
            hooks_get_by_token = None  # type: ignore[assignment]
            events_create = None  # type: ignore[assignment]
            events_list = None  # type: ignore[assignment]

        assert _World().get_environment() is None

    def test_proxy_path_reports_the_header_it_sends(self) -> None:
        """Read back from the header so the two can never drift."""
        world = vercel_mod.VercelWorld(
            token="tok", environment="preview", project_id="prj_1", team_id="team_1"
        )

        assert world.get_environment() == "preview"
        assert world._headers["x-vercel-environment"] == "preview"

    def test_proxy_path_defaults_to_production(self) -> None:
        """`|| 'production'` is part of the contract, not an incidental default."""
        world = vercel_mod.VercelWorld(token="tok", project_id="prj_1", team_id="team_1")

        assert world.get_environment() == "production"

    def test_oidc_path_prefers_target_env(self, monkeypatch) -> None:
        """`VERCEL_ENV` says `preview` inside a custom environment while the OIDC
        claim carries the slug, which would fabricate a mismatch."""
        monkeypatch.setenv("VERCEL_TARGET_ENV", "staging")
        monkeypatch.setenv("VERCEL_ENV", "preview")

        assert vercel_mod.VercelWorld().get_environment() == "staging"

    def test_oidc_path_falls_back_to_vercel_env(self, monkeypatch) -> None:
        monkeypatch.delenv("VERCEL_TARGET_ENV", raising=False)
        monkeypatch.setenv("VERCEL_ENV", "preview")

        assert vercel_mod.VercelWorld().get_environment() == "preview"

    def test_unknown_outside_vercel(self, monkeypatch) -> None:
        """Guessing here would refuse a legitimate preview delivery."""
        for name in ("VERCEL_TARGET_ENV", "VERCEL_ENV"):
            monkeypatch.delenv(name, raising=False)

        assert vercel_mod.VercelWorld().get_environment() is None

    def test_empty_env_var_is_unknown_not_empty_string(self, monkeypatch) -> None:
        monkeypatch.setenv("VERCEL_TARGET_ENV", "")
        monkeypatch.setenv("VERCEL_ENV", "")

        assert vercel_mod.VercelWorld().get_environment() is None


def test_vercel_world_environment_is_not_read_from_the_process_on_proxy_path() -> None:
    """The header wins: on the proxy path the backend ignores our env vars."""
    previous = os.environ.get("VERCEL_ENV")
    os.environ["VERCEL_ENV"] = "development"
    try:
        world = vercel_mod.VercelWorld(
            token="tok", environment="production", project_id="prj_1", team_id="team_1"
        )
        assert world.get_environment() == "production"
    finally:
        if previous is None:
            os.environ.pop("VERCEL_ENV", None)
        else:
            os.environ["VERCEL_ENV"] = previous
