# This module intentionally omits `from __future__ import annotations` so the
# subscriber resolver is tested with runtime annotation objects.

from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel

from vercel.queue import Message, subscribe
from vercel.queue._internal.subscribers import call_subscribers_sync

from .helpers import make_metadata


def test_subscribe_uses_runtime_payload_annotation(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    runtime_alias = Payload
    calls: list[Payload] = []

    def handle(payload: object) -> None:
        calls.append(cast("Payload", payload))

    handle.__annotations__["payload"] = Annotated[runtime_alias, "metadata"]
    subscribe(topic="runtime", consumer_group="test-group")(handle)

    call_subscribers_sync(
        Message(
            payload={"count": "7"},
            metadata=make_metadata("runtime", consumer_group="test-group"),
        )
    )

    assert calls == [Payload(count=7)]


def test_subscribe_uses_runtime_annotation_with_model_field_forward_refs(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    payload_model = type(
        "Payload",
        (BaseModel,),
        {"__module__": __name__, "__annotations__": {"child": "Child"}},
    )
    calls: list[BaseModel] = []

    def handle(payload: object) -> None:
        calls.append(cast("BaseModel", payload))

    handle.__annotations__["payload"] = payload_model
    subscribe(topic="runtime-model-field-ref", consumer_group="test-group")(handle)

    call_subscribers_sync(
        Message(
            payload={"child": {"count": "8"}},
            metadata=make_metadata("runtime-model-field-ref", consumer_group="test-group"),
        )
    )

    assert calls == [payload_model(child=Child(count=8))]


def test_subscribe_uses_runtime_annotation_with_outer_scope_model_refs(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    def register() -> tuple[type[BaseModel], list[BaseModel], object]:
        payload_model = type(
            "Payload",
            (BaseModel,),
            {"__module__": __name__, "__annotations__": {"child": "Child"}},
        )
        calls: list[BaseModel] = []

        def handle(payload: object) -> None:
            calls.append(cast("BaseModel", payload))

        handle.__annotations__["payload"] = payload_model
        subscribe(topic="runtime-outer-model-field-ref", consumer_group="test-group")(handle)
        return payload_model, calls, handle

    payload_model, calls, handle = register()

    call_subscribers_sync(
        Message(
            payload={"child": {"count": "9"}},
            metadata=make_metadata("runtime-outer-model-field-ref", consumer_group="test-group"),
        )
    )

    assert calls == [payload_model(child=Child(count=9))]
    assert handle is not None


def test_subscribe_uses_runtime_alias_with_outer_scope_model_refs(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    def register() -> tuple[type[BaseModel], list[object], object]:
        payload_model = type(
            "Payload",
            (BaseModel,),
            {"__module__": __name__, "__annotations__": {"child": "Child"}},
        )
        calls: list[object] = []
        payload_alias = dict.__class_getitem__((str, payload_model))

        def handle(payload: object) -> None:
            calls.append(payload)

        handle.__annotations__["payload"] = payload_alias
        subscribe(topic="runtime-outer-alias-model-ref", consumer_group="test-group")(handle)
        return payload_model, calls, handle

    payload_model, calls, handle = register()

    call_subscribers_sync(
        Message(
            payload={"item": {"child": {"count": "10"}}},
            metadata=make_metadata("runtime-outer-alias-model-ref", consumer_group="test-group"),
        )
    )

    assert calls == [{"item": payload_model(child=Child(count=10))}]
    assert handle is not None


def test_subscribe_uses_runtime_container_aliases_with_model_refs(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    payload_model = type(
        "Payload",
        (BaseModel,),
        {"__module__": __name__, "__annotations__": {"child": "Child"}},
    )
    list_alias = list.__class_getitem__(payload_model)
    dict_list_alias = dict.__class_getitem__((str, list_alias))

    list_calls: list[object] = []

    def list_payload(payload: object) -> None:
        list_calls.append(payload)

    list_payload.__annotations__["payload"] = list_alias
    subscribe(topic="runtime-complex-list", consumer_group="test-group")(list_payload)

    dict_list_calls: list[object] = []

    def dict_list_payload(payload: object) -> None:
        dict_list_calls.append(payload)

    dict_list_payload.__annotations__["payload"] = dict_list_alias
    subscribe(topic="runtime-complex-dict-list", consumer_group="test-group")(dict_list_payload)

    call_subscribers_sync(
        Message(
            payload=[{"child": {"count": "12"}}],
            metadata=make_metadata("runtime-complex-list", consumer_group="test-group"),
        )
    )
    call_subscribers_sync(
        Message(
            payload={"items": [{"child": {"count": "13"}}]},
            metadata=make_metadata("runtime-complex-dict-list", consumer_group="test-group"),
        )
    )

    assert list_calls == [[payload_model(child=Child(count=12))]]
    assert dict_list_calls == [{"items": [payload_model(child=Child(count=13))]}]


def test_subscribe_uses_runtime_message_alias_with_model_refs(
    isolated_subscriptions: None,
) -> None:
    class Child(BaseModel):
        count: int

    payload_model = type(
        "Payload",
        (BaseModel,),
        {"__module__": __name__, "__annotations__": {"child": "Child"}},
    )
    payload_alias = dict.__class_getitem__((str, payload_model))
    message_alias = cast("Any", Message)[payload_alias]
    calls: list[Message[object]] = []

    def handle(message: Message[object]) -> None:
        calls.append(message)

    handle.__annotations__["message"] = message_alias
    subscribe(topic="runtime-message-complex", consumer_group="test-group")(handle)

    metadata = make_metadata("runtime-message-complex", consumer_group="test-group")
    call_subscribers_sync(Message(payload={"item": {"child": {"count": "14"}}}, metadata=metadata))

    assert len(calls) == 1
    assert calls[0].payload == {"item": payload_model(child=Child(count=14))}
    assert calls[0].metadata is metadata


def test_subscribe_uses_runtime_annotated_alias_with_literal_string_values(
    isolated_subscriptions: None,
) -> None:
    class Payload(BaseModel):
        count: int

    calls: list[object] = []
    payload_alias = Annotated[dict[Literal["primary"], Payload], "metadata"]

    def handle(payload: object) -> None:
        calls.append(payload)

    handle.__annotations__["payload"] = payload_alias
    subscribe(topic="runtime-literal-forward-ref", consumer_group="test-group")(handle)

    call_subscribers_sync(
        Message(
            payload={"primary": {"count": "11"}},
            metadata=make_metadata("runtime-literal-forward-ref", consumer_group="test-group"),
        )
    )

    assert calls == [{"primary": Payload(count=11)}]
