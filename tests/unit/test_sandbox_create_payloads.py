from copy import deepcopy

from hypothesis import HealthCheck, given, settings, strategies as st

from vercel._internal.payload import RawPayload, marshal_payload


def _snake_key_strategy() -> st.SearchStrategy[str]:
    head = st.sampled_from(["alpha", "beta", "gamma", "delta", "__private"])
    tail = st.lists(st.sampled_from(["one", "two", "three"]), min_size=0, max_size=2)
    return st.builds(
        lambda prefix, suffixes: (
            "_".join([prefix, *suffixes]) if not prefix.startswith("__") else prefix
        ),
        head,
        tail,
    )


def _raw_payload_strategy() -> st.SearchStrategy[RawPayload]:
    raw_leaf = st.none() | st.booleans() | st.integers() | st.text()
    raw_value = st.recursive(
        raw_leaf,
        lambda children: (
            st.lists(children, max_size=3)
            | st.dictionaries(_snake_key_strategy(), children, max_size=3)
        ),
        max_leaves=8,
    )
    return raw_value.map(RawPayload)


def _payload_strategy() -> st.SearchStrategy[object]:
    leaf = st.none() | st.booleans() | st.integers() | st.text()
    return st.recursive(
        leaf | _raw_payload_strategy(),
        lambda children: (
            st.lists(children, max_size=3)
            | st.dictionaries(_snake_key_strategy(), children, max_size=3)
        ),
        max_leaves=12,
    )


def _expected_payload(value: object) -> object:
    if isinstance(value, RawPayload):
        return value.value
    if isinstance(value, dict):
        return {_camelize_key(key): _expected_payload(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [_expected_payload(item) for item in value]
    return value


def _camelize_key(key: str) -> str:
    if not key or key.startswith("__"):
        return key

    parts = key.split("_")
    return parts[0] + "".join(part[:1].upper() + part[1:] for part in parts[1:])


@settings(suppress_health_check=[HealthCheck.too_slow])
@given(_payload_strategy())
def test_marshal_payload_matches_recursive_reference_transform(value: object) -> None:
    original = deepcopy(value)

    assert marshal_payload(value) == _expected_payload(original)
    assert value == original
