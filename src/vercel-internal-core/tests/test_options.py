from dataclasses import dataclass

import pytest

from vercel._internal.core.errors import VercelServiceOptionsError
from vercel._internal.core.options import (
    ServiceOptions,
    collect_service_options,
)


@dataclass(frozen=True, slots=True)
class FirstOptions(ServiceOptions):
    value: str


class SharedOptionsKey(ServiceOptions):
    @classmethod
    def service_options_key(cls) -> type[ServiceOptions]:
        return SharedOptionsKey


@dataclass(frozen=True, slots=True)
class SharedOptions(SharedOptionsKey):
    value: str


@dataclass(frozen=True, slots=True)
class OtherSharedOptions(SharedOptionsKey):
    value: str


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (
            [FirstOptions("first"), FirstOptions("replacement")],
            "at most one object per logical service",
        ),
        (
            [SharedOptions("first"), OtherSharedOptions("replacement")],
            "at most one object per logical service",
        ),
        ([object()], "only ServiceOptions instances"),
    ],
)
def test_collect_service_options_rejects_invalid_collections(
    options: list[object], message: str
) -> None:
    with pytest.raises(VercelServiceOptionsError, match=message):
        collect_service_options(options)  # type: ignore[arg-type]
