from dataclasses import dataclass

import pytest

from vercel.internal.core.errors import VercelServiceOptionsError
from vercel.internal.core.options import (
    ServiceOptions,
    collect_service_options,
)


@dataclass(frozen=True, slots=True)
class FirstOptions(ServiceOptions):
    value: str


@pytest.mark.parametrize(
    ("options", "message"),
    [
        (
            [FirstOptions("first"), FirstOptions("replacement")],
            "at most one object per concrete type",
        ),
        ([object()], "only ServiceOptions instances"),
    ],
)
def test_collect_service_options_rejects_invalid_collections(
    options: list[object], message: str
) -> None:
    with pytest.raises(VercelServiceOptionsError, match=message):
        collect_service_options(options)  # type: ignore[arg-type]
