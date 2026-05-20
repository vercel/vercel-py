from __future__ import annotations

import pytest

from tests.unstable.fake_sandbox_api import FakeSandboxAPI


@pytest.fixture
def fake_sandbox_api() -> FakeSandboxAPI:
    return FakeSandboxAPI()
