from __future__ import annotations

from tests.unstable.fake_sandbox_api import FakeSandboxAPI
from vercel._internal.http.transport import JSONBody


async def test_fake_sandbox_api_records_requests_and_returns_scripted_responses(
    fake_sandbox_api: FakeSandboxAPI,
) -> None:
    fake_sandbox_api.script_response(
        status_code=202,
        json={"id": "sbx_test123", "status": "pending"},
        headers={"x-test": "yes"},
    )

    response = await fake_sandbox_api.send(
        "post",
        "/v1/sandboxes",
        params={"teamId": "team_123"},
        headers={"authorization": "Bearer token"},
        body=JSONBody({"runtime": "python3.12"}),
    )

    assert response.status_code == 202
    assert response.json() == {"id": "sbx_test123", "status": "pending"}
    assert response.headers["x-test"] == "yes"
    assert len(fake_sandbox_api.requests) == 1
    request = fake_sandbox_api.requests[0]
    assert request.method == "POST"
    assert request.path == "/v1/sandboxes"
    assert request.query == {"teamId": "team_123"}
    assert request.headers == {"authorization": "Bearer token"}
    assert request.body == {"runtime": "python3.12"}
