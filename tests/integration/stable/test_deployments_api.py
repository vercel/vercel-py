from __future__ import annotations

import pytest
import respx
from httpx import Response

import vercel
from tests.integration.conftest import VERCEL_API_BASE
from vercel.stable.options import DeploymentCreateRequest
from vercel.stable.sdk.deployments import Deployment, UploadedDeploymentFile


@pytest.fixture
def deployment_payload() -> dict[str, object]:
    return {
        "id": "dpl_test123",
        "name": "test-project",
        "url": "test-project.vercel.app",
        "inspectorUrl": "https://vercel.com/acme/test-project/inspect",
        "readyState": "READY",
        "meta": {"kept": "raw"},
    }


@pytest.fixture
def uploaded_file_payload() -> dict[str, object]:
    return {
        "fileHash": "sha1:abc123",
        "size": 12,
        "unexpectedField": "kept",
    }


@respx.mock
def test_deployments_create_maps_query_params(
    deployment_payload: dict[str, object],
) -> None:
    route = respx.post(f"{VERCEL_API_BASE}/v13/deployments").mock(
        return_value=Response(200, json=deployment_payload)
    )

    vc = vercel.create_sync_client(timeout=9.0)
    try:
        deployment = (
            vc.get_sdk(token="sdk-token")
            .get_deployments()
            .create(
                request=DeploymentCreateRequest(name="test-project"),
                team_id="team_123",
                team_slug="acme",
                force_new=True,
                skip_auto_detection_confirmation=False,
            )
        )
    finally:
        vc.close()

    assert isinstance(deployment, Deployment)
    assert deployment.id == "dpl_test123"
    assert deployment.ready_state == "READY"
    assert deployment.to_dict()["meta"] == {"kept": "raw"}
    assert route.called
    request = route.calls[0].request
    assert request.url.params["teamId"] == "team_123"
    assert request.url.params["slug"] == "acme"
    assert request.url.params["forceNew"] == "1"
    assert request.url.params["skipAutoDetectionConfirmation"] == "0"
    assert request.headers["authorization"] == "Bearer sdk-token"
    assert request.content == b'{"name":"test-project"}'


@respx.mock
def test_deployments_upload_file_uses_octet_stream(
    uploaded_file_payload: dict[str, object],
) -> None:
    route = respx.post(f"{VERCEL_API_BASE}/v2/files").mock(
        return_value=Response(200, json=uploaded_file_payload)
    )

    vc = vercel.create_sync_client()
    try:
        uploaded = (
            vc.get_sdk(token="sdk-token")
            .get_deployments()
            .upload_file(
                content=b"hello world!",
                content_length=12,
                x_vercel_digest="sha1:abc123",
                x_now_size=12,
                team_id="team_123",
            )
        )
    finally:
        vc.close()

    assert isinstance(uploaded, UploadedDeploymentFile)
    assert uploaded.file_hash == "sha1:abc123"
    assert uploaded.size == 12
    assert uploaded.to_dict()["unexpectedField"] == "kept"
    assert route.called
    request = route.calls[0].request
    assert request.url.params["teamId"] == "team_123"
    assert request.headers["authorization"] == "Bearer sdk-token"
    assert request.headers["content-type"] == "application/octet-stream"
    assert request.headers["content-length"] == "12"
    assert request.headers["x-vercel-digest"] == "sha1:abc123"
    assert request.headers["x-now-size"] == "12"
    assert request.content == b"hello world!"


@respx.mock
def test_deployments_ensure_connected_is_eager_but_request_lazy(
    deployment_payload: dict[str, object],
) -> None:
    route = respx.post(f"{VERCEL_API_BASE}/v13/deployments").mock(
        return_value=Response(200, json=deployment_payload)
    )

    vc = vercel.create_sync_client()
    deployments = vc.get_sdk(token="sdk-token").get_deployments()
    sibling = vc.get_sdk(token="sdk-token").get_deployments()

    try:
        assert deployments.ensure_connected() is deployments
        assert sibling.ensure_connected() is sibling
        assert not route.called

        created = sibling.create(request=DeploymentCreateRequest(name="test-project"))
    finally:
        vc.close()

    assert created.id == "dpl_test123"
    assert route.called


@respx.mock
def test_deployments_with_options_keeps_parent_request_policy_lazy(
    deployment_payload: dict[str, object],
) -> None:
    route = respx.post(f"{VERCEL_API_BASE}/v13/deployments").mock(
        return_value=Response(200, json=deployment_payload)
    )

    vc = vercel.create_sync_client()
    sdk = vc.get_sdk(token="sdk-token", team_id="team_123")
    deployments = sdk.with_options(
        token="overlay-token", team_slug="overlay-team"
    ).get_deployments()

    try:
        created = deployments.create(request=DeploymentCreateRequest(name="test-project"))
    finally:
        vc.close()

    assert created.id == "dpl_test123"
    assert route.called
    request = route.calls[0].request
    assert dict(request.url.params) == {"teamId": "team_123", "slug": "overlay-team"}
    assert request.headers["authorization"] == "Bearer overlay-token"
