import httpx

from vercel import unstable as vercel
from vercel.unstable import sandbox


def test_unstable_error_inheritance() -> None:
    assert issubclass(vercel.VercelSessionError, vercel.VercelError)
    assert issubclass(vercel.VercelSessionClosedError, vercel.VercelSessionError)
    assert issubclass(vercel.VercelServiceOptionsError, vercel.VercelSessionError)
    assert issubclass(sandbox.SandboxError, vercel.VercelError)
    assert issubclass(sandbox.SandboxInvalidHandleError, sandbox.SandboxError)
    assert issubclass(sandbox.SandboxCleanupError, sandbox.SandboxError)
    assert issubclass(sandbox.SandboxApiError, sandbox.SandboxError)
    assert issubclass(sandbox.SandboxResponseError, sandbox.SandboxError)
    assert issubclass(sandbox.SandboxCredentialsError, sandbox.SandboxError)
    assert issubclass(sandbox.SandboxTerminalStateError, sandbox.SandboxError)


def test_sandbox_api_error_extracts_v2_error_code() -> None:
    request = httpx.Request("GET", "https://sandbox.test/v2/sandboxes/preview")
    response = httpx.Response(410, request=request, json={"error": {"code": "sandbox_stopped"}})
    error = sandbox.SandboxApiError(
        response,
        "HTTP 410: Sandbox stopped",
        data={"error": {"code": "sandbox_stopped"}},
    )

    assert error.status_code == 410
    assert error.code == "sandbox_stopped"
    assert error.data == {"error": {"code": "sandbox_stopped"}}
