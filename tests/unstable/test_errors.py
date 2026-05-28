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
