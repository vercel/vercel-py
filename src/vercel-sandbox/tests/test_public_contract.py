import vercel
from vercel import sandbox
from vercel.sandbox import sync


def test_representative_public_objects_preserve_identity_and_error_hierarchy() -> None:
    assert sync.SandboxServiceOptions is sandbox.SandboxServiceOptions
    assert sync.SandboxStatus is sandbox.SandboxStatus
    assert issubclass(sandbox.SandboxError, vercel.VercelError)
    assert sync.SandboxError is sandbox.SandboxError
