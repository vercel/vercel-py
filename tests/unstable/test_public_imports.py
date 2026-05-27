from vercel import unstable as vercel
from vercel.unstable import sandbox
from vercel.unstable.sandbox import sync as sandbox_sync


def test_unstable_public_imports() -> None:
    assert vercel.session is not None
    assert vercel.VercelError is not None
    assert sandbox is not None


def test_unstable_sandbox_public_imports() -> None:
    assert sandbox.create_sandbox is not None
    assert sandbox.get_sandbox is not None
    assert sandbox.query_sandboxes is not None
    assert sandbox.Sandbox is not None
    assert sandbox.SandboxRuntimeSession is not None
    assert sandbox.SandboxServiceOptions is not None
    assert sandbox.SandboxStatus.RUNNING == "running"
    assert sandbox.SandboxCredentialsError is not None
    assert sandbox.SandboxError is not None
    assert sandbox.SandboxResponseError is not None
    assert sandbox.sync is sandbox_sync


def test_unstable_sandbox_sync_public_imports() -> None:
    assert sandbox_sync.create_sandbox is not None
    assert sandbox_sync.get_sandbox is not None
    assert sandbox_sync.query_sandboxes is not None
    assert sandbox_sync.Sandbox is not None
    assert sandbox_sync.SandboxRuntimeSession is not None
    assert sandbox_sync.SandboxStatus.RUNNING == "running"
    assert sandbox_sync.SandboxCredentialsError is not None
    assert sandbox_sync.SandboxResponseError is not None
