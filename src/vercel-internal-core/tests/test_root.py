import subprocess
import sys

import vercel
from vercel.internal.core import errors
from vercel.internal.core.session import session


def test_root_public_contract_preserves_shared_identity_and_error_hierarchy() -> None:
    assert vercel.session is session
    assert vercel.VercelError is errors.VercelError
    assert vercel.VercelServiceOptionsError is errors.VercelServiceOptionsError
    assert vercel.VercelSessionClosedError is errors.VercelSessionClosedError
    assert vercel.VercelSessionError is errors.VercelSessionError
    assert issubclass(vercel.VercelSessionError, vercel.VercelError)
    assert issubclass(vercel.VercelSessionClosedError, vercel.VercelSessionError)
    assert issubclass(vercel.VercelServiceOptionsError, vercel.VercelSessionError)


def test_importing_root_does_not_import_or_resolve_sandbox() -> None:
    code = (
        "import sys, vercel; "
        "assert 'vercel.sandbox' not in sys.modules; "
        "assert not hasattr(vercel, 'sandbox')"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
