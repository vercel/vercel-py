from vercel.headers import set_headers

from .credentials import Credentials, get_credentials
from .token import (
    VercelOidcTokenError,
    decode_oidc_payload,
    get_token_payload,
    get_vercel_oidc_token,
    get_vercel_oidc_token_sync,
)
from .verify import (
    VercelOidcVerificationError,
    extract_bearer_token,
    resolve_vercel_oidc_token_identity,
    verify_vercel_oidc_token,
)

__all__ = [
    "VercelOidcTokenError",
    "VercelOidcVerificationError",
    "extract_bearer_token",
    "resolve_vercel_oidc_token_identity",
    "verify_vercel_oidc_token",
    "get_vercel_oidc_token",
    "get_vercel_oidc_token_sync",
    "get_token_payload",
    "set_headers",
    "Credentials",
    "get_credentials",
    "decode_oidc_payload",
]
