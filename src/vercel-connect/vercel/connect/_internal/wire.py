"""Wire representations of the public request types.

Shared so the request body and the cache key can never disagree about what a
request means. They diverged once: a stray `type` key inside a custom
authorization detail was dropped from the body but still split the cache.
"""

from typing import Any

from vercel.connect._internal.models import (
    ConnectAuthorizationDetail,
    ConnectCustomAuthorizationDetail,
    ConnectGitHubAppInstallationAuthorizationDetail,
    ConnectJwtBearerTokenSubject,
    ConnectTokenSubject,
    JSONObject,
)


def serialize_subject(subject: ConnectTokenSubject) -> dict[str, Any]:
    """Render a subject as the API expects it."""
    body: dict[str, Any] = {"type": subject.type}
    match subject:
        case ConnectJwtBearerTokenSubject():
            body["sub"] = subject.sub
            if subject.iss is not None:
                body["iss"] = subject.iss
            if subject.aud is not None:
                body["aud"] = subject.aud
            if subject.additional_claims is not None:
                body["additionalClaims"] = dict(subject.additional_claims)
        case _:
            for name in ("id", "issuer", "token"):
                value = getattr(subject, name, None)
                if value is not None:
                    body[name] = value
    return body


def serialize_authorization_detail(detail: ConnectAuthorizationDetail) -> JSONObject:
    """Render one authorization detail (RFC 9396) as the API expects it."""
    match detail:
        case ConnectGitHubAppInstallationAuthorizationDetail():
            body: dict[str, Any] = {"type": detail.type}
            if detail.org is not None:
                body["org"] = detail.org
            if detail.permissions is not None:
                body["permissions"] = list(detail.permissions)
            if detail.repositories is not None:
                body["repositories"] = list(detail.repositories)
            return body
        case ConnectCustomAuthorizationDetail():
            # `type` is written last so a stray "type" key in `details` cannot
            # silently change which kind of authorization is being requested.
            return {**dict(detail.details), "type": detail.type}


__all__ = ["serialize_authorization_detail", "serialize_subject"]
