Add OIDC token signature verification. `vercel.oidc.verify_vercel_oidc_token`
and its async twin `vercel.oidc.aio.verify_vercel_oidc_token` verify a Vercel
OIDC token against the JWKS at `oidc.vercel.com`, pinning the issuer, requiring
RS256, and checking the project, environment, owner, and audience claims.
`vercel.oidc.extract_bearer_token` reads the credential out of request headers.
Verification fails closed: when the expected project or environment cannot be
resolved from the arguments or the environment, every token is rejected.

Claims are compared for equality only. There is no project wildcard: `"*"` is an
ordinary string, so a token cannot widen its own scope to every project in a
team.

The issuer is pinned to Vercel's OIDC service and is not configurable. Both the
root issuer `https://oidc.vercel.com` and the team-scoped
`https://oidc.vercel.com/<team>` are accepted, since Vercel mints both and one
global key signs them; the JWKS URL is a constant, so a token can never
influence where signing keys come from.

`vercel.oidc.resolve_vercel_oidc_token_identity`, and its async twin, return an
opaque, stable identity for a token. A token is a signature over an identity plus
an expiry, so one identity is issued many tokens over time; this is what to key
identity-scoped client state on. The signature, issuer and expiry are verified
before anything is read, but no claim is checked and none is returned, so it is
not an authorization check.

This requires the new `verify` extra, which pulls in `pyjwt[crypto]`:

    pip install "vercel-oidc[verify]"

The extra keeps `cryptography` off installs that do not verify tokens.
