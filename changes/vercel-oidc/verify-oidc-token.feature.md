Add OIDC token signature verification. `vercel.oidc.verify_vercel_oidc_token`
and its async twin `vercel.oidc.aio.verify_vercel_oidc_token` verify a Vercel
OIDC token against the JWKS at `oidc.vercel.com`, pinning the issuer, requiring
RS256, and checking the project, environment, owner, and audience claims.
`vercel.oidc.extract_bearer_token` reads the credential out of request headers.
Verification fails closed: when the expected project or environment cannot be
resolved from the arguments or the environment, every token is rejected.

This requires the new `verify` extra, which pulls in `pyjwt[crypto]`:

    pip install "vercel-oidc[verify]"

The extra keeps `cryptography` off installs that do not verify tokens.
