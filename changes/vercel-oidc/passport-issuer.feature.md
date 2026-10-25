Add an `issuer` option to `verify_vercel_oidc_token` that accepts Vercel Passport tokens with `issuer="passport"`.

The issuer is chosen from a closed set rather than given as a URL, so verification still cannot be widened beyond Vercel's own issuers. The default, `"oidc"`, keeps the existing behavior.
