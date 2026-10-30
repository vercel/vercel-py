# Changelog

## 0.1.0 - 2026-10-30

### Features

- Add `vercel-passport`, a Python port of `@vercel/passport` for reading and verifying Vercel Passport visitor identities. (#484)

  `get_identity` reads the current request's Passport identity and verifies it, including sessions split across several cookies, and `require_identity` does the same but raises unless there is an identity with a token. `verify_identity` verifies a Passport token forwarded from another app. All three are available from `vercel.passport` (async) and `vercel.passport.sync`.

  In local development, `local_identity()` sets the identity of requests without a token, and `VERCEL_PASSPORT_DEV=1` opts in to a synthesized one. Unverified debugging tokens are accepted only in local development unless `allow_unverified=True` is passed.
