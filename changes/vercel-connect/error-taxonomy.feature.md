Add error classes for the codes Connect forwards from an upstream provider's
token endpoint: `AuthorizationPendingError` (`authorization_pending` and
`slow_down`), `AuthorizationDeniedError` (`access_denied`),
`AuthorizationExpiredError` (`expired_token`), `InvalidGrantError`
(`invalid_grant`), and `ConnectNotFoundError` (`not_found`).

A device-code poll loop can now tell "keep waiting" from "stop and start over"
by catching a class rather than comparing `ConnectApiError.code` strings.

Error codes are read from the fields that name them, and the ambiguous flat
`error` field is treated as a code only when it is one this SDK recognizes;
anything else there is a human-readable message. A code arriving in a `code`
field is always reported, so a code added server-side needs no release here.
